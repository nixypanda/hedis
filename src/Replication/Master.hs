{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Replication.Master (acceptReplica, runMasterToReplicaReplicationCmds, sendReplConfs) where

import Control.Concurrent.Async (async)
import Control.Concurrent.STM (
    TQueue,
    atomically,
    modifyTVar,
    newTQueueIO,
    newTVarIO,
    readTQueue,
    readTVarIO,
 )
import Control.Monad (forM_, forever)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader (ask))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (fromString)
import Data.Time (NominalDiffTime)
import Network.Simple.TCP (Socket, send)

import Command
import CommandResult
import Control.Concurrent (threadDelay)
import Redis
import Replication.Config (MasterState (..), ReplicaConn (..), Replication (..))
import Resp.Command (cmdToResp)
import Resp.Core (encode)
import Time (timeout')

acceptReplica :: Socket -> Redis r CommandResult -- fix r to be Master
acceptReplica sock = do
    EnvMaster _ (MkMasterState{..}) <- ask
    _ <- initReplica sock
    masterReplOffset' <- liftIO $ readTVarIO masterReplOffset
    pure $ RRepl $ ResFullResync masterReplId masterReplOffset'

initReplica :: Socket -> Redis r ()
initReplica rcSocket = do
    EnvMaster _ (MkMasterState{..}) <- ask
    rcOffset <- liftIO $ newTVarIO (-1)
    rcQueue <- liftIO newTQueueIO
    rcAuxCmdOffset <- liftIO $ newTVarIO 0

    rcSender <- liftIO $ async $ replicaSender rcSocket rdbFile rcQueue
    let rc = MkReplicaConn{..}
    liftIO $ atomically $ modifyTVar replicaRegistry (rc :)

replicaSender :: Socket -> FilePath -> TQueue ByteString -> IO ()
replicaSender sock rdbFile q = do
    -- Phase 1: RDB snapshot
    rdbFileData <- liftIO $ BS.readFile rdbFile
    let respEncoded = "$" <> fromString (show $ BS.length rdbFileData) <> "\r\n" <> rdbFileData
    send sock respEncoded

    -- Phase 2: Write Command Propogation
    forever $ do
        resp <- liftIO $ atomically $ readTQueue q
        send sock resp

runMasterToReplicaReplicationCmds :: (HasReplication r) => MasterToReplica -> Redis r CommandResult
runMasterToReplicaReplicationCmds cmd = do
    env <- ask
    case cmd of
        CmdReplConfGetAck -> do
            offset <- liftIO $ readTVarIO (getOffset env)
            pure $ RRepl $ ReplConfAck offset

sendReplConfs :: (HasReplication r) => ClientState -> Int -> NominalDiffTime -> Redis r CommandResult
sendReplConfs _clientState n tout = do
    env <- ask
    case getReplication env of
        MkReplicationMaster masterState -> do
            registry <- liftIO $ readTVarIO masterState.replicaRegistry
            targetOffset <- liftIO $ readTVarIO masterState.masterReplOffset

            liftIO $ print $ "WAIT targetOffset=" <> show targetOffset <> " replicas=" <> show n <> " timeout=" <> show tout

            -- trigger GETACK
            liftIO $ forM_ registry $ \r ->
                send r.rcSocket (encode $ cmdToResp $ RedRepl $ CmdMasterToReplica CmdReplConfGetAck)

            -- The main server is already listening for commands so we just poll
            let countAcked :: IO Int
                countAcked = do
                    offsets <- mapM (readTVarIO . rcOffset) registry
                    pure $ length (filter (>= targetOffset) offsets)

                poll :: IO Int
                poll = do
                    acked <- countAcked
                    if acked >= n
                        then pure acked
                        else do
                            threadDelay 1000
                            poll

            mres <- liftIO $ timeout' tout poll
            acked <- liftIO $ maybe countAcked pure mres
            pure $ RInt $ if targetOffset > 0 then acked else length registry
        MkReplicationReplica _ ->
            error "WAIT called on replica"
