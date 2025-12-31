{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Replication.Master (
    acceptReplica,
    runMasterToReplicaReplicationCmds,
    sendReplConfs,
    runAndReplicateIO,
    runAndReplicateSTM,
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async)
import Control.Concurrent.STM (STM, TQueue, atomically, modifyTVar, newTQueueIO, newTVarIO, readTQueue, readTVar, readTVarIO, writeTQueue)
import Control.Monad (forM_, forever)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Reader (MonadReader (ask))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (fromString)
import Data.Time (NominalDiffTime, UTCTime)
import Network.Simple.TCP (Socket, send)

import Execution.Base (runCmdIO, runCmdSTM)
import Protocol.Command
import Protocol.MasterCmd
import Protocol.Result
import Replication.Config (MasterState (..), ReplicaConn (..), Replication (..))
import Resp.Core (encode)
import Time (timeout')
import Types.Redis
import Wire.Class (ToResp (..))

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

runMasterToReplicaReplicationCmds :: (HasReplication r) => Env r -> MasterToReplica -> STM CommandResult
runMasterToReplicaReplicationCmds env cmd = do
    case cmd of
        CmdReplConfGetAck -> do
            offset <- readTVar (getOffset env)
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
                send r.rcSocket (encode $ toResp $ RedRepl $ CmdMasterToReplica CmdReplConfGetAck)

            -- The main server is already listening for commands so we just poll
            let countAcked :: IO Int
                countAcked = do
                    offsets <- mapM (readTVarIO . rcOffset) registry
                    pure $ length (filter (>= targetOffset) offsets)

                poll :: IO Int
                poll = do
                    acked <- countAcked
                    if acked >= n then pure acked else threadDelay 1000 >> poll

            mres <- liftIO $ timeout' tout poll
            acked <- liftIO $ maybe countAcked pure mres
            pure $ RInt $ if targetOffset > 0 then acked else length registry
        MkReplicationReplica _ ->
            error "WAIT called on replica"

runAndReplicateSTM :: (HasReplication r, HasStores r) => Env r -> UTCTime -> CmdSTM -> STM (Either CommandError CommandResult)
runAndReplicateSTM env now cmd = do
    res <- runCmdSTM env now cmd
    case replicateSTMCmdAs cmd <$> res of
        Right (Just cmd') -> do
            propagateWrite (getReplication env) cmd'
            modifyTVar (getOffset env) (+ respBytes cmd')
            pure res
        _ -> pure res

-- The run and adding to queue is not atomic
runAndReplicateIO :: (HasStores r, HasReplication r) => Env r -> CmdIO -> Redis r CommandResult
runAndReplicateIO env cmd = do
    res <- runCmdIO cmd
    case replicateIOCmdAs cmd res of
        Nothing -> pure res
        Just cmd' -> do
            liftIO $ atomically $ do
                propagateWrite (getReplication env) cmd'
                modifyTVar (getOffset env) (+ respBytes cmd')
            pure res

propagateWrite :: Replication -> PropogationCmd -> STM ()
propagateWrite repli cmd =
    case repli of
        MkReplicationMaster (MkMasterState{..}) -> do
            registry <- readTVar replicaRegistry
            mapM_ (\rc -> writeTQueue rc.rcQueue $ encode $ toResp cmd) registry
        MkReplicationReplica _ ->
            pure () -- replicas never propagate
