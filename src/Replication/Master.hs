{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Replication.Master (
    runRdbLoad,
    acceptReplica,
    sendReplConfs,
    runAndReplicateIO,
    runAndReplicateSTM,
) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async)
import Control.Concurrent.STM
import Control.Exception (Exception (..), try)
import Control.Monad (forM_, forever)
import Control.Monad.Error.Class (liftEither)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Reader (MonadReader (ask))
import Data.Bifunctor (first)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (fromString)
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Network.Simple.TCP (Socket, send)
import System.IO.Error (isDoesNotExistError)

import Execution.Base (runCmdIO, runCmdSTM)
import Protocol.Command
import Protocol.Replication
import Protocol.Result
import Rdb.Load (loadRdbData)
import Replication.Config
import Resp.Core (encode)
import Time (timeout')
import Types.Redis
import Wire.Class (ToResp (..))

runRdbLoad :: Redis Master ()
runRdbLoad = do
    EnvMaster cs ms <- ask
    eRdbFileBinary <- liftIO $ try (BS.readFile $ rdbFilePath ms)
    case eRdbFileBinary of
        Left eIO
            | isDoesNotExistError eIO -> do
                logInfo $ "RDB file does not exist: " <> rdbFilePath ms <> ". Starting with empty state."
                pure ()
            | otherwise -> liftEither $ Left (IOError $ displayException eIO)
        Right rdbFileBinary -> do
            now <- liftIO getCurrentTime
            r <- liftIO $ atomically $ loadRdbData now rdbFileBinary cs.stores
            liftEither $ first RdbParsingError r
            pure ()

acceptReplica :: Socket -> Redis Master Success -- fix r to be Master
acceptReplica sock = do
    EnvMaster _ (MkMasterState{..}) <- ask
    _ <- initReplica sock
    masterReplOffset' <- liftIO $ readTVarIO masterReplOffset
    pure $ ReplyReplication $ ReplFullResync masterReplId masterReplOffset'

initReplica :: Socket -> Redis Master ()
initReplica rcSocket = do
    EnvMaster _ ms@(MkMasterState{..}) <- ask
    rcOffset <- liftIO $ newTVarIO (-1)
    rcQueue <- liftIO newTQueueIO
    rcAuxCmdOffset <- liftIO $ newTVarIO 0
    let rdbFile = rdbFilePath ms

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

sendReplConfs :: (HasReplication r) => ClientState -> Int -> NominalDiffTime -> Redis r Result
sendReplConfs _clientState n tout = do
    env <- ask
    case getReplication env of
        MkReplicationMaster masterState -> do
            (registry, targetOffset) <- liftIO $ atomically $ do
                r <- readTVar masterState.replicaRegistry
                t <- readTVar masterState.masterReplOffset
                pure (r, t)

            -- trigger GETACK
            liftIO $ forM_ registry $ \r ->
                send r.rcSocket (encode $ toResp CmdReplConfGetAck)

            -- The main server is already listening for commands so we just poll
            let countAcked :: STM Int
                countAcked = do
                    offsets <- mapM (readTVar . rcOffset) registry
                    pure $ length (filter (>= targetOffset) offsets)

                poll :: IO Int
                poll = do
                    acked <- liftIO $ atomically countAcked
                    if acked >= n then pure acked else threadDelay 1000 >> poll

            mres <- liftIO $ timeout' tout poll
            acked <- liftIO $ maybe (liftIO $ atomically countAcked) pure mres
            pure $ ResultOk $ ReplyResp $ RespInt $ if targetOffset > 0 then acked else length registry
        MkReplicationReplica _ ->
            pure $ ResultErr ErrWaitOnRplica

runAndReplicateSTM ::
    (HasReplication r, HasStores r) => Env r -> UTCTime -> CmdSTM -> STM (Either Failure Success)
runAndReplicateSTM env now cmd = do
    res <- runCmdSTM env now cmd
    case replicateSTMCmdAs cmd <$> res of
        Right (Just cmd') -> do
            propagateWrite (getReplication env) cmd'
            modifyTVar (getOffset env) (+ respBytes cmd')
            pure res
        _ -> pure res

-- The run and adding to queue is not atomic
runAndReplicateIO :: (HasStores r, HasReplication r) => Env r -> CmdIO -> Redis r Success
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
