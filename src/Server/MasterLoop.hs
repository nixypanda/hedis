{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Server.MasterLoop (runMasterLoop, runRdbLoad) where

import Control.Concurrent.Async (async)
import Control.Concurrent.STM
import Control.Exception (Exception (..), try)
import Control.Monad (forever)
import Control.Monad.Error.Class (liftEither)
import Control.Monad.IO.Class (MonadIO (..))
import Control.Monad.Reader (MonadReader (ask))
import Data.Bifunctor (Bifunctor (first), first)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.List (find)
import Data.String (fromString)
import Data.Time (getCurrentTime)
import Network.Simple.TCP (Socket, send)
import System.IO.Error (isDoesNotExistError)

import Dispatch.Client (runCmd)
import Protocol.Command
import Protocol.Replication
import Protocol.Result
import Rdb.Load (loadRdbData)
import Replication.Config
import Resp.Client (mkRespConn, recvResp)
import Resp.Core (Resp, encode)
import Types.Redis
import Wire.Class (FromResp (..), ToResp (..))

data Incoming
    = FromReplica ReplicaToMaster
    | FromClient Command

respToIncoming :: Resp -> Either String Incoming
respToIncoming resp =
    case fromResp resp of
        Right replCmd -> Right (FromReplica replCmd)
        Left _ -> FromClient <$> fromResp resp

runMasterLoop :: ClientState -> Redis Master ()
runMasterLoop clientState = do
    respConn <- liftIO $ mkRespConn clientState.socket
    forever $ do
        resp <- liftIO $ recvResp respConn
        incoming <- liftEither $ first RespParsingError $ respToIncoming resp

        case incoming of
            FromReplica replCmd -> do
                result <- runReplicaToMasterCmds clientState.socket replCmd
                case result of
                    Nothing -> pure ()
                    Just res -> do
                        let encoded = encode $ toResp res
                        liftIO $ send clientState.socket encoded
            FromClient cmd -> do
                result <- runCmd clientState cmd
                let encoded = encode $ toResp result
                liftIO $ send clientState.socket encoded

runReplicaToMasterCmds :: Socket -> ReplicaToMaster -> Redis Master (Maybe Success)
runReplicaToMasterCmds socket cmd = do
    case cmd of
        CmdReplicaToMasterPing -> pure $ Just ReplyPong
        CmdReplConfCapabilities -> pure $ Just ReplyOk
        CmdReplConfListen _ -> pure $ Just ReplyOk
        CmdPSync "?" (-1) -> do
            let rcSocket = socket
            Just <$> acceptReplica rcSocket
        CmdPSync _ _ -> error "not handled"
        CmdReplConfAck offset -> do
            logInfo $ "ACK offset=" <> show offset
            logInfo $ "received on socket " <> show socket
            EnvMaster _ masterState <- ask

            registry <- liftIO $ readTVarIO masterState.replicaRegistry
            let findReplica = find (\r -> r.rcSocket == socket) registry
            _ <- case findReplica of
                Nothing -> error "f this shit how to figure it out"
                Just r -> do
                    liftIO $ atomically $ writeTVar r.rcOffset offset
                    pure ()
            pure Nothing

-- Setup replica connection

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

-- Load Rdb file

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
