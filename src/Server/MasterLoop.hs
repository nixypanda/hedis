{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Server.MasterLoop (runMasterLoop) where

import Control.Concurrent.STM (atomically, readTVarIO, writeTVar)
import Control.Monad (forever)
import Control.Monad.Except (liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader (ask))
import Data.Bifunctor (Bifunctor (first))
import Data.List (find)
import Network.Simple.TCP (Socket, send)

import Protocol.Command
import Protocol.Replication
import Protocol.Result
import Replication.Config (MasterState (..), ReplicaConn (..))
import Replication.Master (acceptReplica)
import Resp.Client (mkRespConn, recvResp)
import Resp.Core (Resp, encode)
import Server.ClientLoop (runCmd)
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
