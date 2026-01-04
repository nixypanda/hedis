{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Server.ReplicaLoop (runReplication, runReplicaLoop) where

import Control.Concurrent.STM (STM, atomically, modifyTVar, readTVar)
import Control.Monad (forever)
import Control.Monad.Except (liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader (ask))
import Data.Bifunctor (Bifunctor (first))
import Data.Time (getCurrentTime)
import Network.Simple.TCP (Socket, send)

import Execution.Base (runCmdSTM)
import Protocol.Replication
import Protocol.Result
import Replication.Replica (doHandshake)
import Resp.Client (RespConn, mkRespConn, recvRdb, recvResp, sendResp)
import Resp.Core (encode)
import Server.ClientLoop (runCmd)
import Types.Redis
import Wire.Class (FromResp (..), ToResp (..))

runReplicaLoop :: ClientState -> Redis Replica ()
runReplicaLoop clientState = do
    respConn <- liftIO $ mkRespConn clientState.socket
    forever $ do
        resp <- liftIO $ recvResp respConn
        cmd <- liftEither $ first RespParsingError $ fromResp resp

        result <- runCmd clientState cmd
        let encoded = encode $ toResp result
        liftIO $ send clientState.socket encoded

runMasterToReplicaCmds :: Env Replica -> MasterToReplica -> STM Success
runMasterToReplicaCmds env cmd = do
    case cmd of
        CmdReplConfGetAck -> do
            offset <- readTVar (getOffset env)
            pure $ ReplyReplication $ ReplConfAck offset

runReplication :: Socket -> Redis Replica ()
runReplication socket = do
    EnvReplica _ state <- ask
    respConn <- liftIO $ mkRespConn socket

    doHandshake state respConn
    logInfo $ "Handshake complete. Waiting for RDB" <> show socket

    _rdb <- liftIO $ recvRdb respConn
    liftIO $ print _rdb
    logInfo $ "RDB received, starting master listening loop" <> show socket

    receiveMasterUpdatesLoop respConn

receiveMasterUpdatesLoop :: RespConn -> Redis Replica ()
receiveMasterUpdatesLoop respConn = do
    env <- ask
    forever $ do
        resp <- liftIO $ recvResp respConn
        command <- liftEither $ first RespParsingError $ fromResp resp
        now <- liftIO getCurrentTime
        let offsetIncr = modifyTVar (getOffset env) (+ respBytes command)
        case command of
            PropogationCmd c -> liftIO $ atomically $ do
                _ <- runCmdSTM env now . propogationCmdToCmdSTM $ c
                offsetIncr
            ReplicationCommand c -> do
                res <- liftIO $ atomically $ do
                    res <- runMasterToReplicaCmds env c
                    offsetIncr
                    pure $ ResultOk res
                liftIO $ sendResp respConn $ toResp res
            MasterPing -> do
                liftIO $ atomically offsetIncr
