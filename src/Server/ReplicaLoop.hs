{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Server.ReplicaLoop (runReplication, runReplicaLoop) where

import Control.Concurrent.STM (STM, atomically, modifyTVar, readTVar, readTVarIO, writeTVar)
import Control.Monad (forever)
import Control.Monad.Except (MonadError (throwError), liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader (ask))
import Data.Bifunctor (Bifunctor (first))
import Data.Time (getCurrentTime)
import Network.Simple.TCP (Socket, send)

import Dispatch.Client (runCmd)
import Dispatch.Store (runCmdSTM)
import Resp.Client (RespConn, mkRespConn, recvRdb, recvResp, sendResp)
import Resp.Core (encode)
import Types.Propogation
import Types.Redis
import Types.Replication
import Types.Result
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

doHandshake :: ReplicaState -> RespConn -> Redis Replica ()
doHandshake (MkReplicaState{..}) respConn = do
    -- Ping
    let cmd1 = CmdReplicaToMasterPing
    liftIO $ sendResp respConn $ toResp cmd1
    r1 <- liftIO $ recvResp respConn
    cmdRes1 <- liftEither $ first RespParsingError $ fromResp r1
    case cmdRes1 of
        (ResultOk ReplyPong) -> pure ()
        other -> throwError $ HandshakeError $ InvalidReturn cmd1 ReplyPong other

    -- Replconf
    let cmd2 = CmdReplConfListen localPort
    liftIO $ sendResp respConn $ toResp cmd2
    r2 <- liftIO $ recvResp respConn
    cmdRes2 <- liftEither $ first RespParsingError $ fromResp r2
    case cmdRes2 of
        (ResultOk ReplyOk) -> pure ()
        other -> throwError $ HandshakeError $ InvalidReturn cmd2 ReplyOk other

    -- Replconf capabilities
    let cmd3 = CmdReplConfCapabilities
    liftIO $ sendResp respConn $ toResp cmd3
    r3 <- liftIO $ recvResp respConn
    cmdRes3 <- liftEither $ first RespParsingError $ fromResp r3
    case cmdRes3 of
        (ResultOk ReplyOk) -> pure ()
        other -> throwError $ HandshakeError $ InvalidReturn cmd3 ReplyOk other

    -- Psync
    knownMasterRepl' <- liftIO $ readTVarIO knownMasterRepl
    replicaOffset' <- liftIO $ readTVarIO replicaOffset
    let cmd4 = CmdPSync knownMasterRepl' replicaOffset'
    liftIO $ sendResp respConn $ toResp cmd4
    r4 <- liftIO $ recvResp respConn
    cmdRes4 <- liftEither $ first RespParsingError $ fromResp r4
    case cmdRes4 of
        (ResultOk (ReplyReplication (ReplFullResync sid _))) -> liftIO . atomically $ do
            writeTVar knownMasterRepl sid
            writeTVar replicaOffset 0
        other -> throwError $ HandshakeError $ InvalidReturn cmd4 (ReplyReplication (ReplFullResync "" 0)) other
    pure ()
