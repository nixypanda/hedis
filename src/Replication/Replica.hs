{-# LANGUAGE OverloadedStrings #-}

module Replication.Replica (doHandshake) where

import Control.Concurrent.STM (atomically, readTVarIO, writeTVar)
import Control.Monad.Except (MonadError (throwError), liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.Bifunctor (Bifunctor (first))

import Protocol.Command
import Protocol.Result
import Replication.Config
import Resp.Client (RespConn, recvResp, sendResp)
import Types.Redis
import Wire.Class (FromResp (..), ToResp (..))

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
