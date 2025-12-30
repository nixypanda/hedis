{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Execution (runReplication, clientLoopWrite) where

import Control.Concurrent.STM (STM, atomically, modifyTVar, newTVarIO, readTVarIO, writeTVar)
import Control.Monad (forever)
import Control.Monad.Except (liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader (ask))
import Data.Bifunctor (Bifunctor (first))
import Data.Time (UTCTime, getCurrentTime)
import Network.Simple.TCP (Socket, send)

import Command
import CommandResult
import Execution.Base (runServerInfoCmds)
import Redis
import Replication.Command (runAndReplicateIO, runAndReplicateSTM)
import Replication.Master (runMasterToReplicaReplicationCmds, sendReplConfs)
import Replication.Replica (doHandshake, runReplicaToMasterReplicationCmds)
import Resp.Client (RespConn, mkRespConn, recvRdb, recvResp, sendResp)
import Resp.Command (respBytes, respToCmd)
import Resp.Core (Resp (..), encode)
import Resp.Result (resultToResp)

--- client server

clientLoopWrite :: (HasLogger r, HasStores r, HasReplication r) => ClientState -> Socket -> Redis r ()
clientLoopWrite clientState socket = do
    respConn <- liftIO $ mkRespConn socket
    forever $ do
        resp <- liftIO $ recvResp respConn
        result <- processOne clientState resp
        case result of
            Just result' -> do
                let encoded = encode (resultToResp result')
                liftIO $ send socket encoded
            Nothing -> pure ()

processOne :: (HasLogger r, HasStores r, HasReplication r) => ClientState -> Resp -> Redis r (Maybe CommandResult)
processOne clientState resp = do
    cmd <- liftEither $ first ConversionError $ respToCmd resp
    runCmd clientState cmd

-- Common Command Execution

runCmd :: (HasStores r, HasReplication r, HasLogger r) => ClientState -> Command -> Redis r (Maybe CommandResult)
runCmd clientState command = do
    env <- ask
    now <- liftIO getCurrentTime
    txState <- liftIO $ readTVarIO clientState.txState
    case command of
        RedRepl (CmdReplicaToMaster c) -> runReplicaToMasterReplicationCmds clientState.socket c
        RedRepl (CmdMasterToReplica c) -> Just <$> runMasterToReplicaReplicationCmds c
        RedInfo section -> Just <$> runServerInfoCmds section
        RedIO cmd' -> Just <$> runAndReplicateIO env cmd'
        CmdWait n tout -> Just <$> sendReplConfs clientState n tout
        cmd -> case txState of
            InTx cs ->
                Just <$> case cmd of
                    RedSTM c -> liftIO $ atomically $ do
                        writeTVar clientState.txState (InTx $ c : cs)
                        pure $ RSimple "QUEUED"
                    RedTrans m -> case m of
                        Multi -> pure $ RErr $ RTxErr RMultiInMulti
                        Exec -> liftIO $ atomically $ executeQueuedCmds clientState env now cs
                        Discard -> liftIO $ atomically $ do
                            writeTVar clientState.txState NoTx
                            pure ResOk
            NoTx -> case cmd of
                RedSTM cmd' -> Just <$> liftIO (atomically $ runAndReplicateSTM env now cmd')
                RedTrans txCmd ->
                    Just <$> case txCmd of
                        Exec -> pure $ RErr $ RTxErr RExecWithoutMulti
                        Discard -> pure $ RErr $ RTxErr RDiscardWithoutMulti
                        Multi -> liftIO $ atomically $ do
                            modifyTVar clientState.txState (const $ InTx [])
                            pure ResOk

-- Transaction

executeQueuedCmds :: (HasStores r, HasReplication r) => ClientState -> Env r -> UTCTime -> [CmdSTM] -> STM CommandResult
executeQueuedCmds clientState env now cmds = do
    vals <- mapM (runAndReplicateSTM env now) (reverse cmds)
    modifyTVar clientState.txState (const NoTx)
    pure $ RArray vals

-- replica master update receive

runReplication :: Socket -> Redis Replica ()
runReplication socket = do
    EnvReplica _ state <- ask
    respConn <- liftIO $ mkRespConn socket
    doHandshake state respConn

    logInfo $ "Handshake complete. Waiting for RDB" <> show socket
    _rdb <- liftIO $ recvRdb respConn

    txState <- liftIO $ newTVarIO NoTx
    let fakeClient = MkClientState{..}
    receiveMasterUpdates respConn fakeClient

receiveMasterUpdates :: RespConn -> ClientState -> Redis Replica ()
receiveMasterUpdates respConn fakeClient = forever $ do
    env <- ask
    resp <- liftIO $ recvResp respConn
    result <- processOne fakeClient resp
    case result of
        Just r@(RRepl cr) -> case cr of
            ReplOk -> pure ()
            ReplConfAck _ -> do
                liftIO $ atomically $ modifyTVar (getOffset env) (+ respBytes resp)
                liftIO $ sendResp respConn $ resultToResp r
            ResFullResync _ _ -> pure ()
        Just ResPong -> do
            liftIO $ atomically $ modifyTVar (getOffset env) (+ respBytes resp)
        _ -> pure ()
