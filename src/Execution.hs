{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Execution (runReplication, clientLoopWrite) where

import Control.Concurrent.STM (STM, atomically, modifyTVar, readTVarIO, writeTVar)
import Control.Monad (forever)
import Control.Monad.Except (liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader (ask))
import Data.Bifunctor (Bifunctor (first))
import Data.Time (UTCTime, getCurrentTime)
import Network.Simple.TCP (Socket, send)

import Command
import CommandResult
import Execution.Base (runCmdSTM, runServerInfoCmds)
import Redis
import Replication.Command (MasterCommand (..), propogationCmdToCmdSTM, respToMasterCmd, runAndReplicateIO, runAndReplicateSTM)
import Replication.Master (runMasterToReplicaReplicationCmds, sendReplConfs)
import Replication.Replica (doHandshake, runReplicaToMasterReplicationCmds)
import Resp.Client (RespConn, mkRespConn, recvRdb, recvResp, sendResp)
import Resp.Command (respBytes, respToCmd)
import Resp.Core (encode)
import Resp.Result (cmdResultToResp, resultToResp)

--- client server

clientLoopWrite :: (HasLogger r, HasStores r, HasReplication r) => ClientState -> Socket -> Redis r ()
clientLoopWrite clientState socket = do
    respConn <- liftIO $ mkRespConn socket
    forever $ do
        resp <- liftIO $ recvResp respConn
        cmd <- liftEither $ first ConversionError $ respToCmd resp
        result <- runCmd clientState cmd
        case result of
            ResNothing -> pure ()
            res -> do
                let encoded = encode $ resultToResp res
                liftIO $ send socket encoded

-- Common Command Execution

runCmd :: (HasStores r, HasReplication r, HasLogger r) => ClientState -> Command -> Redis r Result
runCmd clientState command = do
    env <- ask
    now <- liftIO getCurrentTime
    txState <- liftIO $ readTVarIO clientState.txState
    case command of
        RedRepl (CmdReplicaToMaster c) -> resFromMaybe <$> runReplicaToMasterReplicationCmds clientState.socket c
        RedRepl (CmdMasterToReplica c) -> ResNormal <$> liftIO (atomically $ runMasterToReplicaReplicationCmds env c)
        RedInfo section -> ResNormal <$> runServerInfoCmds section
        RedIO cmd' -> ResNormal <$> runAndReplicateIO env cmd'
        CmdWait n tout -> ResNormal <$> sendReplConfs clientState n tout
        cmd -> case txState of
            InTx cs ->
                case cmd of
                    RedSTM c -> liftIO $ atomically $ do
                        writeTVar clientState.txState (InTx $ c : cs)
                        pure . ResNormal $ RSimple "QUEUED"
                    RedTrans m -> case m of
                        Multi -> pure . ResError $ RTxErr RMultiInMulti
                        Exec -> ResCombined <$> liftIO (atomically $ executeQueuedCmds clientState env now cs)
                        Discard -> liftIO $ atomically $ do
                            writeTVar clientState.txState NoTx
                            pure . ResNormal $ ResOk
            NoTx -> case cmd of
                RedSTM cmd' -> resFromEither <$> liftIO (atomically $ runAndReplicateSTM env now cmd')
                RedTrans txCmd ->
                    case txCmd of
                        Exec -> pure . ResError $ RTxErr RExecWithoutMulti
                        Discard -> pure . ResError $ RTxErr RDiscardWithoutMulti
                        Multi -> liftIO $ atomically $ do
                            modifyTVar clientState.txState (const $ InTx [])
                            pure . ResNormal $ ResOk

-- Transaction

executeQueuedCmds :: (HasStores r, HasReplication r) => ClientState -> Env r -> UTCTime -> [CmdSTM] -> STM [Either CommandError CommandResult]
executeQueuedCmds clientState env now cmds = do
    vals <- mapM (runAndReplicateSTM env now) (reverse cmds)
    modifyTVar clientState.txState (const NoTx)
    pure vals

-- replica master update receive

runReplication :: Socket -> Redis Replica ()
runReplication socket = do
    EnvReplica _ state <- ask
    respConn <- liftIO $ mkRespConn socket
    doHandshake state respConn

    logInfo $ "Handshake complete. Waiting for RDB" <> show socket
    _rdb <- liftIO $ recvRdb respConn

    receiveMasterUpdates respConn

receiveMasterUpdates :: RespConn -> Redis Replica ()
receiveMasterUpdates respConn = do
    forever $ do
        env <- ask
        resp <- liftIO $ recvResp respConn
        liftIO $ print $ "received " <> show resp
        command <- liftEither $ first ConversionError $ respToMasterCmd resp
        now <- liftIO getCurrentTime
        liftIO $ print $ "processing " <> show command
        case command of
            PropogationCmd c -> liftIO $ atomically $ do
                _ <- runCmdSTM env now . propogationCmdToCmdSTM $ c
                modifyTVar (getOffset env) (+ respBytes resp)
            ReplicationCommand c -> do
                res <- liftIO $ atomically $ do
                    res <- runMasterToReplicaReplicationCmds env c
                    modifyTVar (getOffset env) (+ respBytes resp)
                    pure res
                liftIO $ sendResp respConn $ cmdResultToResp res
            MasterPing -> do
                liftIO $ atomically $ modifyTVar (getOffset env) (+ respBytes resp)
