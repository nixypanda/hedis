{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Execution (runReplication, clientLoopWrite) where

import Control.Concurrent.STM (
    STM,
    atomically,
    modifyTVar,
    newTVarIO,
    readTVarIO,
    writeTVar,
 )
import Control.Monad (forM_, forever, when)
import Control.Monad.Except (MonadError (throwError), liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader (ask), asks)
import Data.Bifunctor (Bifunctor (first))
import Data.List (find, singleton)
import Data.String (fromString)
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Network.Simple.TCP (Socket, recv, send)
import System.Timeout (timeout)

import Command
import CommandResult
import Control.Concurrent (threadDelay)
import Redis
import Replication (
    MasterState (..),
    ReplicaConn (..),
    ReplicaState (..),
    Replication (..),
    acceptReplica,
    propagateWrite,
    replicationInfo,
 )
import Resp (Resp (..), decodeMany, encode)
import RespSession (RespConn, mkRespConn, recvRdb, recvResp, sendResp)
import Store.ListStore qualified as LS
import Store.StreamStore qualified as StS
import Store.StringStore qualified as SS
import Store.TypeStore qualified as TS
import Time (nominalDiffTimeToMicros)

runCmd :: (HasStores r, HasReplication r, HasLogger r) => ClientState -> Command -> Redis r (Maybe CommandResult)
runCmd clientState cmd = do
    env <- ask
    now <- liftIO getCurrentTime
    txState <- liftIO $ readTVarIO clientState.txState

    case txState of
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
                        pure $ RSimple "OK"
                _ -> pure $ RErr $ RTxErr RNotSupportedInTx
        NoTx -> case cmd of
            RedSTM cmd' -> Just <$> liftIO (atomically $ runAndReplicate env now cmd')
            RedIO cmd' -> Just <$> runCmdIO cmd'
            RedTrans txCmd ->
                Just <$> case txCmd of
                    Exec -> pure $ RErr $ RTxErr RExecWithoutMulti
                    Discard -> pure $ RErr $ RTxErr RDiscardWithoutMulti
                    Multi -> liftIO $ atomically $ do
                        modifyTVar clientState.txState (const $ InTx [])
                        pure $ RSimple "OK"
            RedRepl c -> runReplicationCmds clientState c
            RedInfo section -> Just <$> runServerInfoCmds section

runServerInfoCmds :: (MonadReader (Env r) m, HasReplication r, MonadIO m) => Maybe SubInfo -> m CommandResult
runServerInfoCmds cmd = do
    env <- ask
    case cmd of
        Just IReplication -> do
            info <- liftIO $ replicationInfo $ getReplication env
            pure $ RBulk $ Just info
        _ -> error "not handled"

runReplicationCmds :: (HasReplication r, HasLogger r) => ClientState -> CmdReplication -> Redis r (Maybe CommandResult)
runReplicationCmds clientState cmd = do
    env <- ask
    case cmd of
        CmdReplConfCapabilities -> pure $ Just $ RSimple "OK"
        CmdReplConfListen _ -> pure $ Just $ RSimple "OK"
        CmdPSync "?" (-1) -> case getReplication env of
            MkReplicationMaster masterState -> do
                let rcSocket = clientState.socket
                Just <$> liftIO (acceptReplica (getLogger env) masterState rcSocket)
            MkReplicationReplica _ -> error "called on replica" -- handle later
        CmdPSync _ _ -> error "not handled"
        CmdFullResync _ _ -> error "not handled"
        CmdReplConfGetAck -> do
            offset <- liftIO $ readTVarIO (getOffset env)
            pure $ Just $ RRepl $ ReplConfAck offset
        CmdWait n tout -> Just <$> sendReplConfs clientState n tout
        CmdReplConfAck offset -> do
            logInfo $ "ACK offset=" <> show offset
            logInfo $ "received on socket " <> show clientState.socket

            _ <- case getReplication env of
                MkReplicationMaster masterState -> do
                    registry <- liftIO $ readTVarIO masterState.replicaRegistry
                    let findReplica = find (\r -> r.rcSocket == clientState.socket) registry
                    case findReplica of
                        Nothing -> error "f this shit how to figure it out"
                        Just r -> do
                            liftIO $ atomically $ writeTVar r.rcOffset offset
                            pure ()
                MkReplicationReplica _ -> error "called on replica"

            pure Nothing

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
                send r.rcSocket (encode $ cmdToResp $ RedRepl CmdReplConfGetAck)

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

executeQueuedCmds :: (HasStores r, HasReplication r) => ClientState -> Env r -> UTCTime -> [CmdSTM] -> STM CommandResult
executeQueuedCmds clientState env now cmds = do
    vals <- mapM (runAndReplicate env now) (reverse cmds)
    modifyTVar clientState.txState (const NoTx)
    pure $ RArray vals

runAndReplicate :: (HasReplication r, HasStores r) => Env r -> UTCTime -> CmdSTM -> STM CommandResult
runAndReplicate env now cmd = do
    res <- runCmdSTM env now cmd
    when (isWriteCmd cmd) $ do
        propagateWrite (getReplication env) cmd
        modifyTVar (getOffset env) (+ cmdSTMBytes cmd)
    pure res

runCmdSTM :: (HasStores r) => Env r -> UTCTime -> CmdSTM -> STM CommandResult
runCmdSTM env now cmd = do
    let tvListMap = (listStore . getStores) env
        tvStringMap = (stringStore . getStores) env
        tvTypeIndex = (typeIndex . getStores) env
        tvStreamMap = (streamStore . getStores) env
    case cmd of
        CmdPing -> pure ResPong
        CmdEcho xs -> pure $ RBulk (Just xs)
        -- type index
        CmdType x -> do
            ty <- TS.getTypeSTM tvTypeIndex x
            pure $ ResType ty
        STMString c -> SS.runStringStoreSTM tvTypeIndex tvStringMap now c
        STMList c -> LS.runListStoreSTM tvTypeIndex tvListMap c
        STMStream c -> StS.runStreamStoreSTM tvTypeIndex tvStreamMap now c

runCmdIO :: (MonadReader (Env r) m, HasStores r, MonadIO m) => CmdIO -> m CommandResult
runCmdIO cmd = do
    tvListMap <- asks (listStore . getStores)
    tvStreamMap <- asks (streamStore . getStores)
    case cmd of
        -- List Store
        CmdBLPop key 0 -> do
            vals <- liftIO $ atomically (LS.blpopSTM tvListMap key)
            pure $ RArraySimple vals
        CmdBLPop key t -> do
            val <- liftIO (timeout' t (atomically (LS.blpopSTM tvListMap key)))
            pure $ maybe RArrayNull RArraySimple val
        -- Stream Store
        CmdXReadBlock key sid 0 -> do
            sid' <- liftIO $ atomically (StS.xResolveStreamIdSTM tvStreamMap key sid)
            vals <- liftIO (atomically (StS.xReadBlockSTM tvStreamMap key sid'))
            pure $ (RArrayKeyValues . singleton) vals
        CmdXReadBlock key sid tout -> do
            sid' <- liftIO $ atomically (StS.xResolveStreamIdSTM tvStreamMap key sid)
            vals <- liftIO (timeout' tout (atomically (StS.xReadBlockSTM tvStreamMap key sid')))
            pure $ maybe RArrayNull (RArrayKeyValues . singleton) vals

runReplication :: Socket -> Redis Replica ()
runReplication socket = do
    EnvReplica _ (MkReplicaState{..}) <- ask
    respConn <- liftIO $ mkRespConn socket
    -- Ping
    let cmd1 = RedSTM CmdPing
    liftIO $ sendResp respConn $ cmdToResp cmd1
    r1 <- liftIO $ recvResp respConn
    case r1 of
        Str s | s == "PONG" -> pure ()
        other -> throwError $ HandshakeError $ InvalidReturn cmd1 "PONG" (fromString $ show other)

    -- Replconf
    let cmd2 = RedRepl (CmdReplConfListen localPort)
    liftIO $ sendResp respConn $ cmdToResp cmd2
    r2 <- liftIO $ recvResp respConn
    case r2 of
        Str s | s == "OK" -> pure ()
        other -> throwError $ HandshakeError $ InvalidReturn cmd2 "OK" (fromString $ show other)

    -- Replconf capabilities
    let cmd3 = RedRepl CmdReplConfCapabilities
    liftIO $ sendResp respConn $ cmdToResp cmd3
    r3 <- liftIO $ recvResp respConn
    case r3 of
        Str s | s == "OK" -> pure ()
        other -> throwError $ HandshakeError $ InvalidReturn cmd3 "OK" (fromString $ show other)

    -- Psync
    knownMasterRepl' <- liftIO $ readTVarIO knownMasterRepl
    replicaOffset' <- liftIO $ readTVarIO replicaOffset
    let cmd4 = RedRepl $ CmdPSync knownMasterRepl' replicaOffset'
    liftIO $ sendResp respConn $ cmdToResp cmd4
    r4 <- liftIO $ recvResp respConn
    case respToCmd r4 of
        Right (RedRepl (CmdFullResync sid _)) -> liftIO . atomically $ do
            writeTVar knownMasterRepl sid
            writeTVar replicaOffset 0
        _ -> throwError $ HandshakeError $ InvalidReturn cmd4 "FULLRESYNC <repli_id> 0" (fromString $ show r4)

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

-- Command execution

processOne :: (HasLogger r, HasStores r, HasReplication r) => ClientState -> Resp -> Redis r (Maybe CommandResult)
processOne clientState resp = do
    cmd <- liftEither $ first ConversionError $ respToCmd resp
    runCmd clientState cmd

processOneWrite :: (HasLogger r, HasStores r, HasReplication r) => ClientState -> Socket -> Resp -> Redis r ()
processOneWrite clientState socket resp = do
    result <- processOne clientState resp
    case result of
        Just result' -> do
            let encoded = encode (resultToResp result')
            liftIO $ send socket encoded
            pure ()
        Nothing -> pure ()

clientLoopWrite :: (HasLogger r, HasStores r, HasReplication r) => ClientState -> Socket -> Redis r ()
clientLoopWrite clientState socket = go mempty
  where
    bufferSize = 1024

    go buf = do
        mbs <- liftIO $ recv socket bufferSize
        bs <- maybe (throwError EmptyBuffer) pure mbs

        let buf' = buf <> bs
        case decodeMany buf' of
            Left err -> throwError (ParsingError err)
            Right (resps, rest) -> do
                mapM_ (processOneWrite clientState socket) resps
                go rest

-- helpers

timeout' :: NominalDiffTime -> IO a -> IO (Maybe a)
timeout' t = timeout (nominalDiffTimeToMicros t)
