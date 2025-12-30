{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Execution (runReplication, clientLoopWrite) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async)
import Control.Concurrent.STM (
    STM,
    TQueue,
    atomically,
    modifyTVar,
    newTQueueIO,
    newTVarIO,
    readTQueue,
    readTVar,
    readTVarIO,
    writeTQueue,
    writeTVar,
 )
import Control.Monad (forM_, forever, when)
import Control.Monad.Except (MonadError (throwError), liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader (ask), asks)
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.List (find, singleton)
import Data.String (fromString)
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Network.Simple.TCP (Socket, send)
import System.Timeout (timeout)

import Command
import CommandResult
import Redis
import Replication (
    MasterState (..),
    ReplicaConn (..),
    ReplicaState (..),
    Replication (..),
    replicationInfo,
 )
import Resp (Resp (..), encode)
import RespSession (RespConn, mkRespConn, recvRdb, recvResp, sendResp)
import Store.ListStore qualified as LS
import Store.StreamStore qualified as StS
import Store.StringStore qualified as SS
import Store.TypeStore qualified as TS
import Time (nominalDiffTimeToMicros)

--- servers

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
            CmdWait n tout -> Just <$> sendReplConfs clientState n tout

runServerInfoCmds :: (HasReplication r) => Maybe SubInfo -> Redis r CommandResult
runServerInfoCmds cmd = do
    env <- ask
    case cmd of
        Just IReplication -> do
            info <- liftIO $ replicationInfo $ getReplication env
            pure $ RBulk $ Just info
        _ -> error "not handled"

runMasterToReplicaReplicationCmds :: (HasReplication r) => MasterToReplica -> Redis r CommandResult
runMasterToReplicaReplicationCmds cmd = do
    env <- ask
    case cmd of
        CmdReplConfGetAck -> do
            offset <- liftIO $ readTVarIO (getOffset env)
            pure $ RRepl $ ReplConfAck offset

runReplicaToMasterReplicationCmds :: (HasReplication r, HasLogger r) => ClientState -> ReplicaToMaster -> Redis r (Maybe CommandResult)
runReplicaToMasterReplicationCmds clientState cmd = do
    env <- ask
    case cmd of
        CmdReplConfCapabilities -> pure $ Just $ RSimple "OK"
        CmdReplConfListen _ -> pure $ Just $ RSimple "OK"
        CmdPSync "?" (-1) -> case getReplication env of
            MkReplicationMaster _ -> do
                let rcSocket = clientState.socket
                Just <$> acceptReplica rcSocket
            MkReplicationReplica _ -> error "called on replica" -- handle later
        CmdPSync _ _ -> error "not handled"
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

runReplicationCmds :: (HasReplication r, HasLogger r) => ClientState -> CmdReplication -> Redis r (Maybe CommandResult)
runReplicationCmds clientState cmd = case cmd of
    CmdMasterToReplica cmd' -> Just <$> runMasterToReplicaReplicationCmds cmd'
    CmdReplicaToMaster cmd' -> runReplicaToMasterReplicationCmds clientState cmd'

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
                send r.rcSocket (encode $ cmdToResp $ RedRepl $ CmdMasterToReplica CmdReplConfGetAck)

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
    let tvListMap = getListStore env
        tvStringMap = getStringStore env
        tvTypeIndex = getTypeIndex env
        tvStreamMap = getStreamStore env
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

runCmdIO :: (HasStores r) => CmdIO -> Redis r CommandResult
runCmdIO cmd = do
    tvListMap <- asks getListStore
    tvStreamMap <- asks getStreamStore
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
    EnvReplica _ state <- ask
    respConn <- liftIO $ mkRespConn socket
    doHandshake state respConn

    logInfo $ "Handshake complete. Waiting for RDB" <> show socket
    _rdb <- liftIO $ recvRdb respConn

    txState <- liftIO $ newTVarIO NoTx
    let fakeClient = MkClientState{..}
    receiveMasterUpdates respConn fakeClient

doHandshake :: ReplicaState -> RespConn -> Redis Replica ()
doHandshake (MkReplicaState{..}) respConn = do
    -- Ping
    let cmd1 = RedSTM CmdPing
    liftIO $ sendResp respConn $ cmdToResp cmd1
    r1 <- liftIO $ recvResp respConn
    cmdRes1 <- liftEither $ first ConversionError $ respToResult r1
    case cmdRes1 of
        ResPong -> pure ()
        other -> throwError $ HandshakeError $ InvalidReturn cmd1 ResPong other

    -- Replconf
    let cmd2 = RedRepl $ CmdReplicaToMaster $ CmdReplConfListen localPort
    liftIO $ sendResp respConn $ cmdToResp cmd2
    r2 <- liftIO $ recvResp respConn
    cmdRes2 <- liftEither $ first ConversionError $ respToResult r2
    case cmdRes2 of
        ResOk -> pure ()
        other -> throwError $ HandshakeError $ InvalidReturn cmd2 ResOk other

    -- Replconf capabilities
    let cmd3 = RedRepl $ CmdReplicaToMaster CmdReplConfCapabilities
    liftIO $ sendResp respConn $ cmdToResp cmd3
    r3 <- liftIO $ recvResp respConn
    cmdRes3 <- liftEither $ first ConversionError $ respToResult r3
    case cmdRes3 of
        ResOk -> pure ()
        other -> throwError $ HandshakeError $ InvalidReturn cmd3 ResOk other

    -- Psync
    knownMasterRepl' <- liftIO $ readTVarIO knownMasterRepl
    replicaOffset' <- liftIO $ readTVarIO replicaOffset
    let cmd4 = RedRepl $ CmdReplicaToMaster $ CmdPSync knownMasterRepl' replicaOffset'
    liftIO $ sendResp respConn $ cmdToResp cmd4
    r4 <- liftIO $ recvResp respConn
    cmdRes4 <- liftEither $ first ConversionError $ respToResult r4
    case cmdRes4 of
        (RRepl (ResFullResync sid _)) -> liftIO . atomically $ do
            writeTVar knownMasterRepl sid
            writeTVar replicaOffset 0
        other -> throwError $ HandshakeError $ InvalidReturn cmd4 (RRepl (ResFullResync "" 0)) other
    pure ()

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

propagateWrite :: Replication -> CmdSTM -> STM ()
propagateWrite repli cmd =
    case repli of
        MkReplicationMaster (MkMasterState{..}) -> do
            registry <- readTVar replicaRegistry
            mapM_ (\rc -> writeTQueue rc.rcQueue $ encode $ cmdToResp $ RedSTM cmd) registry
        MkReplicationReplica _ ->
            pure () -- replicas never propagate

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

-- helpers

timeout' :: NominalDiffTime -> IO a -> IO (Maybe a)
timeout' t = timeout (nominalDiffTimeToMicros t)
