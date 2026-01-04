{-# LANGUAGE OverloadedStrings #-}

module Dispatch.Client (runCmd) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (STM, TVar, atomically, modifyTVar, readTVar, writeTQueue, writeTVar)
import Control.Monad (forM_)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader (ask))
import Data.ByteString (ByteString)
import Data.List (delete, nub)
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Network.Simple.TCP (send)

import Auth (Sha256, UserFlags (..), UserProperty (..))
import Dispatch.Store (runCmdIO, runCmdSTM, runConfigInfoCmds, runServerInfoCmds)
import Protocol.Command
import Protocol.Message
import Protocol.Replication
import Protocol.Result
import Replication.Config
import Resp.Core (encode)
import Store.AuthStore (AuthStore)
import Store.AuthStore qualified as AS
import Store.PubSubStore (getChannels)
import Store.PubSubStore qualified as PS
import Store.TypeStore qualified as TS
import StoreBackend.TypeIndex (ValueType (..))
import Time (timeout')
import Types.Redis
import Wire.Class (ToResp (..))

runCmd :: (HasStores r, HasReplication r) => ClientState -> Command -> Redis r Result
runCmd clientState command = do
    env <- ask
    authRes <- liftIO . atomically $ requireAuthSTM env clientState
    case authRes of
        Left err ->
            case command of
                RedAuth (CmdAuth uname pass) ->
                    resFromEither
                        <$> liftIO (atomically $ performAuth clientState (getAuthStore env) uname pass)
                _ -> pure $ ResultErr err
        Right () -> do
            now <- liftIO getCurrentTime
            (txState, subbedChannels) <- liftIO $ atomically $ do
                ts <- readTVar clientState.txState
                sc <- readTVar clientState.subbedChannels
                pure (ts, sc)
            let isSubscribed = not . null $ subbedChannels
            if isSubscribed
                then case command of
                    RedSTM CmdPing -> pure $ ResultOk ReplyPongSubscribed
                    RedSub cmd' -> ResultOk <$> handlePubSubMessage clientState cmd'
                    cmd -> pure $ ResultErr $ ErrCmdNotAllowedInMode cmd ModeSubscribed
                else case command of
                    RedAuth cmd' -> resFromEither <$> liftIO (atomically $ handleAuthCommands env clientState cmd')
                    RedInfo section -> liftIO . atomically $ ResultOk <$> runServerInfoCmds env section
                    RedConfig section -> pure . ResultOk $ runConfigInfoCmds env section
                    RedIO cmd' -> ResultOk <$> runAndReplicateIO env cmd'
                    RedSub cmd' -> ResultOk <$> handlePubSubMessage clientState cmd'
                    CmdWait n tout -> sendReplConfs clientState n tout
                    cmd -> case txState of
                        InTx cs ->
                            case cmd of
                                RedSTM c -> liftIO $ atomically $ do
                                    writeTVar clientState.txState (InTx $ c : cs)
                                    pure . ResultOk $ ReplyResp $ RespSimple "QUEUED"
                                RedTrans m -> case m of
                                    CmdMulti -> pure . ResultErr $ ErrTx TxMultiInMulti
                                    CmdExec ->
                                        ResultTx <$> liftIO (atomically $ executeQueuedCmds clientState env now cs)
                                    CmdDiscard -> liftIO $ atomically $ do
                                        writeTVar clientState.txState NoTx
                                        pure . ResultOk $ ReplyOk
                        NoTx -> case cmd of
                            RedSTM cmd' ->
                                resFromEither <$> liftIO (atomically $ runAndReplicateSTM env now cmd')
                            RedTrans txCmd ->
                                case txCmd of
                                    CmdExec -> pure . ResultErr $ ErrTx TxExecWithoutMulti
                                    CmdDiscard -> pure . ResultErr $ ErrTx TxDiscardWithoutMulti
                                    CmdMulti -> liftIO $ atomically $ do
                                        modifyTVar clientState.txState (const $ InTx [])
                                        pure . ResultOk $ ReplyOk

-- Auth

requireAuthSTM :: (HasStores r) => Env r -> ClientState -> STM (Either Failure ())
requireAuthSTM env clientState = do
    let tvAuthStore = getAuthStore env
    isAuth <- readTVar clientState.authenticated
    if isAuth
        then pure (Right ())
        else do
            name <- readTVar clientState.name
            muser <- AS.getUserSTM name tvAuthStore
            case muser of
                Nothing -> pure $ Left $ ErrAuth AuthNoAuth
                Just userProp ->
                    if userProp.flags.nopass
                        then do
                            writeTVar clientState.authenticated True
                            pure (Right ())
                        else pure $ Left $ ErrAuth AuthNoAuth

handleAuthCommands ::
    (HasStores r) => Env r -> ClientState -> CmdAuth -> STM (Either Failure Success)
handleAuthCommands env clientState command = do
    let tvAuthStore = getAuthStore env
    case command of
        CmdAclWhoAmI -> pure . Right . ReplyResp . RespBulk $ Just "default"
        CmdAclGetUser uname -> do
            muser <- AS.getUserSTM uname tvAuthStore
            pure . Right . maybe (ReplyResp RespArrayNull) ReplyUserProperty $ muser
        CmdAclSetUser uname pass -> do
            AS.addPasswordSTM uname pass tvAuthStore
            pure $ Right ReplyOk
        CmdAuth uname pass -> performAuth clientState tvAuthStore uname pass

performAuth ::
    ClientState -> TVar AuthStore -> Key -> Sha256 -> STM (Either Failure Success)
performAuth clientState tvAuthStore uname pass = do
    muser <- AS.getUserSTM uname tvAuthStore
    let passes = maybe [] (.passwords) muser
    if pass `elem` passes
        then do
            modifyTVar clientState.authenticated (const True)
            modifyTVar clientState.name (const uname)
            pure $ Right ReplyOk
        else pure $ Left $ ErrAuth AuthWrongPassword

-- pub-sub

handlePubSubMessage :: (HasStores r) => ClientState -> PubSub -> Redis r Success
handlePubSubMessage clientState command = do
    env <- ask
    case command of
        CmdSubscribe chan -> liftIO $ atomically $ subscribeToChannel clientState env chan
        CmdPublish chan msg -> liftIO $ publishToChannel env chan msg
        CmdUnsubscribe chan -> liftIO $ atomically $ unsubscribeFromChannel clientState env chan

publishToChannel :: (HasStores r) => Env r -> Key -> ByteString -> IO Success
publishToChannel env key msg = do
    channels <- liftIO $ atomically $ getChannels key (getPubSubStore env)
    forM_ channels $ \chan -> do
        let msg' = MkMessage key msg
        send chan (encode $ toResp msg')
    pure $ ReplyResp $ RespInt (length channels)

subscribeToChannel :: (HasStores r) => ClientState -> Env r -> Key -> STM Success
subscribeToChannel clientState env chan = do
    let tvPubSubStore = getPubSubStore env
        tvTypeIndex = getTypeIndex env
    _ <-
        TS.setIfAvailable tvTypeIndex chan VChannel
            *> PS.addChannel chan clientState.socket tvPubSubStore
    modifyTVar clientState.subbedChannels (nub . (chan :))
    chans <- readTVar clientState.subbedChannels
    pure $ ReplySubscribed chan (length chans)

unsubscribeFromChannel :: (HasStores r) => ClientState -> Env r -> Key -> STM Success
unsubscribeFromChannel clientState env chan = do
    let tvPubSubStore = getPubSubStore env
    PS.removeChannel chan clientState.socket tvPubSubStore
    modifyTVar clientState.subbedChannels (delete chan)
    chans <- readTVar clientState.subbedChannels
    pure $ ReplyUnsubscribed chan (length chans)

-- Transaction

executeQueuedCmds ::
    (HasStores r, HasReplication r) =>
    ClientState ->
    Env r ->
    UTCTime ->
    [CmdSTM] ->
    STM [Either Failure Success]
executeQueuedCmds clientState env now cmds = do
    vals <- mapM (runAndReplicateSTM env now) (reverse cmds)
    modifyTVar clientState.txState (const NoTx)
    pure vals

-- Replication

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

-- Replication (WAIT)

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
