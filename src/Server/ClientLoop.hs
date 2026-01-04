{-# LANGUAGE OverloadedStrings #-}

module Server.ClientLoop (clientLoopWrite, runCmd) where

import Control.Concurrent.STM (STM, TVar, atomically, modifyTVar, readTVar, writeTVar)
import Control.Monad (forM_, forever)
import Control.Monad.Except (liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader (ask))
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString (ByteString)
import Data.List (delete, nub)
import Data.Time (UTCTime, getCurrentTime)
import Network.Simple.TCP (Socket, send)

import Auth.Types (Sha256, UserFlags (..), UserProperty (..))
import Execution.Base (runConfigInfoCmds, runServerInfoCmds)
import Protocol.Command
import Protocol.Message (Message (MkMessage))
import Protocol.Result
import Replication.Master (runAndReplicateIO, runAndReplicateSTM, runMasterToReplicaReplicationCmds, sendReplConfs)
import Resp.Client (mkRespConn, recvResp)
import Resp.Core (encode)
import Store.AuthStore (AuthStore)
import Store.AuthStore qualified as AS
import Store.PubSubStore (getChannels)
import Store.PubSubStore qualified as PS
import Store.TypeStore qualified as TS
import StoreBackend.TypeIndex (ValueType (..))
import Types.Redis
import Wire.Class (FromResp (..), ToResp (..))

--- client server

clientLoopWrite :: (HasLogger r, HasStores r, HasReplication r) => ClientState -> Socket -> Redis r ()
clientLoopWrite clientState socket = do
    respConn <- liftIO $ mkRespConn socket
    forever $ do
        resp <- liftIO $ recvResp respConn
        cmd <- liftEither $ first RespParsingError $ fromResp resp
        result <- runCmd clientState cmd
        case result of
            ResultIgnore -> pure ()
            res -> do
                let encoded = encode $ toResp res
                liftIO $ send socket encoded

-- Common Command Execution

runCmd :: (HasStores r, HasReplication r, HasLogger r) => ClientState -> Command -> Redis r Result
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
                    RedRepl (CmdMasterToReplica c) ->
                        liftIO . atomically $ ResultOk <$> runMasterToReplicaReplicationCmds env c
                    RedAuth cmd' -> resFromEither <$> handleAuthCommands clientState cmd'
                    RedInfo section -> liftIO . atomically $ ResultOk <$> runServerInfoCmds env section
                    RedConfig section -> pure . ResultOk $ runConfigInfoCmds env section
                    RedIO cmd' -> ResultOk <$> runAndReplicateIO env cmd'
                    RedSub cmd' -> ResultOk <$> handlePubSubMessage clientState cmd'
                    CmdWait n tout -> ResultOk <$> sendReplConfs clientState n tout
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
    (HasStores r) => ClientState -> CmdAuth -> Redis r (Either Failure Success)
handleAuthCommands clientState command = do
    env <- ask
    let tvAuthStore = getAuthStore env
    case command of
        CmdAclWhoAmI -> pure . Right . ReplyResp . RespBulk $ Just "default"
        CmdAclGetUser uname -> liftIO . atomically $ do
            muser <- AS.getUserSTM uname tvAuthStore
            pure . Right . maybe (ReplyResp RespArrayNull) ReplyUserProperty $ muser
        CmdAclSetUser uname pass -> liftIO . atomically $ do
            AS.addPasswordSTM uname pass tvAuthStore
            pure $ Right ReplyOk
        CmdAuth uname pass -> liftIO . atomically $ performAuth clientState tvAuthStore uname pass

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
