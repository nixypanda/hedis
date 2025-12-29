{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE OverloadedStrings #-}

module Redis (
    Env (..),
    Redis (..),
    RedisError (..),
    Master,
    Replica,
    HasLogger (..),
    HasStores (..),
    HasReplication (..),
    ClientState (..),
    handleRequest,
    mkMasterEnv,
    mkReplicaEnv,
    runReplication,
    TxState (..),
    clientLoopDiscard,
    clientLoopWrite,
) where

import Control.Concurrent.STM (STM, TVar, atomically, modifyTVar, newTVarIO, readTVarIO, writeTVar)
import Control.Exception (Exception)
import Control.Monad (when)
import Control.Monad.Except (ExceptT (..), MonadError (throwError), liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader (ask), ReaderT, asks)
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Kind (Type)
import Data.List (singleton)
import Data.String (fromString)
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Network.Simple.TCP (Socket, recv, send)
import System.Log.FastLogger (
    LogType' (..),
    TimedFastLogger,
    ToLogStr (toLogStr),
    defaultBufSize,
    newTimeCache,
    newTimedFastLogger,
    simpleTimeFormat,
 )
import System.Timeout (timeout)
import Text.Parsec (ParseError, getInput, runParser)

import Command
import CommandResult
import RDB (rdbParser)
import Replication (
    MasterConfig,
    MasterState (..),
    ReplicaConfig,
    ReplicaState (..),
    Replication (..),
    acceptReplica,
    initMasterState,
    initReplicaState,
    propagateWrite,
    replicationInfo,
 )
import Resp (Resp (..), decode, decodeMany, encode)
import Store.ListStore (ListStore)
import Store.ListStore qualified as LS
import Store.StreamStore (StreamStore)
import Store.StreamStore qualified as StS
import Store.StringStore (StringStore)
import Store.StringStore qualified as SS
import Store.TypeStore (IncorrectType, TypeIndex)
import Store.TypeStore qualified as TS
import StoreBackend.TypeIndex (ValueType (..))
import Text.Parsec.ByteString (Parser)
import Time (nominalDiffTimeToMicros)

-- Types

data RedisError
    = ParsingError ParseError
    | EncodeError String
    | EmptyBuffer
    | Unimplemented String
    | ConversionError String
    | InvalidCommand String
    | WrongType IncorrectType
    deriving (Show, Exception)

-- Transactions

data TxState = NoTx | InTx [CmdSTM] -- queue only STM-capable commands
    deriving (Show)

-- Env

data Stores = MkStores
    { stringStore :: TVar StringStore
    , listStore :: TVar ListStore
    , typeIndex :: TVar TypeIndex
    , streamStore :: TVar StreamStore
    }

data Master
data Replica

data Env (r :: Type) where
    EnvMaster :: CommonEnv -> MasterState -> Env Master
    EnvReplica :: CommonEnv -> ReplicaState -> Env Replica

data CommonEnv = MkCommonEnv
    { stores :: Stores
    , envLogger :: TimedFastLogger
    }

mkCommonEnv :: IO CommonEnv
mkCommonEnv = do
    stores <- atomically $ do
        stringStore <- SS.emptySTM
        listStore <- LS.emptySTM
        typeIndex <- TS.emptySTM
        streamStore <- StS.emptySTM
        pure $ MkStores{..}

    timeCache <- newTimeCache simpleTimeFormat
    (envLogger, _cleanup) <- newTimedFastLogger timeCache (LogStderr defaultBufSize)
    pure $ MkCommonEnv stores envLogger

mkMasterEnv :: MasterConfig -> IO (Env Master)
mkMasterEnv mc = EnvMaster <$> mkCommonEnv <*> initMasterState mc

mkReplicaEnv :: ReplicaConfig -> IO (Env Replica)
mkReplicaEnv rc = EnvReplica <$> mkCommonEnv <*> pure (initReplicaState rc)

data ClientState = MkClientState
    { txState :: TVar TxState
    , socket :: Socket
    }

newtype Redis r a = MkRedis {runRedis :: ReaderT (Env r) (ExceptT RedisError IO) a}
    deriving newtype (Functor, Applicative, Monad, MonadError RedisError, MonadIO, MonadReader (Env r))

class HasStores r where
    getStores :: Env r -> Stores

instance HasStores Master where
    getStores :: Env Master -> Stores
    getStores (EnvMaster c _) = c.stores

instance HasStores Replica where
    getStores :: Env Replica -> Stores
    getStores (EnvReplica c _) = c.stores

class HasLogger r where
    getLogger :: Env r -> TimedFastLogger

instance HasLogger Master where
    getLogger :: Env Master -> TimedFastLogger
    getLogger (EnvMaster c _) = c.envLogger

instance HasLogger Replica where
    getLogger :: Env Replica -> TimedFastLogger
    getLogger (EnvReplica c _) = c.envLogger

class HasReplication r where
    getReplication :: Env r -> Replication

instance HasReplication Master where
    getReplication :: Env Master -> Replication
    getReplication (EnvMaster _ ms) = MkReplicationMaster ms

instance HasReplication Replica where
    getReplication :: Env Replica -> Replication
    getReplication (EnvReplica _ rs) = MkReplicationReplica rs

-- Execution

runCmd :: (HasStores r, HasReplication r, HasLogger r) => ClientState -> Command -> Redis r CommandResult
runCmd clientState cmd = do
    env <- ask
    now <- liftIO getCurrentTime
    txState <- liftIO $ readTVarIO clientState.txState
    logInfo $ "Current client state " <> show txState

    case txState of
        InTx cs -> case cmd of
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
            RedSTM cmd' -> do
                res <- liftIO $ atomically $ runAndReplicate env now cmd'
                logInfo $ "Running and replicating " <> show cmd' <> "result:" <> show res
                pure res
            RedIO cmd' -> runCmdIO cmd'
            RedTrans txCmd -> case txCmd of
                Exec -> pure $ RErr $ RTxErr RExecWithoutMulti
                Discard -> pure $ RErr $ RTxErr RDiscardWithoutMulti
                Multi -> liftIO $ atomically $ do
                    modifyTVar clientState.txState (const $ InTx [])
                    pure $ RSimple "OK"
            RedRepl c -> runReplicationCmds clientState c
            RedInfo section -> runServerInfoCmds section

runServerInfoCmds :: (MonadReader (Env r) m, HasReplication r) => Maybe SubInfo -> m CommandResult
runServerInfoCmds cmd = do
    env <- ask
    case cmd of
        Just IReplication -> pure $ RBulk $ Just $ replicationInfo $ getReplication env
        _ -> error "not handled"

runReplicationCmds :: (MonadReader (Env r1) m, HasReplication r1, MonadIO m, HasLogger r1) => ClientState -> CmdReplication -> m CommandResult
runReplicationCmds clientState cmd = do
    env <- ask
    case cmd of
        CmdReplConfCapabilities -> pure $ RSimple "OK"
        CmdReplConfListen _ -> pure $ RSimple "OK"
        CmdPSync "?" (-1) -> case getReplication env of
            MkReplicationMaster masterState -> do
                let rcSocket = clientState.socket
                liftIO $ acceptReplica (getLogger env) masterState rcSocket
            MkReplicationReplica _ -> error "called on replica" -- handle later
        CmdPSync _ _ -> error "not handled"
        CmdFullResync _ _ -> error "not handled"

executeQueuedCmds :: (HasStores r, HasReplication r) => ClientState -> Env r -> UTCTime -> [CmdSTM] -> STM CommandResult
executeQueuedCmds clientState env now cmds = do
    vals <- mapM (runAndReplicate env now) (reverse cmds)
    modifyTVar clientState.txState (const NoTx)
    pure $ RArray vals

runAndReplicate :: (HasReplication r, HasStores r) => Env r -> UTCTime -> CmdSTM -> STM CommandResult
runAndReplicate env now cmd = do
    res <- runCmdSTM env now cmd
    when (isWriteCmd cmd) $
        propagateWrite (getReplication env) cmd
    pure res

runCmdSTM :: (HasStores r) => Env r -> UTCTime -> CmdSTM -> STM CommandResult
runCmdSTM env now cmd = do
    let tvListMap = (listStore . getStores) env
        tvStringMap = (stringStore . getStores) env
        tvTypeIndex = (typeIndex . getStores) env
        tvStreamMap = (streamStore . getStores) env
    case cmd of
        CmdPing -> pure $ RSimple "PONG"
        CmdEcho xs -> pure $ RBulk (Just xs)
        -- type index
        CmdType x -> do
            ty <- TS.getTypeSTM tvTypeIndex x
            pure $ maybe (RSimple "none") (RSimple . fromString . show) ty
        -- String Store
        CmdSet key val mexpiry -> do
            TS.setIfAvailable tvTypeIndex key VString *> SS.setSTM tvStringMap key val now mexpiry
            pure $ RSimple "OK"
        CmdGet key -> do
            val <- SS.getSTM tvStringMap key now
            pure $ RBulk val
        CmdIncr key -> do
            val <- SS.incrSTM tvStringMap key now
            pure $ either (const $ RErr RIncrError) RInt val
        -- List Store
        CmdRPush key xs -> do
            count <- TS.setIfAvailable tvTypeIndex key VList *> LS.rpushSTM tvListMap key xs
            pure $ RInt count
        CmdLPush key xs -> do
            count <- TS.setIfAvailable tvTypeIndex key VList *> LS.lpushSTM tvListMap key xs
            pure $ RInt count
        CmdLPop key Nothing -> do
            val <- LS.lpopSTM tvListMap key
            pure $ RBulk val
        CmdLPop key (Just mLen) -> do
            vals <- LS.lpopsSTM tvListMap key mLen
            pure $ RArraySimple vals
        CmdLRange key range -> do
            vals <- LS.lrangeSTM tvListMap key range
            pure $ RArraySimple vals
        CmdLLen key -> do
            len <- LS.llenSTM tvListMap key
            pure $ RInt len
        -- Stream Store
        CmdXAdd key sId ks -> do
            val <- TS.setIfAvailable tvTypeIndex key VStream *> StS.xAddSTM tvStreamMap key sId ks now
            pure $ either (RErr . RStreamError) RStreamId val
        CmdXRange key range -> do
            vals <- StS.xRangeSTM tvStreamMap key range
            pure $ RArrayStreamValues vals
        CmdXRead keys -> do
            vals <- StS.xReadSTM tvStreamMap keys
            pure $ RArrayKeyValues vals

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
    env <- ask
    case env of
        EnvReplica _ (MkReplicaState{..}) -> do
            -- replInfo <- asks (.replication)
            liftIO $ send socket $ encode $ cmdToResp $ RedSTM CmdPing
            logInfo "Ping sent"
            (r1, buf1) <- liftIO $ recvResp1 socket mempty
            logInfo $ show r1 <> show buf1
            liftIO $ expectSimple "PONG" r1
            logInfo "PONG receieved"

            liftIO $ send socket $ encode $ cmdToResp $ RedRepl (CmdReplConfListen localPort)
            logInfo "ReplConfListen sent"
            (r2, buf2) <- liftIO $ recvResp1 socket buf1
            liftIO $ expectSimple "OK" r2
            logInfo "OK received"

            liftIO $ send socket $ encode $ cmdToResp $ RedRepl CmdReplConfCapabilities
            logInfo "ReplConfCapabilities sent"
            (r3, buf3) <- liftIO $ recvResp1 socket buf2
            liftIO $ expectSimple "OK" r3
            logInfo "OK received"

            liftIO $ send socket $ encode $ cmdToResp $ RedRepl $ CmdPSync "?" (-1)
            (r4, buf4) <- liftIO $ recvResp1 socket buf3
            liftIO $ expectFullResync r4

            logInfo $ "Handshake complete. Waiting for RDB" <> show socket
            txState <- liftIO $ newTVarIO NoTx
            logInfo $ "Replication from master complete. Receiving new updates on " <> show socket

            let recvMore buf = do
                    bs <- liftIO (recv socket 4096) >>= maybe (throwError EmptyBuffer) pure
                    pure (buf <> bs)
            let go buf = do
                    case parseWithRemainder rdbParser buf of
                        Right (_rdb, rest) -> clientLoopDiscard MkClientState{..} socket rest
                        Left _ -> recvMore buf >>= go

            go buf4

parseWithRemainder :: Parser a -> BS.ByteString -> Either ParseError (a, BS.ByteString)
parseWithRemainder p bs =
    case runParser ((,) <$> p <*> getInput) () "" bs of
        Left e -> Left e
        Right r -> Right r

recvResp1 :: Socket -> ByteString -> IO (Resp, ByteString)
recvResp1 sock = go
  where
    go acc = do
        case decodeMany acc of
            Right (r : _, rest) -> pure (r, rest)
            Right ([], _) -> recvMore acc
            Left _ -> recvMore acc

    recvMore acc = do
        bs <- recv sock 64 >>= maybe (fail "EOF") pure
        go (acc <> bs)

expectSimple :: ByteString -> Resp -> IO ()
expectSimple expected r = case r of
    Str s | s == expected -> pure ()
    other -> fail $ "Unexpected reply: " <> show other

expectFullResync :: Resp -> IO ()
expectFullResync r = case r of
    Str s | "FULLRESYNC" `BS.isPrefixOf` s -> pure ()
    other -> fail $ "Expected FULLRESYNC, got " <> show other

-- Command execution

handleRequest :: (HasLogger r, HasStores r, HasReplication r) => ClientState -> ByteString -> Redis r ByteString
handleRequest clientState bs = do
    txState <- liftIO $ readTVarIO clientState.txState
    logInfo ("Client State: " <> show txState)
    logInfo ("Received: " <> show bs)
    resp <- liftEither $ first ParsingError $ decode bs
    logDebug ("Decoded: " <> show resp)
    cmd <- liftEither $ first ConversionError $ respToCmd resp
    logDebug ("Command: " <> show cmd)
    result <- runCmd clientState cmd
    logDebug ("Execution result: " <> show result)
    let resultResp = resultToResp result
        encoded = encode resultResp
    logInfo ("Encoded: " <> show encoded)
    pure encoded

processOne :: (HasLogger r, HasStores r, HasReplication r) => ClientState -> Resp -> Redis r CommandResult
processOne clientState resp = do
    logDebug ("Decoded: " <> show resp)
    cmd <- liftEither $ first ConversionError $ respToCmd resp
    logDebug ("Command: " <> show cmd)
    result <- runCmd clientState cmd
    logDebug ("Execution result: " <> show result)
    pure result

processOneWrite :: (HasLogger r, HasStores r, HasReplication r) => ClientState -> Socket -> Resp -> Redis r ()
processOneWrite clientState socket resp = do
    result <- processOne clientState resp
    let encoded = encode (resultToResp result)
    logInfo ("Encoded: " <> show encoded)
    liftIO $ send socket encoded

processOneDiscard :: ClientState -> Resp -> Redis Replica ()
processOneDiscard clientState resp = do
    result <- processOne clientState resp
    logInfo ("Discarding sending result for: " <> show result)
    pure ()

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

clientLoopDiscard :: ClientState -> Socket -> ByteString -> Redis Replica ()
clientLoopDiscard clientState socket buf = do
    let bufferSize = 64
    logInfo "Waiting for write command"
    mbs <- liftIO $ recv socket bufferSize
    bs <- maybe (throwError EmptyBuffer) pure mbs
    logInfo $ "Received write command" <> show bs

    let buf' = buf <> bs
    case decodeMany buf' of
        Left err -> do
            logInfo $ "Error decoding: " <> show err
            throwError (ParsingError err)
        Right (resps, rest) -> do
            logInfo $ "Processing " <> show resps <> " left - " <> show rest
            mapM_ (processOneDiscard clientState) resps
            clientLoopDiscard clientState socket rest

-- helpers

timeout' :: NominalDiffTime -> IO a -> IO (Maybe a)
timeout' t = timeout (nominalDiffTimeToMicros t)

logInfo :: (MonadReader (Env r) m, MonadIO m, HasLogger r) => String -> m ()
logInfo msg = do
    logger <- asks getLogger
    liftIO $ logger (\t -> toLogStr (show t <> "[INFO] " <> msg <> "\n"))

logDebug :: (MonadReader (Env r) m, MonadIO m, HasLogger r) => String -> m ()
logDebug msg = do
    logger <- asks getLogger
    liftIO $ logger (\t -> toLogStr (show t <> "[DEBUG] " <> msg <> "\n"))
