{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}

module Redis (
    Env (..),
    Redis (..),
    RedisError (..),
    ReplicationRole (..),
    ReplicaOf (..),
    handleRequest,
    mkNewEnv,
    TxState (..),
) where

import Control.Concurrent.STM (STM, TVar, atomically, modifyTVar, readTVarIO, writeTVar)
import Control.Exception (Exception)
import Control.Monad.Except (ExceptT (..), MonadError, liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader (ask), ReaderT, asks)
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString (ByteString)
import Data.List (singleton)
import Data.String (fromString)
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
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
import Text.Parsec (ParseError)
import Time (nominalDiffTimeToMicros)

import Command (
    CmdIO (..),
    CmdSTM (..),
    CmdTransaction (..),
    Command (..),
    CommandError (..),
    CommandResult (..),
    SubInfo (..),
    TransactionError (..),
    respToCmd,
    resultToResp,
 )
import Resp (decode, encode)
import Store.ListStore (ListStore)
import Store.ListStore qualified as LS
import Store.StreamStore (StreamStore)
import Store.StreamStore qualified as StS
import Store.StringStore (StringStore)
import Store.StringStore qualified as SS
import Store.TypeStore (IncorrectType, TypeIndex)
import Store.TypeStore qualified as TS
import StoreBackend.TypeIndex (ValueType (..))

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

-- Replication

data ReplicaOf = ReplicaOf
    { masterHost :: String
    , masterPort :: Int
    }
    deriving (Show, Eq)

data ReplicationRole = Master | Replica ReplicaOf
    deriving (Show)

newtype Replication = MkReplication
    { role :: ReplicationRole
    }
    deriving (Show)

-- Env

data Stores = MkStores
    { stringStore :: TVar StringStore
    , listStore :: TVar ListStore
    , typeIndex :: TVar TypeIndex
    , streamStore :: TVar StreamStore
    }

data Env = MkEnv
    { stores :: Stores
    , envLogger :: TimedFastLogger
    , replication :: Replication
    }

mkNewEnv :: ReplicationRole -> IO Env
mkNewEnv rr = do
    stores <- atomically $ do
        stringStore <- SS.emptySTM
        listStore <- LS.emptySTM
        typeIndex <- TS.emptySTM
        streamStore <- StS.emptySTM
        pure $ MkStores{..}
    timeCache <- newTimeCache simpleTimeFormat
    (envLogger, _cleanup) <- newTimedFastLogger timeCache (LogStderr defaultBufSize)
    let replication = MkReplication rr
    pure MkEnv{..}

newtype Redis a = MkRedis {runRedis :: ReaderT Env (ExceptT RedisError IO) a}
    deriving newtype (Functor, Applicative, Monad, MonadError RedisError, MonadIO, MonadReader Env)

-- Execution

runCmd :: TVar TxState -> Command -> Redis CommandResult
runCmd tvTxState cmd = do
    env <- ask
    now <- liftIO getCurrentTime
    txState <- liftIO $ readTVarIO tvTxState
    case (txState, cmd) of
        (NoTx, RedSTM cmd') -> liftIO $ atomically $ runCmdSTM env now cmd'
        (InTx cs, RedSTM c) -> liftIO $ atomically $ do
            writeTVar tvTxState (InTx $ c : cs)
            pure $ RSimple "QUEUED"
        (NoTx, RedIO cmd') -> runCmdIO cmd'
        (InTx _, RedIO _) -> pure $ RErr $ RTxErr RNotSupportedInTx
        (NoTx, RedTrans Multi) -> liftIO $ atomically $ do
            modifyTVar tvTxState (const $ InTx [])
            pure $ RSimple "OK"
        (InTx _, RedTrans Multi) -> pure $ RErr $ RTxErr RMultiInMulti
        (NoTx, RedTrans Exec) -> pure $ RErr $ RTxErr RExecWithoutMulti
        (InTx cs, RedTrans Exec) -> liftIO $ atomically $ do
            vals <- mapM (runCmdSTM env now) (reverse cs)
            modifyTVar tvTxState (const NoTx)
            pure $ RArray vals
        (NoTx, RedTrans Discard) -> pure $ RErr $ RTxErr RDiscardWithoutMulti
        (InTx _, RedTrans Discard) -> liftIO $ atomically $ do
            writeTVar tvTxState NoTx
            pure $ RSimple "OK"
        (InTx _, RedInfo _) -> pure $ RErr $ RTxErr RNotSupportedInTx
        (NoTx, RedInfo (Just IReplication)) -> pure $ RBulk $ Just $ replicationInfo env.replication
        (NoTx, RedInfo _) -> undefined

replicationInfo :: Replication -> ByteString
replicationInfo MkReplication{..} = "role:" <> roleInfo role

roleInfo :: ReplicationRole -> ByteString
roleInfo Master = "master"
roleInfo (Replica _) = "slave"

runCmdSTM :: Env -> UTCTime -> CmdSTM -> STM CommandResult
runCmdSTM env now cmd = do
    let tvListMap = env.stores.listStore
        tvStringMap = env.stores.stringStore
        tvTypeIndex = env.stores.typeIndex
        tvStreamMap = env.stores.streamStore
    case cmd of
        Ping -> pure $ RSimple "PONG"
        Echo xs -> pure $ RBulk (Just xs)
        -- type index
        Type x -> do
            ty <- TS.getTypeSTM tvTypeIndex x
            pure $ maybe (RSimple "none") (RSimple . fromString . show) ty
        -- String Store
        Set key val mexpiry -> do
            TS.setIfAvailable tvTypeIndex key VString *> SS.setSTM tvStringMap key val now mexpiry
            pure $ RSimple "OK"
        Get key -> do
            val <- SS.getSTM tvStringMap key now
            pure $ RBulk val
        Incr key -> do
            val <- SS.incrSTM tvStringMap key now
            pure $ either (const $ RErr RIncrError) RInt val
        -- List Store
        RPush key xs -> do
            count <- TS.setIfAvailable tvTypeIndex key VList *> LS.rpushSTM tvListMap key xs
            pure $ RInt count
        LPush key xs -> do
            count <- TS.setIfAvailable tvTypeIndex key VList *> LS.lpushSTM tvListMap key xs
            pure $ RInt count
        LPop key Nothing -> do
            val <- LS.lpopSTM tvListMap key
            pure $ RBulk val
        LPop key (Just mLen) -> do
            vals <- LS.lpopsSTM tvListMap key mLen
            pure $ RArraySimple vals
        LRange key range -> do
            vals <- LS.lrangeSTM tvListMap key range
            pure $ RArraySimple vals
        LLen key -> do
            len <- LS.llenSTM tvListMap key
            pure $ RInt len
        -- Stream Store
        XAdd key sId ks -> do
            val <- TS.setIfAvailable tvTypeIndex key VStream *> StS.xAddSTM tvStreamMap key sId ks now
            pure $ either (RErr . RStreamError) RStreamId val
        XRange key range -> do
            vals <- StS.xRangeSTM tvStreamMap key range
            pure $ RArrayStreamValues vals
        XRead keys -> do
            vals <- StS.xReadSTM tvStreamMap keys
            pure $ RArrayKeyValues vals

runCmdIO :: CmdIO -> Redis CommandResult
runCmdIO cmd = do
    tvListMap <- asks (.stores.listStore)
    tvStreamMap <- asks (.stores.streamStore)
    case cmd of
        -- List Store
        BLPop key 0 -> do
            vals <- liftIO $ atomically (LS.blpopSTM tvListMap key)
            pure $ RArraySimple vals
        BLPop key t -> do
            val <- liftIO (timeout' t (atomically (LS.blpopSTM tvListMap key)))
            pure $ maybe RArrayNull RArraySimple val
        -- Stream Store
        XReadBlock key sid 0 -> do
            sid' <- liftIO $ atomically (StS.xResolveStreamIdSTM tvStreamMap key sid)
            vals <- liftIO (atomically (StS.xReadBlockSTM tvStreamMap key sid'))
            pure $ (RArrayKeyValues . singleton) vals
        XReadBlock key sid tout -> do
            sid' <- liftIO $ atomically (StS.xResolveStreamIdSTM tvStreamMap key sid)
            vals <- liftIO (timeout' tout (atomically (StS.xReadBlockSTM tvStreamMap key sid')))
            pure $ maybe RArrayNull (RArrayKeyValues . singleton) vals

-- Command execution

handleRequest :: TVar TxState -> ByteString -> Redis ByteString
handleRequest tvTxState bs = do
    txState <- liftIO $ readTVarIO tvTxState
    logInfo ("Client State: " <> show txState)
    logInfo ("Received: " <> show bs)
    resp <- liftEither $ first ParsingError $ decode bs
    logDebug ("Decoded: " <> show resp)
    cmd <- liftEither $ first ConversionError $ respToCmd resp
    logDebug ("Command: " <> show cmd)
    result <- runCmd tvTxState cmd
    logDebug ("Execution result: " <> show result)
    let resultResp = resultToResp result
    encoded <- liftEither $ first EncodeError $ encode resultResp
    logInfo ("Encoded: " <> show encoded)
    pure encoded

-- helpers

timeout' :: NominalDiffTime -> IO a -> IO (Maybe a)
timeout' t = timeout (nominalDiffTimeToMicros t)

logInfo :: (MonadReader Env m, MonadIO m) => String -> m ()
logInfo msg = do
    logger <- asks envLogger
    liftIO $ logger (\t -> toLogStr (show t <> "[INFO] " <> msg <> "\n"))

logDebug :: (MonadReader Env m, MonadIO m) => String -> m ()
logDebug msg = do
    logger <- asks envLogger
    liftIO $ logger (\t -> toLogStr (show t <> "[DEBUG] " <> msg <> "\n"))
