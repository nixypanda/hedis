{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}

module Redis (Env (..), Redis (..), RedisError (..), handleRequest, mkNewEnv) where

import Control.Concurrent.STM (
    STM,
    TVar,
    atomically,
    modifyTVar',
    newTVarIO,
    readTVar,
    retry,
    throwSTM,
    writeTVar,
 )
import Control.Exception (Exception)
import Control.Monad (unless)
import Control.Monad.Except (ExceptT (..), MonadError, liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader, ReaderT, asks)
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString (ByteString)
import Data.List (singleton)
import Data.Map (Map)
import Data.Map qualified as M
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

import Command (Command (..), CommandResult (..), Key, respToCommand, resultToResp)
import Resp (decode, encode)
import Store.ExpiringMap (ExpiringMap)
import Store.ExpiringMap qualified as EM
import Store.ListMap (ListMap, Range (..))
import Store.ListMap qualified as LM
import Store.StreamMap (ConcreteStreamId, StreamId, StreamMap, StreamMapError (..), XRStreamId (..))
import Store.StreamMap qualified as SM
import Store.TypeIndex (RequiredType (..), ValueType (..), checkAvailable)
import Time (nominalDiffTimeToMicros)

-- Types

data RedisError
    = ParsingError ParseError
    | EncodeError String
    | EmptyBuffer
    | Unimplemented String
    | ConversionError String
    | InvalidCommand String
    | WrongType
    deriving (Show, Exception)

type ListStore = ListMap Key ByteString
type StringStore = ExpiringMap Key ByteString
type TypeIndex = Map Key ValueType
type StreamStore = StreamMap Key ByteString ByteString

data Env = MkEnv
    { stringStore :: TVar StringStore
    , listStore :: TVar ListStore
    , typeIndex :: TVar TypeIndex
    , streamStore :: TVar StreamStore
    , envLogger :: TimedFastLogger
    }

newtype Redis a = MkRedis {runRedis :: ReaderT Env (ExceptT RedisError IO) a}
    deriving newtype (Functor, Applicative, Monad, MonadError RedisError, MonadIO, MonadReader Env)

mkNewEnv :: IO Env
mkNewEnv = do
    stringStore <- newTVarIO EM.empty
    listStore <- newTVarIO LM.empty
    typeIndex <- newTVarIO M.empty
    streamStore <- newTVarIO SM.empty
    timeCache <- newTimeCache simpleTimeFormat
    (envLogger, _cleanup) <- newTimedFastLogger timeCache (LogStderr defaultBufSize)
    pure MkEnv{..}

-- Execution

runCmd :: Command -> Redis CommandResult
runCmd cmd = do
    tvListMap <- asks listStore
    tvStringMap <- asks stringStore
    tvTypeIndex <- asks typeIndex
    tvStreamMap <- asks streamStore
    now <- liftIO getCurrentTime
    case cmd of
        Ping -> pure $ RSimple "PONG"
        Echo xs -> pure $ RBulk (Just xs)
        -- type index
        Type x -> do
            ty <- liftIO $ atomically $ getTypeSTM tvTypeIndex x
            pure $ maybe (RSimple "none") (RSimple . fromString . show) ty
        -- String Store
        Set key val mexpiry -> do
            liftIO $ atomically (setIfAvailable tvTypeIndex key VString *> setSTM tvStringMap key val now mexpiry)
            pure $ RSimple "OK"
        Get key -> do
            val <- liftIO $ atomically (getSTM tvStringMap key now)
            pure $ RBulk val
        -- List Store
        Rpush key xs -> do
            count <- liftIO (atomically (setIfAvailable tvTypeIndex key VList *> rpushSTM tvListMap key xs))
            pure $ RInt count
        Lpush key xs -> do
            count <- liftIO (atomically (setIfAvailable tvTypeIndex key VList *> lpushSTM tvListMap key xs))
            pure $ RInt count
        Lpop key Nothing -> do
            val <- liftIO (atomically (lpopSTM tvListMap key))
            pure $ RBulk val
        Lpop key (Just mLen) -> do
            vals <- liftIO (atomically (lpopsSTM tvListMap key mLen))
            pure $ RArray vals
        Blpop key 0 -> do
            vals <- liftIO $ atomically (blpopSTM tvListMap key)
            pure $ RArray vals
        Blpop key tout -> do
            val <- liftIO (timeout (nominalDiffTimeToMicros tout) (atomically (blpopSTM tvListMap key)))
            pure $ maybe RNullArray RArray val
        Lrange key start end -> do
            vals <- liftIO (atomically (lrangeSTM tvListMap key MkRange{..}))
            pure $ RArray vals
        Llen key -> do
            len <- liftIO (atomically (llenSTM tvListMap key))
            pure $ RInt len
        -- Stream Store
        Xadd key sId chunked -> do
            val <- liftIO $ atomically (setIfAvailable tvTypeIndex key VStream *> xAddSTM tvStreamMap key sId chunked now)
            pure $ either RStreamError RStreamId val
        XRange key range -> do
            vals <- liftIO (atomically (xRangeSTM tvStreamMap key range))
            pure $ RStreamValues vals
        XRead keys -> do
            vals <- liftIO (atomically (xReadSTM tvStreamMap keys))
            pure $ RKeyValues vals
        XReadBlock 0 key sid -> do
            sid' <- liftIO $ atomically (xResolveStreamIdSTM tvStreamMap key sid)
            vals <- liftIO (atomically (xReadBlockSTM tvStreamMap key sid'))
            pure $ (RKeyValues . singleton) vals
        XReadBlock tout key sid -> do
            sid' <- liftIO $ atomically (xResolveStreamIdSTM tvStreamMap key sid)
            vals <- liftIO (timeout (nominalDiffTimeToMicros tout) (atomically (xReadBlockSTM tvStreamMap key sid')))
            pure $ maybe RNullArray (RKeyValues . singleton) vals

-- TYPE index

getTypeSTM :: TVar TypeIndex -> Key -> STM (Maybe ValueType)
getTypeSTM tvIdx key = M.lookup key <$> readTVar tvIdx

availableSTM :: TVar TypeIndex -> Key -> RequiredType -> STM ()
availableSTM tvIdx key req = do
    idx <- readTVar tvIdx
    unless (checkAvailable (M.lookup key idx) req) $
        throwSTM WrongType

setTypeSTM :: TVar TypeIndex -> Key -> ValueType -> STM ()
setTypeSTM tvIdx key ty =
    modifyTVar' tvIdx (M.insert key ty)

setIfAvailable :: TVar TypeIndex -> Key -> ValueType -> STM ()
setIfAvailable tvIdx key ty = availableSTM tvIdx key (AbsentOr ty) *> setTypeSTM tvIdx key ty

-- StringStore Operations

setSTM :: TVar StringStore -> Key -> ByteString -> UTCTime -> Maybe NominalDiffTime -> STM ()
setSTM tv key val now mexpiry = modifyTVar' tv (EM.insert key val now mexpiry)

getSTM :: TVar StringStore -> Key -> UTCTime -> STM (Maybe ByteString)
getSTM tv key now = do
    m <- readTVar tv
    pure (EM.lookup key now m)

-- ListStore Operations

rpushSTM :: TVar ListStore -> Key -> [ByteString] -> STM Int
rpushSTM tv key xs = do
    m <- readTVar tv
    let m' = LM.append key xs m
        len = LM.lookupCount key m'
    writeTVar tv m'
    pure len

lpushSTM :: TVar ListStore -> Key -> [ByteString] -> STM Int
lpushSTM tv key xs = do
    m <- readTVar tv
    let m' = LM.revPrepend key xs m
        len = LM.lookupCount key m'
    writeTVar tv m'
    pure len

lpopSTM :: TVar ListStore -> Key -> STM (Maybe ByteString)
lpopSTM tv key = do
    m <- readTVar tv
    case LM.leftPop key m of
        Nothing -> undefined
        Just (x, m') -> do
            writeTVar tv m'
            pure $ Just x

lpopsSTM :: TVar ListStore -> Key -> Int -> STM [ByteString]
lpopsSTM tv key n = do
    m <- readTVar tv
    let (xs, m') = LM.leftPops key n m
    writeTVar tv m'
    pure xs

blpopSTM :: TVar ListStore -> Key -> STM [ByteString]
blpopSTM tv key = do
    m <- readTVar tv
    case LM.leftPop key m of
        Nothing -> retry
        Just (x, m') -> do
            writeTVar tv m'
            pure [key, x]

lrangeSTM :: TVar ListStore -> Key -> Range -> STM [ByteString]
lrangeSTM tv key range = do
    m <- readTVar tv
    pure (LM.list key range m)

llenSTM :: TVar ListStore -> Key -> STM Int
llenSTM tv key = do
    m <- readTVar tv
    pure (LM.lookupCount key m)

-- stream operations

xAddSTM :: TVar StreamStore -> Key -> StreamId -> [(ByteString, ByteString)] -> UTCTime -> STM (Either StreamMapError ConcreteStreamId)
xAddSTM tv key sid vals time = do
    m <- readTVar tv
    case SM.insert key sid vals time m of
        Left err -> pure $ Left err
        Right (csid, m') -> do
            writeTVar tv m'
            pure $ Right csid

xRangeSTM :: TVar StreamStore -> Key -> SM.XRange -> STM [SM.Value ByteString ByteString]
xRangeSTM tv key range = do
    m <- readTVar tv
    pure (SM.query key range m)

xReadSTM :: TVar StreamStore -> [(Key, ConcreteStreamId)] -> STM [(Key, [SM.Value ByteString ByteString])]
xReadSTM tv keys = do
    m <- readTVar tv
    pure $ map (\(k, sid) -> (k, SM.queryEx k sid m)) keys

xResolveStreamIdSTM :: TVar StreamStore -> Key -> XRStreamId -> STM ConcreteStreamId
xResolveStreamIdSTM tv key sid = do
    m <- readTVar tv
    case sid of
        Dollar -> pure $ SM.lookupLatest key m
        Concrete s -> pure s

xReadBlockSTM :: TVar StreamStore -> Key -> ConcreteStreamId -> STM (Key, [SM.Value ByteString ByteString])
xReadBlockSTM tv key sid = do
    mInner <- readTVar tv
    case SM.queryEx key sid mInner of
        [] -> retry
        xs -> pure (key, xs)

-- Command execution

handleRequest :: ByteString -> Redis ByteString
handleRequest bs = do
    logInfo ("Received: " <> show bs)
    resp <- liftEither $ first ParsingError $ decode bs
    logDebug ("Decoded: " <> show resp)
    cmd <- liftEither $ first ConversionError $ respToCommand resp
    logDebug ("Command: " <> show cmd)
    result <- runCmd cmd
    logDebug ("Execution result: " <> show result)
    let resultResp = resultToResp result
    encoded <- liftEither $ first EncodeError $ encode resultResp
    logInfo ("Encoded: " <> show encoded)
    pure encoded

logInfo :: (MonadReader Env m, MonadIO m) => String -> m ()
logInfo msg = do
    logger <- asks envLogger
    liftIO $ logger (\t -> toLogStr (show t <> "[INFO] " <> msg <> "\n"))

logDebug :: (MonadReader Env m, MonadIO m) => String -> m ()
logDebug msg = do
    logger <- asks envLogger
    liftIO $ logger (\t -> toLogStr (show t <> "[DEBUG] " <> msg <> "\n"))
