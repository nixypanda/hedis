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
import Control.Monad.Except (ExceptT (..), MonadError, liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader, ReaderT, asks)
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString (ByteString)
import Data.List (singleton)
import Data.Map (Map)
import Data.Map qualified as M
import Data.Maybe (fromMaybe)
import Data.String (IsString (fromString))
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime, secondsToNominalDiffTime)
import ExpiringMap qualified as EM
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

import ExpiringMap (ExpiringMap)
import ListMap (ListMap, Range (..))
import ListMap qualified as LM
import Parsers (readConcreteStreamId, readDollarStreamId, readFloatBS, readIntBS, readStreamId, readXRange)
import Resp (Resp (..), decode, encode)
import StreamMap (ConcreteStreamId, StreamId, StreamMap, StreamMapError (..), XRStreamId (..))
import StreamMap qualified as SM
import Time (millisToNominalDiffTime, nominalDiffTimeToMicros)

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

type Key = ByteString
type Expiry = Int
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

-- Conversion (from Resp)

data Command
    = Ping
    | Echo ByteString
    | Type Key
    | Set Key ByteString (Maybe Expiry)
    | Get Key
    | Rpush Key [ByteString]
    | Lpush Key [ByteString]
    | Lrange Key Int Int
    | Llen Key
    | Lpop Key (Maybe Int)
    | Blpop Key NominalDiffTime
    | Xadd Key StreamId [(ByteString, ByteString)]
    | XRange Key SM.XRange
    | XRead [(Key, ConcreteStreamId)]
    | XReadBlock NominalDiffTime Key XRStreamId
    deriving (Show, Eq)

extractBulk :: Resp -> Either String ByteString
extractBulk (BulkStr x) = Right x
extractBulk r = Left $ "Invalit Type" <> show r

chunksOf2 :: [a] -> Either String [(a, a)]
chunksOf2 [] = Right []
chunksOf2 (x : y : xs) = ((x, y) :) <$> chunksOf2 xs
chunksOf2 _ = Left "Invalid chunk of 2"

respToCommand :: Resp -> Either String Command
respToCommand (Array 1 [BulkStr "PING"]) = pure Ping
respToCommand (Array 2 [BulkStr "ECHO", BulkStr xs]) = pure $ Echo xs
respToCommand (Array 2 [BulkStr "TYPE", BulkStr key]) = pure $ Type key
respToCommand (Array 5 [BulkStr "SET", BulkStr key, BulkStr val, BulkStr "PX", BulkStr t]) = Set key val . Just <$> readIntBS t
respToCommand (Array 3 [BulkStr "SET", BulkStr key, BulkStr val]) = pure $ Set key val Nothing
respToCommand (Array 2 [BulkStr "GET", BulkStr key]) = pure $ Get key
respToCommand (Array 2 [BulkStr "LLEN", BulkStr key]) = pure $ Llen key
respToCommand (Array 2 [BulkStr "LPOP", BulkStr key]) = pure $ Lpop key Nothing
respToCommand (Array 3 [BulkStr "LPOP", BulkStr key, BulkStr len]) = Lpop key . Just <$> readIntBS len
respToCommand (Array 4 [BulkStr "LRANGE", BulkStr key, BulkStr start, BulkStr stop]) = Lrange key <$> readIntBS start <*> readIntBS stop
respToCommand (Array 3 [BulkStr "BLPOP", BulkStr key, BulkStr tout]) = Blpop key . secondsToNominalDiffTime . realToFrac <$> readFloatBS tout
respToCommand (Array _ ((BulkStr "RPUSH") : (BulkStr key) : vals)) = Rpush key <$> mapM extractBulk vals
respToCommand (Array _ ((BulkStr "LPUSH") : (BulkStr key) : vals)) = Lpush key <$> mapM extractBulk vals
respToCommand (Array _ ((BulkStr "XADD") : (BulkStr key) : (BulkStr sId) : vals)) = do
    vals' <- mapM extractBulk vals
    chunked <- chunksOf2 vals'
    sId' <- readStreamId sId
    pure $ Xadd key sId' chunked
respToCommand (Array 4 [BulkStr "XRANGE", BulkStr key, BulkStr s, BulkStr e]) = XRange key <$> readXRange s e
respToCommand (Array _ (BulkStr "XREAD" : BulkStr "streams" : vals)) = do
    vals' <- mapM extractBulk vals
    let (keys, ids) = splitAt (length vals' `div` 2) vals'
    ids' <- mapM readConcreteStreamId ids
    pure $ XRead $ zip keys ids'
respToCommand (Array 6 [BulkStr "XREAD", BulkStr "block", BulkStr t, BulkStr "streams", BulkStr key, BulkStr sid]) = do
    t' <- millisToNominalDiffTime <$> readIntBS t
    sid' <- readDollarStreamId sid
    pure $ XReadBlock t' key sid'
respToCommand r = Left $ "Conversion Error" <> show r

-- Conversion (to Resp)

listToResp :: [ByteString] -> Resp
listToResp xs = Array (length xs) $ map BulkStr xs

streamIdToResp :: ConcreteStreamId -> Resp
streamIdToResp (ms, i) = BulkStr . fromString $ show ms <> "-" <> show i

arrayToResp :: [SM.Value ByteString ByteString] -> Resp
arrayToResp ar = Array (length ar) $ map valueToResp ar
  where
    valueToResp :: SM.Value ByteString ByteString -> Resp
    valueToResp v = Array 2 [streamIdToResp v.streamId, listToResp $ vals v.vals]
    vals :: [(ByteString, ByteString)] -> [ByteString]
    vals = concatMap (\(k, v) -> [k, v])

arrayOfArrayToResp :: [(Key, [SM.Value ByteString ByteString])] -> Resp
arrayOfArrayToResp ar = Array (length ar) $ map keyValsToResp ar

keyValsToResp :: (ByteString, [SM.Value ByteString ByteString]) -> Resp
keyValsToResp (k, vs) = Array 2 [BulkStr k, arrayToResp vs]

stremMapErrorToResp :: StreamMapError -> Resp
stremMapErrorToResp BaseStreamId = StrErr "ERR The ID specified in XADD must be greater than 0-0"
stremMapErrorToResp NotLargerId = StrErr "ERR The ID specified in XADD is equal or smaller than the target stream top item"

-- Execution

runCmd :: Command -> Redis Resp
runCmd cmd = do
    tvListMap <- asks listStore
    tvStringMap <- asks stringStore
    tvTypeIndex <- asks typeIndex
    tvStreamMap <- asks streamStore
    now <- liftIO getCurrentTime
    case cmd of
        Ping -> pure $ Str "PONG"
        Echo xs -> pure $ BulkStr xs
        Type x -> do
            ty <- liftIO $ atomically $ getTypeSTM tvTypeIndex x
            pure $ maybe (Str "none") (Str . toString) ty
        Set key val mexpiry -> do
            liftIO $ atomically (setIfAvailable tvTypeIndex key VString *> setSTM tvStringMap key val now mexpiry)
            pure (Str "OK")
        Get key -> do
            m <- liftIO $ atomically (getSTM tvStringMap key now)
            pure $ maybe NullBulk BulkStr m
        Rpush key xs -> Int <$> liftIO (atomically (setIfAvailable tvTypeIndex key VList *> rpushSTM tvListMap key xs))
        Lpush key xs -> Int <$> liftIO (atomically (setIfAvailable tvTypeIndex key VList *> lpushSTM tvListMap key xs))
        Lpop key Nothing -> maybe NullBulk BulkStr <$> liftIO (atomically (lpopSTM tvListMap key))
        Lpop key (Just mLen) -> listToResp <$> liftIO (atomically (lpopsSTM tvListMap key mLen))
        Blpop key 0 -> liftIO $ atomically (blpopSTM tvListMap key)
        Blpop key tout -> fromMaybe NullArray <$> liftIO (timeout (nominalDiffTimeToMicros tout) (atomically (blpopSTM tvListMap key)))
        Lrange key start end -> do
            vals <- liftIO (atomically (lrangeSTM tvListMap key MkRange{..}))
            pure $ listToResp vals
        Llen key -> Int <$> liftIO (atomically (llenSTM tvListMap key))
        Xadd key sId chunked -> do
            val <- liftIO $ atomically (setIfAvailable tvTypeIndex key VStream *> xAddSTM tvStreamMap key sId chunked now)
            pure $ either stremMapErrorToResp streamIdToResp val
        XRange key range -> do
            vals <- liftIO (atomically (xRangeSTM tvStreamMap key range))
            pure $ arrayToResp vals
        XRead keys -> do
            vals <- liftIO (atomically (xReadSTM tvStreamMap keys))
            pure $ arrayOfArrayToResp vals
        XReadBlock 0 key sid -> do
            sid' <- liftIO $ atomically (xResolveStreamIdSTM tvStreamMap key sid)
            vals <- liftIO (atomically (xReadBlockSTM tvStreamMap key sid'))
            pure $ (arrayOfArrayToResp . singleton) vals
        XReadBlock tout key sid -> do
            sid' <- liftIO $ atomically (xResolveStreamIdSTM tvStreamMap key sid)
            vals <- liftIO (timeout (nominalDiffTimeToMicros tout) (atomically (xReadBlockSTM tvStreamMap key sid')))
            pure $ maybe NullArray (arrayOfArrayToResp . singleton) vals

-- TYPE index

data ValueType = VString | VList | VStream deriving (Eq)

toString :: ValueType -> ByteString
toString VString = "string"
toString VList = "list"
toString VStream = "stream"

data RequiredType = AbsentOr ValueType | MustBe ValueType

getTypeSTM :: TVar TypeIndex -> Key -> STM (Maybe ValueType)
getTypeSTM tvIdx key = M.lookup key <$> readTVar tvIdx

availableSTM :: TVar TypeIndex -> Key -> RequiredType -> STM ()
availableSTM tvIdx key req = do
    idx <- readTVar tvIdx
    case (M.lookup key idx, req) of
        (Nothing, AbsentOr _) -> pure ()
        (Just t, AbsentOr t') | t == t' -> pure ()
        (Just t, MustBe t') | t == t' -> pure ()
        _ -> throwSTM WrongType

setTypeSTM :: TVar TypeIndex -> Key -> ValueType -> STM ()
setTypeSTM tvIdx key ty =
    modifyTVar' tvIdx (M.insert key ty)

setIfAvailable :: TVar TypeIndex -> Key -> ValueType -> STM ()
setIfAvailable tvIdx key ty = availableSTM tvIdx key (AbsentOr ty) *> setTypeSTM tvIdx key ty

-- StringStore Operations

setSTM :: TVar StringStore -> Key -> ByteString -> UTCTime -> Maybe Expiry -> STM ()
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

blpopSTM :: TVar ListStore -> Key -> STM Resp
blpopSTM tv key = do
    m <- readTVar tv
    case LM.leftPop key m of
        Nothing -> retry
        Just (x, m') -> do
            writeTVar tv m'
            pure $ listToResp [key, x]

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
    encoded <- liftEither $ first EncodeError $ encode result
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
