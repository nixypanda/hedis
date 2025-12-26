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
    writeTVar,
 )
import Control.Monad.Except (ExceptT (..), MonadError, liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader, ReaderT, asks)
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString (ByteString)
import Data.Maybe (fromMaybe)
import Data.Time (UTCTime, getCurrentTime)
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
import Parsers (readFloatBS, readIntBS)
import Resp (Resp (..), decode, encode)

-- Types

data RedisError
    = ParsingError ParseError
    | EncodeError String
    | EmptyBuffer
    | Unimplemented String
    | ConversionError String
    | InvalidCommand String
    deriving (Show)

type Key = ByteString
type Expiry = Int
type ListStore = ListMap Key ByteString
type StringStore = ExpiringMap Key ByteString

data Env = MkEnv
    { stringStore :: TVar (ExpiringMap ByteString ByteString)
    , listStore :: TVar ListStore
    , envLogger :: TimedFastLogger
    }

newtype Redis a = MkRedis {runRedis :: ReaderT Env (ExceptT RedisError IO) a}
    deriving (Functor, Applicative, Monad, MonadError RedisError, MonadIO, MonadReader Env)

mkNewEnv :: IO Env
mkNewEnv = do
    stringStore <- newTVarIO EM.empty
    listStore <- newTVarIO LM.empty
    timeCache <- newTimeCache simpleTimeFormat
    (envLogger, _cleanup) <- newTimedFastLogger timeCache (LogStderr defaultBufSize)
    pure MkEnv{..}

-- Conversion

data Command
    = Ping
    | Echo ByteString
    | Set Key ByteString (Maybe Expiry)
    | Get Key
    | Rpush Key [ByteString]
    | Lpush Key [ByteString]
    | Lrange Key Int Int
    | Llen Key
    | Lpop Key (Maybe Int)
    | Blpop Key Int
    deriving (Show, Eq)

extractBulk :: Resp -> Either String ByteString
extractBulk (BulkStr x) = Right x
extractBulk r = Left $ "Invalit Type" <> show r

respToCommand :: Resp -> Either String Command
respToCommand (Array 1 [BulkStr "PING"]) = pure Ping
respToCommand (Array 2 [BulkStr "ECHO", BulkStr xs]) = pure $ Echo xs
respToCommand (Array 5 [BulkStr "SET", BulkStr key, BulkStr val, BulkStr "PX", BulkStr t]) = Set key val . Just <$> readIntBS t
respToCommand (Array 3 [BulkStr "SET", BulkStr key, BulkStr val]) = pure $ Set key val Nothing
respToCommand (Array 2 [BulkStr "GET", BulkStr key]) = pure $ Get key
respToCommand (Array _ ((BulkStr "RPUSH") : (BulkStr key) : vals)) = Rpush key <$> mapM extractBulk vals
respToCommand (Array _ ((BulkStr "LPUSH") : (BulkStr key) : vals)) = Lpush key <$> mapM extractBulk vals
respToCommand (Array 2 [BulkStr "LLEN", BulkStr key]) = pure $ Llen key
respToCommand (Array 2 [BulkStr "LPOP", BulkStr key]) = pure $ Lpop key Nothing
respToCommand (Array 3 [BulkStr "LPOP", BulkStr key, BulkStr len]) = Lpop key . Just <$> readIntBS len
respToCommand (Array 4 [BulkStr "LRANGE", BulkStr key, BulkStr start, BulkStr stop]) = Lrange key <$> readIntBS start <*> readIntBS stop
respToCommand (Array 3 [BulkStr "BLPOP", BulkStr key, BulkStr tout]) = Blpop key . secondsToMicros <$> readFloatBS tout
respToCommand r = Left $ "Conversion Error" <> show r

secondsToMicros :: Float -> Int
secondsToMicros = round . (* 1_000_000)

listToResp :: [ByteString] -> Resp
listToResp [x] = BulkStr x
listToResp xs = Array (length xs) $ map BulkStr xs

-- Execution

runCmd :: Command -> Redis Resp
runCmd cmd = do
    tvListMap <- asks listStore
    tvStringMap <- asks stringStore
    case cmd of
        Ping -> pure $ Str "PONG"
        Echo xs -> pure $ BulkStr xs
        Set key val mexpiry -> do
            now <- liftIO getCurrentTime
            liftIO . atomically $ setSTM tvStringMap key val now mexpiry
            pure (Str "OK")
        Get key -> do
            now <- liftIO getCurrentTime
            m <- liftIO . atomically $ getSTM tvStringMap key now
            pure $ maybe NullBulk BulkStr m
        Rpush key xs -> Int <$> liftIO (atomically $ rpushSTM tvListMap key xs)
        Lpush key xs -> Int <$> liftIO (atomically $ lpushSTM tvListMap key xs)
        Lpop key mLen -> listToResp <$> liftIO (atomically $ lpopSTM tvListMap key (fromMaybe 1 mLen))
        Blpop key 0 -> liftIO $ atomically (blpopSTM tvListMap key)
        Blpop key tout -> fromMaybe NullArray <$> liftIO (timeout tout (atomically $ blpopSTM tvListMap key))
        Lrange key start end -> do
            vals <- liftIO (atomically $ lrangeSTM tvListMap key MkRange{..})
            pure $ listToResp vals
        Llen key -> Int <$> liftIO (atomically $ llenSTM tvListMap key)

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

lpopSTM :: TVar ListStore -> Key -> Int -> STM [ByteString]
lpopSTM tv key n = do
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
