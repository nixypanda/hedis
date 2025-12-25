{-# LANGUAGE OverloadedStrings #-}

module Redis (Env (..), Redis, RedisError (..), respToCommand, runCmd, runRedis) where

import Control.Concurrent.STM (
    STM,
    TVar,
    atomically,
    modifyTVar',
    readTVar,
    retry,
    writeTVar,
 )
import Control.Concurrent.STM.TVar (readTVarIO)
import Control.Monad.Except (ExceptT (..), MonadError)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader, ReaderT, asks)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (readInt)
import Data.ByteString.Char8 qualified as BSC
import Data.Maybe (fromMaybe)
import Data.Time (getCurrentTime)
import System.Log.FastLogger (TimedFastLogger)
import System.Timeout (timeout)
import Text.Parsec (ParseError)

import ExpiringMap (ExpiringMap)
import ExpiringMap qualified as EM
import ListMap (ListMap, Range (..))
import ListMap qualified as LM
import Resp (Resp (..))

-- Types

data RedisError
    = ParsingError ParseError
    | EncodeError String
    | EmptyBuffer
    | Unimplemented String
    | ConversionError String
    | InvalidCommand String
    deriving (Show)

data Env = MkEnv
    { stringStore :: TVar (ExpiringMap ByteString ByteString)
    , listStore :: TVar (ListMap ByteString ByteString)
    , envLogger :: TimedFastLogger
    }

newtype Redis a = MkRedis {runRedis :: ReaderT Env (ExceptT RedisError IO) a}
    deriving (Functor, Applicative, Monad, MonadError RedisError, MonadIO, MonadReader Env)

-- Conversion

type Key = ByteString
type Expiry = Int

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

respToCommand :: Resp -> Either String Command
respToCommand (Array 1 [BulkStr "PING"]) = pure Ping
respToCommand (Array 2 [BulkStr "ECHO", BulkStr xs]) = pure $ Echo xs
respToCommand (Array 5 [BulkStr "SET", BulkStr key, BulkStr val, BulkStr "PX", BulkStr t]) =
    case readInt t of
        Nothing -> Left "Invalid integer"
        Just t' -> pure $ Set key val (Just $ fst t')
respToCommand (Array 3 [BulkStr "SET", BulkStr key, BulkStr val]) = pure $ Set key val Nothing
respToCommand (Array 2 [BulkStr "GET", BulkStr key]) = pure $ Get key
respToCommand (Array _ ((BulkStr "RPUSH") : (BulkStr key) : vals)) = pure $ Rpush key (map (\(BulkStr x) -> x) vals)
respToCommand (Array _ ((BulkStr "LPUSH") : (BulkStr key) : vals)) = pure $ Lpush key (map (\(BulkStr x) -> x) vals)
respToCommand (Array 2 [BulkStr "LLEN", BulkStr key]) = pure $ Llen key
respToCommand (Array 2 [BulkStr "LPOP", BulkStr key]) = pure $ Lpop key Nothing
respToCommand (Array 3 [BulkStr "LPOP", BulkStr key, BulkStr len]) = case readInt len of
    Nothing -> Left "Invalid integer"
    Just len' -> pure $ Lpop key (Just $ fst len')
respToCommand (Array 4 [BulkStr "LRANGE", BulkStr key, BulkStr start, BulkStr stop]) = case (readInt start, readInt stop) of
    (Just start', Just stop') -> pure $ Lrange key (fst start') (fst stop')
    v -> Left $ "Invalid Integers" <> show v
respToCommand (Array 3 [BulkStr "BLPOP", BulkStr key, BulkStr tout]) = pure $ Blpop key (secondsToMicros $ read @Float (BSC.unpack tout))
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
    case cmd of
        Ping -> pure $ Str "PONG"
        (Echo xs) -> pure $ BulkStr xs
        (Set key val mexpiry) -> do
            currTime <- liftIO getCurrentTime
            tvStringMap <- asks stringStore
            liftIO . atomically $ modifyTVar' tvStringMap $ EM.insert key val currTime mexpiry
            pure $ Str "OK"
        (Get key) -> do
            currTime <- liftIO getCurrentTime
            tvStringMap <- asks stringStore
            stringMap <- liftIO $ readTVarIO tvStringMap
            let val = EM.lookup key currTime stringMap
            case val of
                Just x -> pure $ BulkStr x
                Nothing -> pure NullBulk
        (Rpush key xs) -> liftIO . atomically $ do
            listMap <- readTVar tvListMap
            let listMap' = LM.append key xs listMap
                len = LM.lookupCount key listMap'
            writeTVar tvListMap listMap'
            pure $ Int len
        (Lpush key xs) -> liftIO . atomically $ do
            listMap <- readTVar tvListMap
            let listMap' = LM.revPrepend key xs listMap
                len = LM.lookupCount key listMap'
            writeTVar tvListMap listMap'
            pure $ Int len
        (Lrange key start end) -> do
            listMap <- liftIO $ readTVarIO tvListMap
            let vals = LM.list key MkRange{..} listMap
            pure $ listToResp vals
        (Llen key) -> do
            listMap <- liftIO $ readTVarIO tvListMap
            let count = LM.lookupCount key listMap
            pure $ Int count
        (Lpop key mLen) ->
            liftIO . atomically $ do
                listMap <- readTVar tvListMap
                let (xs, listMap') = LM.leftPops key (fromMaybe 1 mLen) listMap
                writeTVar tvListMap listMap'
                pure $ listToResp xs
        (Blpop key 0) ->
            liftIO $ atomically (blpopSTM key tvListMap)
        (Blpop key tout) -> do
            res <- liftIO $ timeout tout (atomically $ blpopSTM key tvListMap)
            pure (fromMaybe NullArray res)

blpopSTM :: Key -> TVar (ListMap Key ByteString) -> STM Resp
blpopSTM key tv = do
    m <- readTVar tv
    case LM.leftPop key m of
        Nothing -> retry
        Just (x, m') -> do
            writeTVar tv m'
            pure $ listToResp [key, x]
