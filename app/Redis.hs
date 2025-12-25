{-# LANGUAGE OverloadedStrings #-}

module Redis (Env (..), Redis, RedisError (..), respToCommand, runCmd, runRedis) where

import Control.Concurrent.STM (
    TVar,
    atomically,
    modifyTVar',
    readTVar,
    retry,
    writeTVar,
 )
import Control.Concurrent.STM.TVar (readTVarIO)
import Control.Monad.Except (ExceptT (..), MonadError (throwError))
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader, ReaderT, asks)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (readInt)
import Data.Maybe (fromMaybe)
import Data.Time (getCurrentTime)
import ExpiringMap (ExpiringMap)
import ExpiringMap qualified as EM
import System.Log.FastLogger (TimedFastLogger)
import Text.Parsec (ParseError)

import Resp (Resp (..))

-- Types

data RedisValue
    = Simple ByteString
    | List [ByteString]
    deriving (Show, Eq)

type MemDB = ExpiringMap ByteString RedisValue

data RedisError
    = ParsingError ParseError
    | EncodeError String
    | EmptyBuffer
    | Unimplemented String
    | ConversionError String
    | InvalidCommand String
    deriving (Show)

data Env = MkEnv
    { db :: TVar MemDB
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
    | Set Key RedisValue (Maybe Expiry)
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
        Just t' -> pure $ Set key (Simple val) (Just $ fst t')
respToCommand (Array 3 [BulkStr "SET", BulkStr key, BulkStr val]) = pure $ Set key (Simple val) Nothing
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
respToCommand (Array 3 [BulkStr "BLPOP", BulkStr key, BulkStr timeout]) = case readInt timeout of
    Nothing -> Left "Invalid timeout"
    Just timeout' -> pure $ Blpop key (fst timeout')
respToCommand r = Left $ "Conversion Error" <> show r

redisValueToResp :: RedisValue -> Resp
redisValueToResp (Simple x) = BulkStr x
redisValueToResp (List [x]) = BulkStr x
redisValueToResp (List xs) = Array (length xs) $ map BulkStr xs

-- Execution

runCmd :: Command -> Redis Resp
runCmd cmd = do
    currTime <- liftIO getCurrentTime
    eMap <- asks db
    case cmd of
        Ping -> pure $ Str "PONG"
        (Echo xs) -> pure $ BulkStr xs
        (Set key val mexpiry) -> do
            liftIO . atomically $ modifyTVar' eMap $ EM.insert key val currTime mexpiry
            pure $ Str "OK"
        (Get key) -> do
            rMap <- liftIO $ readTVarIO eMap
            let val = EM.lookup key currTime rMap
            case val of
                Just x -> pure $ redisValueToResp x
                Nothing -> pure NullBulk
        (Rpush key xs) -> liftIO . atomically $ do
            rMap <- readTVar eMap
            let (rMap', len) = case EM.lookup key currTime rMap of
                    Just (Simple _) -> (EM.insert key (List xs) currTime Nothing rMap, length xs)
                    Just (List xs') -> (EM.insert key (List $ xs' ++ xs) currTime Nothing rMap, length $ xs' ++ xs)
                    Nothing -> (EM.insert key (List xs) currTime Nothing rMap, length xs)
            writeTVar eMap rMap'
            pure $ Int len
        (Lpush key xs) -> liftIO . atomically $ do
            rMap <- readTVar eMap
            let (rMap', len) = case EM.lookup key currTime rMap of
                    Just (Simple _) -> (EM.insert key (List xs) currTime Nothing rMap, length xs)
                    Just (List xs') -> (EM.insert key (List $ reverse xs ++ xs') currTime Nothing rMap, length $ xs' ++ xs)
                    Nothing -> (EM.insert key (List xs) currTime Nothing rMap, length xs)
            writeTVar eMap rMap'
            pure $ Int len
        (Lrange key start stop) -> do
            rMap <- liftIO $ readTVarIO eMap
            let val = EM.lookup key currTime rMap
            case val of
                Nothing -> pure $ redisValueToResp $ List []
                Just (Simple _) -> throwError $ InvalidCommand "Invalid command for Simple value"
                Just (List xs) -> pure $ redisValueToResp $ List $ slice start stop xs
        (Llen key) -> do
            rMap <- liftIO $ readTVarIO eMap
            let val = EM.lookup key currTime rMap
            case val of
                Nothing -> pure $ Int 0
                Just (Simple _) -> throwError $ InvalidCommand "Invalid command for Simple value"
                Just (List xs) -> pure $ Int $ length xs
        (Lpop key mLen) ->
            liftIO . atomically $ do
                rMap <- readTVar eMap
                let val = EM.lookup key currTime rMap
                case val of
                    Nothing -> pure NullBulk
                    Just (Simple _) -> pure NullBulk
                    Just (List []) -> pure NullBulk
                    Just (List xs) -> do
                        writeTVar eMap (EM.insert key (List ts) currTime Nothing rMap)
                        pure $ redisValueToResp $ List hs
                      where
                        (hs, ts) = splitAt n xs
                        n = fromMaybe 1 mLen
        (Blpop key timeout) -> liftIO . atomically $ do
            rMap <- readTVar eMap
            case EM.lookup key currTime rMap of
                Nothing -> retry
                Just (Simple _) -> retry
                Just (List []) -> retry
                Just (List (x : xs)) -> do
                    writeTVar eMap (EM.insert key (List xs) currTime Nothing rMap)
                    pure $ redisValueToResp $ List [key, x]

normalize :: Int -> Int -> Int
normalize len i
    | i < 0 = max 0 (len + i)
    | otherwise = i

slice :: Int -> Int -> [a] -> [a]
slice start stop xs =
    let len = length xs
        start' = normalize len start
        stop' = normalize len stop
        count = stop' - start' + 1
     in if count <= 0
            then []
            else take count (drop start' xs)
