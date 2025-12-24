{-# LANGUAGE OverloadedStrings #-}

module Redis (Env (..), Redis, RedisError (..), respToCommand, runCmd, runRedis) where

import Control.Concurrent.STM (
    TVar,
    atomically,
    modifyTVar',
    readTVar,
    writeTVar,
 )
import Control.Concurrent.STM.TVar (readTVarIO)
import Control.Monad.Except (ExceptT (..), MonadError)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader, ReaderT, asks)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (readInt)
import Data.Time (getCurrentTime)
import ExpiringMap (ExpiringMap)
import ExpiringMap qualified as EM
import Resp (Resp (..))
import System.Log.FastLogger (TimedFastLogger)
import Text.Parsec (ParseError)

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
respToCommand r = Left $ "Conversion Error" <> show r

redisValueToResp :: RedisValue -> Resp
redisValueToResp (Simple x) = BulkStr x
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
