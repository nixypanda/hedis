{-# LANGUAGE OverloadedStrings #-}

module Redis (Env (..), Redis, RedisError (..), respToCommand, runCmd, runRedis) where

import Control.Concurrent.STM (TVar, atomically, modifyTVar')
import Control.Concurrent.STM.TVar (readTVarIO)
import Control.Monad.Except (ExceptT (..), MonadError)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader, ReaderT, asks)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (readInt)
import ExpiringMap (ExpiringMap)
import ExpiringMap qualified as EM
import System.Log.FastLogger (TimedFastLogger)
import Text.Parsec (ParseError)

import Data.Time (getCurrentTime)
import Resp (Resp (..))

-- Types

type MemDB = ExpiringMap ByteString ByteString
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
type Value = ByteString
type Expiry = Int

data Command
    = Ping
    | Echo ByteString
    | Set Key Value (Maybe Expiry)
    | Get Key
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
respToCommand r = Left $ "Conversion Error" <> show r

-- Execution

runCmd :: Command -> Redis Resp
runCmd Ping = pure $ Str "PONG"
runCmd (Echo xs) = pure $ BulkStr xs
runCmd (Set key val mexpiry) = do
    eMap <- asks db
    currTime <- liftIO getCurrentTime
    liftIO . atomically $ modifyTVar' eMap $ EM.insert key val currTime mexpiry
    pure $ Str "OK"
runCmd (Get key) = do
    tVarMap <- asks db
    currTime <- liftIO getCurrentTime
    rMap <- liftIO $ readTVarIO tVarMap
    let val = EM.lookup key currTime rMap
    case val of
        Just x -> pure $ BulkStr x
        Nothing -> pure NullBulk
