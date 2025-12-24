{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Concurrent.STM (TVar, atomically, modifyTVar', newTVarIO)
import Control.Concurrent.STM.TVar (readTVarIO)
import Control.Monad.Except (ExceptT (..), MonadError (throwError), liftEither, runExceptT)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader, ReaderT (runReaderT), asks)
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString (ByteString)
import Data.ByteString.Char8 (readInteger)
import Data.Maybe (fromJust)
import ExpiringMap (ExpiringMap)
import ExpiringMap qualified as EM
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, recv, send, serve)
import System.IO (BufferMode (NoBuffering), hSetBuffering, stderr, stdout)
import System.Log.FastLogger (LogType' (..), TimedFastLogger, defaultBufSize, newTimeCache, newTimedFastLogger, toLogStr)
import System.Log.FastLogger.Date (simpleTimeFormat)
import Text.Parsec (ParseError)

import Data.Time (getCurrentTime)
import Resp (Resp (..), decode, encode)

-- Command execution

type MemDB = ExpiringMap ByteString ByteString
data RedisError
    = ParsingError ParseError
    | EncodeError String
    | EmptyBuffer
    | Unimplemented String
    deriving (Show)

data Env = MkEnv
    { db :: TVar MemDB
    , envLogger :: TimedFastLogger
    }

newtype Redis a = MkRedis {runRedis :: ReaderT Env (ExceptT RedisError IO) a}
    deriving (Functor, Applicative, Monad, MonadError RedisError, MonadIO, MonadReader Env)

runCmd :: Resp -> Redis Resp
runCmd (Array 1 [BulkStr "PING"]) = pure $ Str "PONG"
runCmd (Array 2 [BulkStr "ECHO", BulkStr xs]) = pure $ BulkStr xs
runCmd (Array 5 [BulkStr "SET", BulkStr key, BulkStr val, BulkStr "PX", BulkStr t]) = do
    eMap <- asks db
    let milliSeconds = fst <$> readInteger t
    logDebug $ show milliSeconds
    currTime <- liftIO getCurrentTime
    liftIO . atomically $ modifyTVar' eMap $ EM.insertWithExpiry key val currTime (fromJust milliSeconds)
    pure $ Str "OK"
runCmd (Array 3 [BulkStr "SET", BulkStr key, BulkStr val]) = do
    eMap <- asks db
    currTime <- liftIO getCurrentTime
    liftIO . atomically $ modifyTVar' eMap (EM.insert key val currTime)
    logInfo ("SET " <> show key)
    pure $ Str "OK"
runCmd (Array 2 [BulkStr "GET", BulkStr key]) = do
    tVarMap <- asks db
    currTime <- liftIO getCurrentTime
    rMap <- liftIO $ readTVarIO tVarMap
    let val = EM.lookup key currTime rMap
    case val of
        Just x -> pure $ BulkStr x
        Nothing -> pure NullBulk
runCmd r = throwError $ Unimplemented $ "Unknown command: " <> show r

handleRequest :: ByteString -> Redis ByteString
handleRequest bs = do
    logDebug ("Received: " <> show bs)
    resp <- liftEither $ first ParsingError $ decode bs
    logDebug ("Decoded: " <> show resp)
    result <- runCmd resp
    logDebug ("Execution result: " <> show result)
    encoded <- liftEither $ first EncodeError $ encode result
    logDebug ("Encoded: " <> show encoded)
    pure encoded

-- network

bufferSize :: Int
bufferSize = 1024

clientLoop :: Socket -> Redis ()
clientLoop socket = do
    mbs <- recv socket bufferSize
    ebs <- maybe (throwError EmptyBuffer) pure mbs
    encoded <- handleRequest ebs
    send socket encoded
    clientLoop socket

main :: IO ()
main = do
    -- Disable output buffering
    hSetBuffering stdout NoBuffering
    hSetBuffering stderr NoBuffering

    timeCache <- newTimeCache simpleTimeFormat
    (logger, _cleanup) <- newTimedFastLogger timeCache (LogStderr defaultBufSize)
    tvDB <- newTVarIO EM.empty
    let env = MkEnv{db = tvDB, envLogger = logger}
        logInfo' msg = logger (\t -> toLogStr (show t <> "[INFO] " <> msg <> "\n"))
        logError' msg = logger (\t -> toLogStr (show t <> "[ERROR] " <> msg <> "\n"))

    let port = "6379"
    logInfo' $ "Redis server listening on port " ++ port

    serve HostAny port $ \(socket, address) -> do
        logInfo' $ "successfully connected client: " ++ show address
        errOrRes <- runExceptT $ runReaderT (runRedis $ clientLoop socket) env
        case errOrRes of
            Left EmptyBuffer -> logInfo' "Client closed connection"
            Left err -> logError' $ show err
            Right _ -> pure ()
        logInfo' "Closing connection"
        closeSock socket

-- logging helpers

logInfo :: (MonadReader Env m, MonadIO m) => String -> m ()
logInfo msg = do
    logger <- asks envLogger
    liftIO $ logger (\t -> toLogStr (show t <> "[INFO] " <> msg <> "\n"))

logDebug :: (MonadReader Env m, MonadIO m) => String -> m ()
logDebug msg = do
    logger <- asks envLogger
    liftIO $ logger (\t -> toLogStr (show t <> "[DEBUG] " <> msg <> "\n"))
