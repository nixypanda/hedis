{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Concurrent.STM (newTVarIO)
import Control.Monad.Except (MonadError (throwError), liftEither, runExceptT)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader, ReaderT (runReaderT), asks)
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString (ByteString)
import ExpiringMap qualified as EM
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, recv, send, serve)
import System.IO (BufferMode (NoBuffering), hSetBuffering, stderr, stdout)
import System.Log.FastLogger (LogType' (..), defaultBufSize, newTimeCache, newTimedFastLogger, toLogStr)
import System.Log.FastLogger.Date (simpleTimeFormat)

import Redis (Env (..), Redis, RedisError (..), respToCommand, runCmd, runRedis)
import Resp (decode, encode)

-- Command execution

handleRequest :: ByteString -> Redis ByteString
handleRequest bs = do
    logDebug ("Received: " <> show bs)
    resp <- liftEither $ first ParsingError $ decode bs
    logDebug ("Decoded: " <> show resp)
    cmd <- liftEither $ first ConversionError $ respToCommand resp
    logDebug ("Command: " <> show cmd)
    result <- runCmd cmd
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
