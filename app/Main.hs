{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad.Except (MonadError (throwError), liftEither, runExceptT)
import Control.Monad.Reader (MonadReader, ReaderT (runReaderT), asks)
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, recv, send, serve)
import System.IO (BufferMode (NoBuffering), hSetBuffering, stderr, stdout)
import System.Log.FastLogger (toLogStr)
import System.Log.FastLogger.Date (simpleTimeFormat)

import Redis

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

    env <- mkNewEnv
    let logInfo' msg = env.envLogger (\t -> toLogStr (show t <> "[INFO] " <> msg <> "\n"))
        logError' msg = env.envLogger (\t -> toLogStr (show t <> "[ERROR] " <> msg <> "\n"))

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
