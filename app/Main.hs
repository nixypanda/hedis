{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad.Except (MonadError (throwError), liftEither, runExceptT)
import Control.Monad.Reader (MonadReader, ReaderT (runReaderT), asks)
import GHC.Conc (TVar, newTVarIO)
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, recv, send, serve)
import Options.Applicative
import System.IO (BufferMode (NoBuffering), hSetBuffering, stderr, stdout)
import System.Log.FastLogger (toLogStr)
import System.Log.FastLogger.Date (simpleTimeFormat)

import Redis

-- network

bufferSize :: Int
bufferSize = 1024

clientLoop :: TVar TxState -> Socket -> Redis ()
clientLoop txVar socket = do
    mbs <- recv socket bufferSize
    ebs <- maybe (throwError EmptyBuffer) pure mbs
    encoded <- handleRequest txVar ebs
    send socket encoded
    clientLoop txVar socket

-- CLI parsing

newtype CliOptions = MkCliOptions
    { port :: Int
    }

optsParser :: Parser CliOptions
optsParser = do
    port <- option auto (long "port" <> metavar "PORT" <> value 6379 <> showDefault)
    pure MkCliOptions{..}

redis :: ParserInfo CliOptions
redis = info (optsParser <**> helper) (fullDesc <> progDesc "Hedis (A Redis toy-clone)")

-- main

main :: IO ()
main = do
    -- Disable output buffering
    hSetBuffering stdout NoBuffering
    hSetBuffering stderr NoBuffering
    cli <- execParser redis

    env <- mkNewEnv
    let logInfo' msg = env.envLogger (\t -> toLogStr (show t <> "[INFO] " <> msg <> "\n"))
        logError' msg = env.envLogger (\t -> toLogStr (show t <> "[ERROR] " <> msg <> "\n"))

    logInfo' $ "Redis server listening on port " ++ show cli.port

    serve HostAny (show cli.port) $ \(socket, address) -> do
        logInfo' $ "successfully connected client: " ++ show address
        txVar <- newTVarIO NoTx
        errOrRes <- runExceptT $ runReaderT (runRedis $ clientLoop txVar socket) env
        case errOrRes of
            Left EmptyBuffer -> logInfo' "Client closed connection"
            Left err -> logError' $ show err
            Right _ -> pure ()
        logInfo' "Closing connection"
        closeSock socket
