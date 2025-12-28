{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad.Except (MonadError (throwError), liftEither, runExceptT)
import Control.Monad.Reader (MonadReader, ReaderT (runReaderT), asks)
import GHC.Conc (TVar, newTVarIO)
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, connect, recv, send, serve)
import Options.Applicative
import System.IO (BufferMode (NoBuffering), hSetBuffering, stderr, stdout)
import System.Log.FastLogger (LogStr, toLogStr)
import System.Log.FastLogger.Date (simpleTimeFormat)

import Redis
import Replication

-- CLI parsing

data CliOptions = MkCliOptions
    { port :: Int
    , replicaOf :: Maybe ReplicaOf
    }

optsParser :: Parser CliOptions
optsParser = MkCliOptions <$> portParser <*> replicaOfParser

portParser :: Parser Int
portParser =
    option
        auto
        ( long "port"
            <> metavar "PORT"
            <> value 6379
            <> showDefault
            <> help "Port to listen on"
        )

replicaOfParser :: Parser (Maybe ReplicaOf)
replicaOfParser =
    optional $
        option
            (eitherReader parseReplicaOf)
            ( long "replicaof"
                <> metavar "\"HOST PORT\""
                <> help "Make this server a replica of the given master"
            )

parseReplicaOf :: String -> Either String ReplicaOf
parseReplicaOf s =
    case words s of
        [host, portStr] ->
            case reads portStr of
                [(port, "")] -> Right (ReplicaOf host port)
                _ -> Left "Invalid master port"
        _ -> Left "Expected \"<host> <port>\""

redis :: ParserInfo CliOptions
redis = info (optsParser <**> helper) (fullDesc <> progDesc "Hedis (A Redis toy-clone)")

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

-- main

main :: IO ()
main = do
    -- Disable output buffering
    hSetBuffering stdout NoBuffering
    hSetBuffering stderr NoBuffering
    cli <- execParser redis

    let role = maybe RRMaster RRReplica cli.replicaOf
        port = cli.port
    env <- mkNewEnv MkReplicationConfig{..}
    case cli.replicaOf of
        Nothing -> runMaster env cli.port
        Just r -> runReplica env cli.port r

runReplica :: Env -> Int -> ReplicaOf -> IO ()
runReplica env port replicaOf = do
    let (logInfo', logError') = loggingFuncs env.envLogger
    logInfo' $ "Starting replica server on port " <> show port
    connect replicaOf.masterHost (show replicaOf.masterPort) $ \(sock, _) -> do
        logInfo' $ "Connected to master " <> "on (" <> show replicaOf.masterHost <> ":" <> show replicaOf.masterPort <> ")"
        errOrRes <- runExceptT $ runReaderT (runRedis $ runReplication sock) env
        case errOrRes of
            Left EmptyBuffer -> logInfo' "Client closed connection"
            Left err -> logError' $ show err
            Right _ -> pure ()
    runServer env port

runMaster :: Env -> Int -> IO ()
runMaster env port = do
    let (logInfo', _) = loggingFuncs env.envLogger
    logInfo' $ "Starting master server on port " <> show port
    runServer env port

runServer :: Env -> Int -> IO ()
runServer env port = do
    let (logInfo', logError') = loggingFuncs env.envLogger
    serve HostAny (show port) $ \(socket, address) -> do
        logInfo' $ "successfully connected client: " ++ show address
        txVar <- newTVarIO NoTx
        errOrRes <- runExceptT $ runReaderT (runRedis $ clientLoop txVar socket) env
        case errOrRes of
            Left EmptyBuffer -> logInfo' "Client closed connection"
            Left err -> logError' $ show err
            Right _ -> pure ()
        logInfo' "Closing connection"
        closeSock socket

loggingFuncs :: (Show a) => ((a -> LogStr) -> t) -> (String -> t, String -> t)
loggingFuncs envLogger =
    let logInfo' msg = envLogger (\t -> toLogStr (show t <> "[INFO] " <> msg <> "\n"))
        logError' msg = envLogger (\t -> toLogStr (show t <> "[ERROR] " <> msg <> "\n"))
     in (logInfo', logError')
