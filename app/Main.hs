{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad.Except (MonadError (throwError), runExceptT)
import Control.Monad.Reader (ReaderT (runReaderT))
import GHC.Conc (newTVarIO)
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, connect, recv, send, serve)
import Options.Applicative (execParser)
import System.IO (BufferMode (NoBuffering), hSetBuffering, stderr, stdout)
import System.Log.FastLogger (LogStr, toLogStr)

import Cli
import Redis
import Replication

-- network

bufferSize :: Int
bufferSize = 1024

clientLoop :: (HasLogger r, HasStores r, HasReplication r) => ClientState -> Socket -> Redis r ()
clientLoop clientState socket = do
    mbs <- recv socket bufferSize
    ebs <- maybe (throwError EmptyBuffer) pure mbs
    encoded <- handleRequest clientState ebs
    send socket encoded
    clientLoop clientState socket

-- main

main :: IO ()
main = do
    -- Disable output buffering
    hSetBuffering stdout NoBuffering
    hSetBuffering stderr NoBuffering
    cli <- execParser cliArgsParser
    case cli.replicaOf of
        Nothing -> do
            env <- mkMasterEnv ()
            runMaster env cli.port
        Just r -> do
            env <- mkReplicaEnv $ MkReplicaConfig r cli.port
            runReplica env cli.port r

runReplica :: Env Replica -> Int -> ReplicaOf -> IO ()
runReplica env port replicaOf = do
    let (logInfo', logError') = loggingFuncs (getLogger env)
    logInfo' $ "Starting replica server on port " <> show port
    connect replicaOf.masterHost (show replicaOf.masterPort) $ \(sock, _) -> do
        logInfo' $ "Connected to master " <> "on (" <> show replicaOf.masterHost <> ":" <> show replicaOf.masterPort <> ")"
        errOrRes <- runExceptT $ runReaderT (runRedis $ runReplication sock) env
        case errOrRes of
            Left EmptyBuffer -> logInfo' "Client closed connection"
            Left err -> logError' $ show err
            Right _ -> pure ()
    runServer env port

runMaster :: Env Master -> Int -> IO ()
runMaster env port = do
    let (logInfo', _) = loggingFuncs (getLogger env)
    logInfo' $ "Starting master server on port " <> show port
    runServer env port

runServer :: (HasLogger r, HasStores r, HasReplication r) => Env r -> Int -> IO ()
runServer env port = do
    let (logInfo', logError') = loggingFuncs (getLogger env)
    serve HostAny (show port) $ \(socket, address) -> do
        logInfo' $ "successfully connected client: " ++ show address
        txState <- newTVarIO NoTx
        clientId <- generateReplicaId
        let clientState = MkClientState{..}
        errOrRes <- runExceptT $ runReaderT (runRedis $ clientLoop clientState socket) env
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
