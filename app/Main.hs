{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad.Except (runExceptT)
import Control.Monad.Reader (ReaderT (runReaderT))
import GHC.Conc (newTVarIO)
import Network.Simple.TCP (HostPreference (HostAny), closeSock, connect, serve)
import Options.Applicative (execParser)
import System.IO (BufferMode (NoBuffering), hSetBuffering, stderr, stdout)
import System.Log.FastLogger (LogStr, toLogStr)

import Cli
import Redis
import Replication

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

    serve HostAny (show port) $ \(socket, address) -> do
        logInfo' $ "successfully connected client: " ++ show address
        txState <- newTVarIO NoTx
        let clientState = MkClientState{..}
        errOrRes <- runExceptT $ runReaderT (runRedis $ clientLoopDiscard clientState socket) env
        case errOrRes of
            Left EmptyBuffer -> logInfo' "Client closed connection"
            Left err -> logError' $ show err
            Right _ -> pure ()
        logInfo' "Closing connection"
        closeSock socket

runMaster :: Env Master -> Int -> IO ()
runMaster env port = do
    let (logInfo', logError') = loggingFuncs (getLogger env)
    logInfo' $ "Starting master server on port " <> show port
    serve HostAny (show port) $ \(socket, address) -> do
        logInfo' $ "successfully connected client: " ++ show address
        txState <- newTVarIO NoTx
        let clientState = MkClientState{..}
        errOrRes <- runExceptT $ runReaderT (runRedis $ clientLoopWrite clientState socket) env
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
