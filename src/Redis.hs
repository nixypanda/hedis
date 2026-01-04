{-# LANGUAGE OverloadedStrings #-}

module Redis (runReplica, runMaster) where

import Control.Concurrent.Async (async)
import Control.Exception (throwIO)
import Control.Monad.Except (runExceptT)
import Control.Monad.Reader (ReaderT (runReaderT), liftIO)
import GHC.Conc.Sync (atomically)
import Network.Simple.TCP (HostPreference (HostAny), closeSock, connect, serve)
import System.Log.FastLogger (LogStr, toLogStr)

import Server.MasterLoop (runMasterLoop, runRdbLoad)
import Server.ReplicaLoop (runReplicaLoop, runReplication)
import Types.Redis
import Types.Replication

runReplica :: Env Replica -> Int -> ReplicaOf -> IO ()
runReplica env port replicaOf = do
    let (logInfo', _) = loggingFuncs (getLogger env)
    logInfo' $ "Starting replica server on port " <> show port

    _ <- async $ connect replicaOf.masterHost (show replicaOf.masterPort) $ \(sock, _) -> do
        logInfo' $ "Connected to master " <> "on (" <> show replicaOf.masterHost <> ":" <> show replicaOf.masterPort <> ")"
        runRedisIO env (runReplication sock)

    serve HostAny (show port) $ \(socket, address) -> do
        logInfo' $ "successfully connected client: " ++ show address
        clientState <- liftIO $ atomically $ newClient socket
        runRedisIO env (runReplicaLoop clientState)
        logInfo' "Closing connection"
        closeSock socket

runMaster :: Env Master -> Int -> IO ()
runMaster env port = do
    let (logInfo', _) = loggingFuncs (getLogger env)
    runRedisIO env runRdbLoad

    logInfo' $ "Starting master server on port " <> show port
    serve HostAny (show port) $ \(socket, address) -> do
        logInfo' $ "successfully connected client: " ++ show address
        clientState <- liftIO $ atomically $ newClient socket
        runRedisIO env (runMasterLoop clientState)
        logInfo' "Closing connection"
        closeSock socket

loggingFuncs :: (Show a) => ((a -> LogStr) -> t) -> (String -> t, String -> t)
loggingFuncs envLogger =
    let logInfo' msg = envLogger (\t -> toLogStr (show t <> "[INFO] " <> msg <> "\n"))
        logError' msg = envLogger (\t -> toLogStr (show t <> "[ERROR] " <> msg <> "\n"))
     in (logInfo', logError')

runRedisIO :: (HasLogger r) => Env r -> Redis r a -> IO ()
runRedisIO env act = do
    let (logInfo', _) = loggingFuncs (getLogger env)
    res <- runExceptT $ runReaderT (runRedis act) env
    case res of
        Left EmptyBuffer -> logInfo' "Client closed connection"
        Left err -> throwIO err
        Right _ -> pure ()
