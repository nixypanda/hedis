module Main (main) where

import Options.Applicative (execParser)
import System.IO (BufferMode (NoBuffering), hSetBuffering, stderr, stdout)

import Cli
import Redis
import Types.Redis
import Types.Replication

-- main

main :: IO ()
main = do
    -- Disable output buffering
    hSetBuffering stdout NoBuffering
    hSetBuffering stderr NoBuffering

    cli <- execParser cliArgsParser

    case cli.replicaOf of
        Nothing -> do
            env <- mkMasterEnv $ MkMasterConfig cli.rdbConfig
            runMaster env cli.port
        Just r -> do
            env <- mkReplicaEnv $ MkReplicaConfig r cli.port cli.rdbConfig
            runReplica env cli.port r
