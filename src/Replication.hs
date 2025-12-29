{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}

module Replication (
    Replication (..),
    ReplicaOf (..),
    ReplicationConfig (..),
    MasterConfig,
    ReplicaConfig (..),
    MasterState (..),
    ReplicaState (..),
    ReplicaConn (..),
    replicationInfo,
    initMasterState,
    initReplicaState,
    acceptReplica,
    cleanupReplicaRegistry,
    propagateWrite,
) where

import Control.Concurrent.Async (Async, async, cancel)
import Control.Concurrent.STM (
    STM,
    TQueue,
    TVar,
    atomically,
    modifyTVar,
    newTQueueIO,
    newTVarIO,
    readTQueue,
    readTVar,
    readTVarIO,
    writeTQueue,
    writeTVar,
 )
import Control.Exception (Exception)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (fromString)
import Network.Simple.TCP (Socket, send)

import Command (CmdReplication (CmdFullResync), CmdSTM, Command (..), cmdToResp)
import CommandResult (CommandResult (..))
import Resp (encode)

-- CLI flags ──▶ ReplicationConfig
--                 │
--                 ▼
--           Replication (Env)
--            /           \
--   MasterState       ReplicaState
--        │                  │
--     INFO               PSYNC / REPLCONF

-- Used in CLI parsing
data ReplicaOf = MkReplicaOf
    { masterHost :: String
    , masterPort :: Int
    }
    deriving (Show, Eq)

-- ReplicationConfig is stuff read from CLI
data ReplicationConfig = RCMaster MasterConfig | RCReplica ReplicaConfig
    deriving (Show)

type MasterConfig = ()
data ReplicaConfig = MkReplicaConfig {masterInfo :: ReplicaOf, localPort :: Int}
    deriving (Show)

-- Replication is what we store in the Env
data Replication
    = MkReplicationMaster MasterState
    | MkReplicationReplica ReplicaState

data MasterState = MkMasterState
    { masterReplId :: ByteString
    , masterReplOffset :: Int
    , replicaRegistry :: ReplicaRegistry
    , rdbFile :: FilePath
    }

data ReplicaState = MkReplicaState
    { masterInfo :: ReplicaOf
    , knownMasterRepl :: ByteString
    , replicaOffset :: Int
    , localPort :: Int
    }
    deriving (Show)

data ReplicaConn = MkReplicaConn
    { rcSocket :: Socket
    , rcOffset :: TVar Int
    , rcQueue :: TQueue ByteString
    , rcSender :: Async ()
    }

instance Show ReplicaConn where
    show MkReplicaConn{..} = "ReplicaConn " <> show rcSocket

data ReplicationError = ReplicaDisconnected
    deriving (Show, Exception)

type ReplicaRegistry = TVar [ReplicaConn]

initMasterState :: MasterConfig -> IO MasterState
initMasterState _ = do
    registry <- newTVarIO []
    pure $
        MkMasterState
            { masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
            , masterReplOffset = 0
            , replicaRegistry = registry
            , rdbFile = "./master.rdb"
            }

initReplicaState :: ReplicaConfig -> ReplicaState
initReplicaState MkReplicaConfig{..} =
    MkReplicaState
        { masterInfo = masterInfo
        , localPort = localPort
        , replicaOffset = -1
        , knownMasterRepl = "?"
        }

replicationInfo :: Replication -> ByteString
replicationInfo (MkReplicationMaster (MkMasterState{..})) =
    BS.intercalate
        "\r\n"
        [ "role:master"
        , "master_replid:" <> masterReplId
        , "master_repl_offset:" <> fromString (show masterReplOffset)
        ]
replicationInfo (MkReplicationReplica (MkReplicaState{})) = "role:slave"

propagateWrite :: Replication -> CmdSTM -> STM ()
propagateWrite repli cmd =
    case repli of
        MkReplicationMaster (MkMasterState{..}) -> do
            registry <- readTVar replicaRegistry
            mapM_ (\rc -> writeTQueue rc.rcQueue $ encode $ cmdToResp $ RedSTM cmd) registry
        MkReplicationReplica _ ->
            pure () -- replicas never propagate

acceptReplica :: MasterState -> Socket -> IO CommandResult
acceptReplica masterState sock = do
    _ <- initReplica masterState sock
    pure $ RCmd $ RedRepl $ CmdFullResync masterState.masterReplId masterState.masterReplOffset

initReplica :: MasterState -> Socket -> IO ()
initReplica masterState rcSocket = do
    rcOffset <- newTVarIO (-1)
    rcQueue <- newTQueueIO

    rcSender <- async $ replicaSender rcSocket masterState.rdbFile rcQueue
    let rc = MkReplicaConn{..}
    atomically $ modifyTVar masterState.replicaRegistry (rc :)
    pure ()

cleanupReplicaRegistry :: ReplicaRegistry -> IO ()
cleanupReplicaRegistry reg = do
    replicas <- readTVarIO reg
    mapM_ (cancel . (.rcSender)) replicas
    atomically $ writeTVar reg []

replicaSender :: Socket -> FilePath -> TQueue ByteString -> IO ()
replicaSender sock rdbFile q = do
    -- Phase 1: RDB snapshot
    rdbFileData <- readRdbFile rdbFile
    let respEncoded = "$" <> fromString (show $ BS.length rdbFileData) <> "\r\n" <> rdbFileData
    send sock respEncoded

    -- Phase 2: Write Command Propogation
    replicaSenderLoop sock q

replicaSenderLoop :: Socket -> TQueue ByteString -> IO ()
replicaSenderLoop sock q = do
    resp <- atomically $ readTQueue q
    send sock resp
    replicaSenderLoop sock q

readRdbFile :: FilePath -> IO ByteString
readRdbFile = BS.readFile
