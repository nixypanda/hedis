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
    ReplicaRegistry,
    replicationInfo,
    initMasterState,
    initReplicaState,
) where

import Control.Concurrent.Async (Async)
import Control.Concurrent.STM (
    TQueue,
    TVar,
    newTVarIO,
    readTVarIO,
 )
import Control.Exception (Exception)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (fromString)
import Network.Simple.TCP (Socket)

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
    , masterReplOffset :: TVar Int
    , replicaRegistry :: ReplicaRegistry
    , rdbFile :: FilePath
    }

data ReplicaState = MkReplicaState
    { masterInfo :: ReplicaOf
    , knownMasterRepl :: TVar ByteString
    , replicaOffset :: TVar Int
    , localPort :: Int
    }

data ReplicaConn = MkReplicaConn
    { rcSocket :: Socket
    , rcOffset :: TVar Int
    , rcAuxCmdOffset :: TVar Int
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
    replicaRegistry <- newTVarIO []
    masterReplOffset' <- newTVarIO 0
    pure $
        MkMasterState
            { masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
            , masterReplOffset = masterReplOffset'
            , replicaRegistry = replicaRegistry
            , rdbFile = "./master.rdb"
            }

initReplicaState :: ReplicaConfig -> IO ReplicaState
initReplicaState MkReplicaConfig{..} = do
    replicaOffset <- newTVarIO (-1)
    knownMasterRepl <- newTVarIO "?"
    pure $ MkReplicaState{..}

replicationInfo :: Replication -> IO ByteString
replicationInfo (MkReplicationMaster (MkMasterState{..})) = do
    offset <- readTVarIO masterReplOffset
    pure $
        BS.intercalate
            "\r\n"
            [ "role:master"
            , "master_replid:" <> masterReplId
            , "master_repl_offset:" <> fromString (show offset)
            ]
replicationInfo (MkReplicationReplica (MkReplicaState{})) = pure "role:slave"
