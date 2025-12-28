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
    replicationInfo,
    initMaster,
    initReplica,
) where

import Control.Concurrent.STM (TVar)
import Control.Concurrent.STM.TVar (newTVarIO)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Map (Map)
import Data.Map qualified as M
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
    , masterReplOffset :: Int
    , replicaRegistry :: ReplicaRegistry
    }

data ReplicaState = MkReplicaState
    { masterInfo :: ReplicaOf
    , knownMasterRepl :: ByteString
    , replicaOffset :: Int
    , localPort :: Int
    }
    deriving (Show)

data ReplicaConn = ReplicaConn
    { rcSocket :: Socket
    , rcOffset :: TVar Int
    }

-- For ease let's just assign integers
type MasterAssignedReplicaId = Int

type ReplicaRegistry = TVar (Map MasterAssignedReplicaId ReplicaConn)

initMaster :: MasterConfig -> IO MasterState
initMaster _ = do
    registry <- newTVarIO M.empty
    pure $
        MkMasterState
            { masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
            , masterReplOffset = 0
            , replicaRegistry = registry
            }

initReplica :: ReplicaConfig -> ReplicaState
initReplica MkReplicaConfig{..} =
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
