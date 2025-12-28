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
    initReplication,
) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (fromString)

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
    = ReplicationMaster MasterState
    | ReplicationReplica ReplicaState
    deriving (Show)

data MasterState = MkMasterState
    { masterReplId :: ByteString
    , masterReplOffset :: Int
    }
    deriving (Show)

data ReplicaState = MkReplicaState
    { masterInfo :: ReplicaOf
    , knownMasterRepl :: ByteString
    , replicaOffset :: Int
    , localPort :: Int
    }
    deriving (Show)

initReplication :: ReplicationConfig -> Replication
initReplication cfg =
    case cfg of
        RCMaster _ ->
            ReplicationMaster $
                MkMasterState
                    { masterReplId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
                    , masterReplOffset = 0
                    }
        RCReplica (MkReplicaConfig{..}) ->
            ReplicationReplica $
                MkReplicaState
                    { masterInfo = masterInfo
                    , localPort = localPort
                    , replicaOffset = -1
                    , knownMasterRepl = "?"
                    }

replicationInfo :: Replication -> ByteString
replicationInfo (ReplicationMaster (MkMasterState{..})) =
    BS.intercalate
        "\r\n"
        [ "role:master"
        , "master_replid:" <> masterReplId
        , "master_repl_offset:" <> fromString (show masterReplOffset)
        ]
replicationInfo (ReplicationReplica (MkReplicaState{})) = "role:slave"
