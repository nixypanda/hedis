{-# LANGUAGE OverloadedStrings #-}

module Replication (
    Replication,
    ReplicationRole (..),
    ReplicaOf (..),
    replicationInfo,
    initReplication,
) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (fromString)

data ReplicaOf = ReplicaOf
    { masterHost :: String
    , masterPort :: Int
    }
    deriving (Show, Eq)

data ReplicationRole = RRMaster | RRReplica ReplicaOf
    deriving (Show)

data MasterReplicationInfo = MkMasterReplicationInfo
    {replicationID :: ByteString, replicationOffset :: Int}
    deriving (Show)

data Replication
    = Master MasterReplicationInfo
    | Replica ReplicaOf
    deriving (Show)

initReplication :: ReplicationRole -> Replication
initReplication rr = case rr of
    RRMaster -> Master (MkMasterReplicationInfo "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb" 0)
    RRReplica ro -> Replica ro

replicationInfo :: Replication -> ByteString
replicationInfo (Master (MkMasterReplicationInfo{..})) =
    BS.intercalate
        "\r\n"
        [ "role:master"
        , "master_replid:" <> replicationID
        , "master_repl_offset:" <> fromString (show replicationOffset)
        ]
replicationInfo Replica{} = "role:slave"
