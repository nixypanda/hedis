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
    MasterAssignedReplicaId,
    replicationInfo,
    initMasterState,
    initReplicaState,
    generateReplicaId,
    acceptReplica,
    cleanupReplicaRegistry,
) where

import Control.Concurrent.Async (Async, async, cancel)
import Control.Concurrent.STM (TVar, atomically, modifyTVar, newTQueueIO, readTQueue, readTVarIO, writeTVar)
import Control.Concurrent.STM.TQueue (TQueue)
import Control.Concurrent.STM.TVar (newTVarIO)
import Control.Exception (Exception)
import Control.Monad (replicateM)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Char (ord)
import Data.Map (Map)
import Data.Map qualified as M
import Data.String (fromString)
import Network.Simple.TCP (Socket, send)
import System.Random (randomRIO)

import Command (CmdReplication (CmdFullResync), Command (RedRepl), CommandResult (RCmd))

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

-- For ease let's just assign integers
type MasterAssignedReplicaId = ByteString

type ReplicaRegistry = TVar (Map MasterAssignedReplicaId ReplicaConn)

initMasterState :: MasterConfig -> IO MasterState
initMasterState _ = do
    registry <- newTVarIO M.empty
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

alphabet :: [Char]
alphabet = ['0' .. '9'] ++ ['a' .. 'z']

generateReplicaId :: IO MasterAssignedReplicaId
generateReplicaId =
    BS.pack <$> replicateM 40 randomChar
  where
    randomChar = do
        i <- randomRIO (0, length alphabet - 1)
        pure (fromIntegral $ ord $ alphabet !! i)

acceptReplica :: MasterState -> Socket -> MasterAssignedReplicaId -> IO CommandResult
acceptReplica masterState sock clientId = do
    _ <- initReplica masterState sock clientId
    pure $ RCmd $ RedRepl $ CmdFullResync masterState.masterReplId masterState.masterReplOffset

initReplica :: MasterState -> Socket -> MasterAssignedReplicaId -> IO MasterAssignedReplicaId
initReplica masterState rcSocket clientId = do
    rcOffset <- newTVarIO (-1)
    rcQueue <- newTQueueIO
    rid <- generateReplicaId

    rcSender <- async $ replicaSender rcSocket masterState.rdbFile rcQueue
    let rc = MkReplicaConn{..}
    atomically $ modifyTVar masterState.replicaRegistry (M.insert clientId rc)

    pure rid

cleanupReplicaRegistry :: ReplicaRegistry -> IO ()
cleanupReplicaRegistry reg = do
    replicas <- readTVarIO reg
    mapM_ (cancel . (.rcSender)) (M.elems replicas)
    atomically $ writeTVar reg M.empty

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
