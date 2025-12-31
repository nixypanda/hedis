{-# LANGUAGE OverloadedStrings #-}

module Replication.Command (runAndReplicateIO, runAndReplicateSTM, respToMasterCmd, MasterCommand (..), PropogationCmd (..), propogationCmdToCmdSTM) where

import Control.Concurrent.STM (STM, atomically, modifyTVar, readTVar, writeTQueue)
import Control.Monad.IO.Class (MonadIO (..))
import Data.String (fromString)
import Data.Time (NominalDiffTime, UTCTime)

import Command
import CommandResult
import Data.ByteString (ByteString)
import Execution.Base (runCmdIO, runCmdSTM)
import Parsers (readIntBS)
import Redis
import Replication.Config (MasterState (..), ReplicaConn (rcQueue), Replication (..))
import Resp.Command (respBytes)
import Resp.Core (Resp (..), encode)
import Resp.Utils
import Store.StreamStoreParsing (readXAddStreamId, showXaddId)
import StoreBackend.StreamMap (XAddStreamId)
import Time (millisToNominalDiffTime, nominalDiffTimeToMillis)

data MasterCommand
    = PropogationCmd PropogationCmd
    | ReplicationCommand MasterToReplica
    | MasterPing
    deriving (Show)

data PropogationCmd
    = RCmdLPop Key (Maybe Int)
    | RCmdRPush Key [ByteString]
    | RCmdLPush Key [ByteString]
    | RCmdSet Key ByteString (Maybe NominalDiffTime)
    | RCmdXAdd Key XAddStreamId [(ByteString, ByteString)]
    deriving (Show)

propogationCmdToCmdSTM :: PropogationCmd -> CmdSTM
propogationCmdToCmdSTM (RCmdSet key val (Just t)) = STMString $ CmdSet key val (Just t)
propogationCmdToCmdSTM (RCmdSet key val Nothing) = STMString $ CmdSet key val Nothing
propogationCmdToCmdSTM (RCmdRPush key xs) = STMList $ CmdRPush key xs
propogationCmdToCmdSTM (RCmdLPush key xs) = STMList $ CmdLPush key xs
propogationCmdToCmdSTM (RCmdLPop key l) = STMList $ CmdLPop key l
propogationCmdToCmdSTM (RCmdXAdd key sid kvs) = STMStream $ CmdXAdd key sid kvs

replicateIOCmdAs :: CmdIO -> CommandResult -> Maybe PropogationCmd
replicateIOCmdAs (CmdBLPop k 0) (RArraySimple _) = Just $ RCmdLPop k (Just 1)
replicateIOCmdAs (CmdBLPop _ 0) _ = error "Incorrect combination"
replicateIOCmdAs (CmdBLPop _ _) RArrayNull = Nothing
replicateIOCmdAs (CmdBLPop k _) (RArraySimple _) = Just $ RCmdLPop k (Just 1)
replicateIOCmdAs (CmdBLPop{}) _ = error "Incorrect combination"
replicateIOCmdAs (CmdXReadBlock{}) _ = Nothing

replicateSTMCmdAs :: CmdSTM -> CommandResult -> Maybe PropogationCmd
replicateSTMCmdAs (STMString c) cr = replicateStringCmd c cr
replicateSTMCmdAs (STMList c) cr = replicateListCmd c cr
replicateSTMCmdAs (STMStream c) cr = replicateStreamCmd c cr
replicateSTMCmdAs CmdPing _ = Nothing
replicateSTMCmdAs (CmdEcho{}) _ = Nothing
replicateSTMCmdAs (CmdType{}) _ = Nothing

replicateStreamCmd :: StreamCmd -> CommandResult -> Maybe PropogationCmd
replicateStreamCmd (CmdXAdd k sid kvs) _ = Just $ RCmdXAdd k sid kvs
replicateStreamCmd (CmdXRead{}) _ = Nothing
replicateStreamCmd (CmdXRange{}) _ = Nothing

replicateListCmd :: ListCmd -> CommandResult -> Maybe PropogationCmd
replicateListCmd (CmdRPush k vs) _ = Just $ RCmdRPush k vs
replicateListCmd (CmdLPush k vs) _ = Just $ RCmdLPush k vs
replicateListCmd (CmdLPop k mcount) _ = Just $ RCmdLPop k mcount
replicateListCmd (CmdLRange{}) _ = Nothing
replicateListCmd (CmdLLen{}) _ = Nothing

replicateStringCmd :: StringCmd -> CommandResult -> Maybe PropogationCmd
replicateStringCmd (CmdSet k v t) _ = Just $ RCmdSet k v t
replicateStringCmd (CmdGet{}) _ = Nothing
replicateStringCmd (CmdIncr k) (RInt v) = Just $ RCmdSet k (fromString $ show v) Nothing
replicateStringCmd c@(CmdIncr _) cr = error $ "Incorrect combination: Command - " <> show c <> ", result - " <> show cr

propogationCmdToResp :: PropogationCmd -> Resp
propogationCmdToResp (RCmdSet key val (Just t)) = Array 5 [BulkStr "SET", BulkStr key, BulkStr val, BulkStr "PX", BulkStr $ fromString $ show $ nominalDiffTimeToMillis t]
propogationCmdToResp (RCmdSet key val Nothing) = Array 3 [BulkStr "SET", BulkStr key, BulkStr val]
propogationCmdToResp (RCmdRPush key xs) = Array (length xs + 2) (BulkStr "RPUSH" : BulkStr key : map BulkStr xs)
propogationCmdToResp (RCmdLPush key xs) = Array (length xs + 2) (BulkStr "LPUSH" : BulkStr key : map BulkStr xs)
propogationCmdToResp (RCmdLPop key Nothing) = Array 2 [BulkStr "LPOP", BulkStr key]
propogationCmdToResp (RCmdLPop key (Just len)) = Array 3 [BulkStr "LPOP", BulkStr key, BulkStr $ fromString $ show len]
propogationCmdToResp (RCmdXAdd key sid kvs) = Array (3 + length kvs * 2) $ BulkStr "XADD" : BulkStr key : BulkStr (showXaddId sid) : concatMap (\(k, v) -> [BulkStr k, BulkStr v]) kvs

respToMasterCmd :: Resp -> Either String MasterCommand
respToMasterCmd (Array 1 [BulkStr "PING"]) = pure MasterPing
respToMasterCmd (Array 3 [BulkStr "REPLCONF", BulkStr "GETACK", BulkStr "*"]) = pure $ ReplicationCommand CmdReplConfGetAck
respToMasterCmd (Array 5 [BulkStr "SET", BulkStr key, BulkStr val, BulkStr "PX", BulkStr t]) = do
    expiry <- readIntBS t
    let expiry' = millisToNominalDiffTime expiry
    pure . PropogationCmd $ RCmdSet key val (Just expiry')
respToMasterCmd (Array 3 [BulkStr "SET", BulkStr key, BulkStr val]) =
    pure . PropogationCmd $ RCmdSet key val Nothing
respToMasterCmd (Array 2 [BulkStr "LPOP", BulkStr key]) = pure . PropogationCmd $ RCmdLPop key Nothing
respToMasterCmd (Array 3 [BulkStr "LPOP", BulkStr key, BulkStr len]) =
    PropogationCmd . RCmdLPop key . Just <$> readIntBS len
respToMasterCmd (Array _ ((BulkStr "RPUSH") : (BulkStr key) : vals)) =
    PropogationCmd . RCmdRPush key <$> mapM extractBulk vals
respToMasterCmd (Array _ ((BulkStr "LPUSH") : (BulkStr key) : vals)) =
    PropogationCmd . RCmdLPush key <$> mapM extractBulk vals
-- Stream Store
respToMasterCmd (Array _ ((BulkStr "XADD") : (BulkStr key) : (BulkStr sId) : vals)) = do
    vals' <- mapM extractBulk vals
    chunked <- chunksOf2 vals'
    sId' <- readXAddStreamId sId
    pure $ PropogationCmd $ RCmdXAdd key sId' chunked
respToMasterCmd c = Left $ "Invalid Propogation Command: -> " <> show c

propogationCmdBytes :: PropogationCmd -> Int
propogationCmdBytes = respBytes . propogationCmdToResp

runAndReplicateSTM :: (HasReplication r, HasStores r) => Env r -> UTCTime -> CmdSTM -> STM (Either CommandError CommandResult)
runAndReplicateSTM env now cmd = do
    res <- runCmdSTM env now cmd
    case replicateSTMCmdAs cmd <$> res of
        Right (Just cmd') -> do
            propagateWrite (getReplication env) cmd'
            modifyTVar (getOffset env) (+ propogationCmdBytes cmd')
            pure res
        _ -> pure res

-- The run and adding to queue is not atomic
runAndReplicateIO :: (HasStores r, HasReplication r) => Env r -> CmdIO -> Redis r CommandResult
runAndReplicateIO env cmd = do
    res <- runCmdIO cmd
    case replicateIOCmdAs cmd res of
        Nothing -> pure res
        Just cmd' -> do
            liftIO $ atomically $ do
                propagateWrite (getReplication env) cmd'
                modifyTVar (getOffset env) (+ propogationCmdBytes cmd')
            pure res

propagateWrite :: Replication -> PropogationCmd -> STM ()
propagateWrite repli cmd =
    case repli of
        MkReplicationMaster (MkMasterState{..}) -> do
            registry <- readTVar replicaRegistry
            mapM_ (\rc -> writeTQueue rc.rcQueue $ encode $ propogationCmdToResp cmd) registry
        MkReplicationReplica _ ->
            pure () -- replicas never propagate
