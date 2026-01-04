{-# LANGUAGE OverloadedStrings #-}

module Wire.Replication (
    masterCmdToResp,
    respToMasterCmd,
    propogationCmdToResp,
    cmdReplicaToMasterToResp,
    respToReplicaToMasterCmd,
    replicaToMasterToPretty,
    cmdMasterToReplicaToResp,
    respToMasterToReplicaCmd,
    masterToReplicaPretty,
) where

import Data.String (fromString)

import Data.ByteString (ByteString)
import Data.Scientific (floatingOrInteger)
import Geo.Types (Coordinates (MkCoordinates))
import Parsers (readFloatBS, readIntBS, readScientificBS)
import Protocol.Replication
import Resp.Core (Resp (..))
import Resp.Utils
import Store.SortedSetStore (ZScore (..))
import Store.StreamStoreParsing (readXAddStreamId, showXaddId)
import Time (millisToNominalDiffTime, nominalDiffTimeToMillis)

cmdMasterToReplicaToResp :: MasterToReplica -> Resp
cmdMasterToReplicaToResp CmdReplConfGetAck = Array 3 [BulkStr "REPLCONF", BulkStr "GETACK", BulkStr "*"]

cmdReplicaToMasterToResp :: ReplicaToMaster -> Resp
cmdReplicaToMasterToResp (CmdReplConfListen port) = Array 3 [BulkStr "REPLCONF", BulkStr "listening-port", BulkStr $ fromString $ show port]
cmdReplicaToMasterToResp CmdReplConfCapabilities = Array 3 [BulkStr "REPLCONF", BulkStr "capa", BulkStr "psync2"]
cmdReplicaToMasterToResp (CmdPSync sId s) = Array 3 [BulkStr "PSYNC", BulkStr sId, BulkStr $ fromString $ show s]
cmdReplicaToMasterToResp (CmdReplConfAck n) = Array 3 [BulkStr "REPLCONF", BulkStr "ACK", BulkStr $ fromString $ show n]
cmdReplicaToMasterToResp CmdReplicaToMasterPing = Array 1 [BulkStr "PING"]
masterCmdToResp :: MasterCommand -> Resp
masterCmdToResp MasterPing = Array 1 [BulkStr "PING"]
masterCmdToResp (ReplicationCommand CmdReplConfGetAck) = Array 3 [BulkStr "REPLCONF", BulkStr "GETACK", BulkStr "*"]
masterCmdToResp (PropogationCmd cmd) = propogationCmdToResp cmd

propogationCmdToResp :: PropogationCmd -> Resp
propogationCmdToResp (RCmdSet key val (Just t)) = Array 5 [BulkStr "SET", BulkStr key, BulkStr val, BulkStr "PX", BulkStr $ fromString $ show $ nominalDiffTimeToMillis t]
propogationCmdToResp (RCmdSet key val Nothing) = Array 3 [BulkStr "SET", BulkStr key, BulkStr val]
propogationCmdToResp (RCmdRPush key xs) = Array (length xs + 2) (BulkStr "RPUSH" : BulkStr key : map BulkStr xs)
propogationCmdToResp (RCmdLPush key xs) = Array (length xs + 2) (BulkStr "LPUSH" : BulkStr key : map BulkStr xs)
propogationCmdToResp (RCmdLPop key Nothing) = Array 2 [BulkStr "LPOP", BulkStr key]
propogationCmdToResp (RCmdLPop key (Just len)) = Array 3 [BulkStr "LPOP", BulkStr key, BulkStr $ fromString $ show len]
propogationCmdToResp (RCmdXAdd key sid kvs) = Array (3 + length kvs * 2) $ BulkStr "XADD" : BulkStr key : BulkStr (showXaddId sid) : concatMap (\(k, v) -> [BulkStr k, BulkStr v]) kvs
propogationCmdToResp (RCmdZAdd key score value) = Array 4 [BulkStr "ZADD", BulkStr key, BulkStr $ fromString $ show score, BulkStr value]
propogationCmdToResp (RCmdZRem key value) = Array 3 [BulkStr "ZREM", BulkStr key, BulkStr value]
propogationCmdToResp (RCmdGeoAdd key (MkCoordinates lat long) value) = Array 5 [BulkStr "GEOADD", BulkStr key, BulkStr $ fromString $ show long, BulkStr $ fromString $ show lat, BulkStr value]

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
-- Sorted Set
respToMasterCmd (Array 4 [BulkStr "ZADD", BulkStr k, BulkStr score, BulkStr v]) = do
    score' <- readScientificBS score
    case floatingOrInteger score' of
        Left double -> pure . PropogationCmd $ RCmdZAdd k (ZScore double) v
        Right (int :: Integer) -> pure . PropogationCmd $ RCmdZAdd k (GeoScore (fromIntegral int)) v
respToMasterCmd (Array 3 [BulkStr "ZREM", BulkStr k, BulkStr v]) = pure . PropogationCmd $ RCmdZRem k v
-- Geo
respToMasterCmd (Array 5 [BulkStr "GEOADD", BulkStr k, BulkStr lat, BulkStr long, BulkStr v]) = do
    lat' <- readFloatBS lat
    long' <- readFloatBS long
    pure $ PropogationCmd $ RCmdGeoAdd k (MkCoordinates lat' long') v
respToMasterCmd c = Left $ "Invalid Propogation Command: -> " <> show c

respToReplicaToMasterCmd :: Resp -> Either String ReplicaToMaster
respToReplicaToMasterCmd (Array 3 [BulkStr "REPLCONF", BulkStr "listening-port", BulkStr port]) = CmdReplConfListen <$> readIntBS port
respToReplicaToMasterCmd (Array 3 [BulkStr "REPLCONF", BulkStr "capa", BulkStr "psync2"]) = pure CmdReplConfCapabilities
respToReplicaToMasterCmd (Array 3 [BulkStr "REPLCONF", BulkStr "ACK", BulkStr n]) = CmdReplConfAck <$> readIntBS n
respToReplicaToMasterCmd (Array 3 [BulkStr "PSYNC", BulkStr sid, BulkStr offset]) = CmdPSync sid <$> readIntBS offset
-- Not handling PING so it falls through to the normal handling flow
-- respToReplicaToMasterCmd (Array 1 [BulkStr "PING"]) = pure CmdReplicaToMasterPing
respToReplicaToMasterCmd r = Left $ "Conversion Error: " <> show r

respToMasterToReplicaCmd :: Resp -> Either String MasterToReplica
respToMasterToReplicaCmd (Array 3 [BulkStr "REPLCONF", BulkStr "GETACK", BulkStr "*"]) = pure CmdReplConfGetAck
respToMasterToReplicaCmd r = Left $ "Conversion Error: " <> show r

-----------------

masterToReplicaPretty :: MasterToReplica -> ByteString
masterToReplicaPretty CmdReplConfGetAck = "REPLCONF"

replicaToMasterToPretty :: ReplicaToMaster -> ByteString
replicaToMasterToPretty (CmdReplConfListen{}) = "REPLCONF"
replicaToMasterToPretty CmdReplConfCapabilities = "REPLCONF"
replicaToMasterToPretty (CmdPSync{}) = "PSYNC"
replicaToMasterToPretty (CmdReplConfAck{}) = "REPLCONF"
replicaToMasterToPretty CmdReplicaToMasterPing = "PING"
