module Wire.Class (ToResp (..), FromResp (..)) where

import Data.ByteString qualified as BS

import Protocol.Command (Command, MasterToReplica, ReplicaToMaster)
import Protocol.MasterCmd (MasterCommand, PropogationCmd)
import Protocol.Message (Message)
import Protocol.Result (Result, Success)
import Resp.Core (Resp, encode)
import Wire.Client.Command (cmdMasterToReplicaToResp, cmdReplicaToMasterToResp, cmdToResp, respToCmd, respToMasterToReplicaCmd, respToReplicaToMasterCmd)
import Wire.Client.Message (msgToResp, respToMsg)
import Wire.Client.Result (cmdResultToResp, respToResult, resultToResp)
import Wire.MasterCmd (masterCmdToResp, propogationCmdToResp, respToMasterCmd)

class ToResp a where
    toResp :: a -> Resp
    respBytes :: a -> Int
    respBytes = BS.length . encode . toResp

class FromResp a where
    fromResp :: Resp -> Either String a

instance ToResp Command where
    toResp = cmdToResp

instance FromResp Command where
    fromResp = respToCmd

instance FromResp Result where
    fromResp = respToResult

instance ToResp Result where
    toResp = resultToResp

instance FromResp MasterCommand where
    fromResp = respToMasterCmd

instance ToResp MasterCommand where
    toResp = masterCmdToResp

instance ToResp PropogationCmd where
    toResp = propogationCmdToResp

instance ToResp Message where
    toResp = msgToResp

instance FromResp Message where
    fromResp = respToMsg

instance FromResp ReplicaToMaster where
    fromResp = respToReplicaToMasterCmd

instance ToResp ReplicaToMaster where
    toResp = cmdReplicaToMasterToResp

instance ToResp Success where
    toResp = cmdResultToResp

instance ToResp MasterToReplica where
    toResp = cmdMasterToReplicaToResp

instance FromResp MasterToReplica where
    fromResp = respToMasterToReplicaCmd
