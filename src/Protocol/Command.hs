module Protocol.Command (
    Command (..),
    CmdSTM (..),
    CmdIO (..),
    CmdTransaction (..),
    Key,
    CmdReplication (..),
    ReplicaToMaster (..),
    MasterToReplica (..),
    SubInfo (..),
    StringCmd (..),
    ListCmd (..),
    StreamCmd (..),
    SubConfig (..),
    PubSub (..),
    SortedSetCmd (..),
    GeoCmd (..),
    ZScore (..),
    CmdAuth (..),
) where

import Data.ByteString (ByteString)
import Data.Time (NominalDiffTime)

import Data.Word (Word64)
import Geo.Types (Coordinates)
import StoreBackend.ListMap (Range (..))
import StoreBackend.StreamMap (ConcreteStreamId, XAddStreamId (..), XRange (..), XReadStreamId (..))

type Key = ByteString

data SubInfo = IReplication deriving (Show, Eq)
data SubConfig = ConfigDir | ConfigDbFilename deriving (Show, Eq)

data Command
    = RedSTM CmdSTM
    | RedIO CmdIO
    | RedTrans CmdTransaction
    | RedInfo (Maybe SubInfo)
    | RedConfig SubConfig
    | RedRepl CmdReplication
    | RedSub PubSub
    | CmdWait Int NominalDiffTime
    | RedAuth CmdAuth
    deriving (Show, Eq)

data CmdSTM
    = CmdPing
    | CmdEcho ByteString
    | CmdType Key
    | CmdKeys
    | STMString StringCmd
    | STMList ListCmd
    | STMStream StreamCmd
    | STMSortedSet SortedSetCmd
    | STMGeo GeoCmd
    deriving (Show, Eq)

data StringCmd
    = CmdSet Key ByteString (Maybe NominalDiffTime)
    | CmdGet Key
    | CmdIncr Key
    deriving (Show, Eq)

data ListCmd
    = CmdRPush Key [ByteString]
    | CmdLPush Key [ByteString]
    | CmdLPop Key (Maybe Int)
    | CmdLRange Key Range
    | CmdLLen Key
    deriving (Show, Eq)

data StreamCmd
    = CmdXAdd Key XAddStreamId [(ByteString, ByteString)]
    | CmdXRange Key XRange
    | CmdXRead [(Key, ConcreteStreamId)]
    deriving (Show, Eq)

data SortedSetCmd
    = CmdZAdd Key ZScore ByteString
    | CmdZRank Key ByteString
    | CmdZRange Key Range
    | CmdZCard Key
    | CmdZScore Key ByteString
    | CmdZRem Key ByteString
    deriving (Show, Eq)

data GeoCmd
    = CmdGeoAdd Key Coordinates ByteString
    | CmdGeoPos Key [ByteString]
    | CmdGeoDist Key ByteString ByteString
    | CmdGeoSearchByLonLatByRadius Key Coordinates Double -- everything is meters
    deriving (Show, Eq)

data CmdIO
    = CmdBLPop Key NominalDiffTime
    | CmdXReadBlock Key XReadStreamId NominalDiffTime
    deriving (Show, Eq)

data CmdTransaction = Multi | Exec | Discard
    deriving (Show, Eq)

data CmdReplication
    = CmdReplicaToMaster ReplicaToMaster
    | CmdMasterToReplica MasterToReplica
    deriving (Show, Eq)

data ReplicaToMaster
    = CmdReplConfListen Int
    | CmdReplConfCapabilities
    | CmdPSync ByteString Int
    | CmdReplConfAck Int
    deriving (Show, Eq)

data MasterToReplica
    = CmdReplConfGetAck
    deriving (Show, Eq)

data PubSub
    = CmdSubscribe ByteString
    | CmdPublish ByteString ByteString
    | CmdUnsubscribe ByteString
    deriving (Show, Eq)

data CmdAuth
    = CmdAclWhoAmI
    | CmdAclGetUser ByteString
    deriving (Show, Eq)

data ZScore = ZScore Double | GeoScore Word64
    deriving (Eq, Ord, Show)
