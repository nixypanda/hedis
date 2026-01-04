module Types.Command (
    Command (..),
    CmdSTM (..),
    CmdIO (..),
    CmdTransaction (..),
    Key,
    SubInfo (..),
    StringCmd (..),
    ListCmd (..),
    StreamCmd (..),
    SubConfig (..),
    PubSub (..),
    SortedSetCmd (..),
    GeoCmd (..),
    CmdAuth (..),
) where

import Data.ByteString (ByteString)
import Data.Time (NominalDiffTime)

import Auth (Sha256)
import Geo.Types (Coordinates)
import Store.Backend.ListMap (Range (..))
import Store.Backend.StreamMap (
    ConcreteStreamId,
    XAddStreamId (..),
    XRange (..),
    XReadStreamId (..),
 )
import Store.SortedSet (ZScore)

type Key = ByteString

data SubInfo = IReplication deriving (Show, Eq)
data SubConfig = ConfigDir | ConfigDbFilename deriving (Show, Eq)

data Command
    = RedSTM CmdSTM
    | RedIO CmdIO
    | RedTrans CmdTransaction
    | RedConfig SubConfig
    | RedSub PubSub
    | RedAuth CmdAuth
    | RedInfo (Maybe SubInfo)
    | CmdWait Int NominalDiffTime
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

data CmdTransaction
    = CmdMulti
    | CmdExec
    | CmdDiscard
    deriving (Show, Eq)

data PubSub
    = CmdSubscribe ByteString
    | CmdPublish ByteString ByteString
    | CmdUnsubscribe ByteString
    deriving (Show, Eq)

data CmdAuth
    = CmdAclWhoAmI
    | CmdAclGetUser ByteString
    | CmdAclSetUser Key Sha256
    | CmdAuth Key Sha256
    deriving (Show, Eq)
