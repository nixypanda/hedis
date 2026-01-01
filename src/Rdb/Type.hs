module Rdb.Type where

import Data.ByteString (ByteString)
import Data.Word (Word64, Word8)

data RespEncodedRdb = MkRespEncodedRdb
    { len :: !Int
    , payload :: !ByteString
    }
    deriving (Show, Eq)

--

data RdbData = MkRdbData
    { header :: RdbHeader
    , metadata :: [RdbMetadata]
    , dbs :: [RdbDatabase]
    , checksum :: Word64
    }
    deriving (Show, Eq)

data RdbHeader = MkRdbHeader
    { magic :: ByteString
    , version :: ByteString
    }
    deriving (Show, Eq)

data MetaVal = MVInt Int | MVString ByteString
    deriving (Show, Eq)

data RdbMetadata = MkMetaAux ByteString MetaVal
    deriving (Show, Eq)

data RdbDatabase = MkRdbDatabase {identifier :: Int, store :: RdbStore}
    deriving (Show, Eq)

newtype RdbStore = MkHashStore HashTable
    deriving (Show, Eq)

data HashTable = MkHashTable
    { size :: Int
    , expirySize :: Int
    , table :: [(ByteString, ByteString)]
    }
    deriving (Show, Eq)

data RdbOpcode
    = OpAux
    | OpResizeDB
    | OpSelectDB
    | OpEOF
    | OpTypeString
    | OpUnknown Word8
    deriving (Show, Eq)
