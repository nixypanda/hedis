module Rdb.Type where

import Data.ByteString (ByteString)
import Data.Word (Word64)

data RespEncodedRdb = MkRespEncodedRdb
    { len :: !Int
    , payload :: !ByteString
    }
    deriving (Show, Eq)

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

data RdbMetadata = MkMetaAux MetaVal MetaVal
    deriving (Show, Eq)

newtype RdbDatabase = MkRdbDatabase [String]
    deriving (Show, Eq)
