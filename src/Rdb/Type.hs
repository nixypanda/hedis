module Rdb.Type (
    RespEncodedRdb (..),
) where

import Data.ByteString (ByteString)

data RespEncodedRdb = MkRespEncodedRdb
    { len :: !Int
    , payload :: !ByteString
    }
    deriving (Show, Eq)
