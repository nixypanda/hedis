module Auth.Types where

import Data.ByteString (ByteString)

newtype UserProperty = MkUserProperty {flags :: [ByteString]}
    deriving (Show, Eq)
