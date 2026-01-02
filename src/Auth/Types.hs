module Auth.Types (UserFlags (..), UserProperty (..)) where

import Data.ByteString (ByteString)

newtype UserFlags = MkUserFlags {nopass :: Bool}
    deriving (Show, Eq)

data UserProperty = MkUserProperty {flags :: UserFlags, passwords :: [ByteString]}
    deriving (Show, Eq)
