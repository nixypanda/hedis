module Auth.Types (UserFlags (..), UserProperty (..)) where

newtype UserFlags = MkUserFlags {nopass :: Bool}
    deriving (Show, Eq)

newtype UserProperty = MkUserProperty {flags :: UserFlags}
    deriving (Show, Eq)
