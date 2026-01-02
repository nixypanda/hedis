module Auth.Types (
    UserFlags (..),
    UserProperty (..),
    Sha256 (..),
    hashPassword,
    sha256Hex,
) where

import Crypto.Hash (Digest, hash)
import Crypto.Hash.Algorithms (SHA256)
import Data.ByteArray (convert)
import Data.ByteArray.Encoding (Base (..), convertToBase)
import Data.ByteString (ByteString)

newtype UserFlags = MkUserFlags {nopass :: Bool}
    deriving (Show, Eq)

data UserProperty = MkUserProperty
    { flags :: UserFlags
    , passwords :: [Sha256]
    }
    deriving (Show, Eq)

newtype Sha256 = Sha256 {unSha256 :: ByteString}
    deriving (Eq, Ord, Show)

hashPassword :: ByteString -> Sha256
hashPassword = Sha256 . sha256

sha256 :: ByteString -> ByteString
sha256 bs = convert (hash bs :: Digest SHA256)

sha256Hex :: Sha256 -> ByteString
sha256Hex = convertToBase Base16 . unSha256
