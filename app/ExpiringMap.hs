{-# LANGUAGE RecordWildCards #-}

module ExpiringMap (empty, insert, insertWithExpiry, lookup, ExpiringMap) where

import Data.Map.Strict (Map)
import Data.Map.Strict qualified as M
import Data.Time (NominalDiffTime, UTCTime (..), diffUTCTime, secondsToNominalDiffTime)
import Prelude hiding (lookup)

data StoredVal a = MkStoredVal
    { value :: !a
    , storedWhen :: !UTCTime
    , expiration :: !(Maybe NominalDiffTime)
    }
    deriving (Show)

type ExpiringMap k v = Map k (StoredVal v)
type Expiry = Integer

empty :: ExpiringMap k v
empty = M.empty

insert :: (Ord k) => k -> v -> UTCTime -> ExpiringMap k v -> ExpiringMap k v
insert k v t = _insert k v t Nothing

insertWithExpiry :: (Ord k) => k -> v -> UTCTime -> Expiry -> ExpiringMap k v -> ExpiringMap k v
insertWithExpiry k v t e = _insert k v t (Just e)

_insert :: (Ord k) => k -> v -> UTCTime -> Maybe Expiry -> ExpiringMap k v -> ExpiringMap k v
_insert k value storedWhen maybeExpiry db = do
    let expiration = milliSecontsToNominalDiffTime <$> maybeExpiry
    M.insert k MkStoredVal{..} db

lookup :: (Ord k) => k -> UTCTime -> ExpiringMap k v -> Maybe v
lookup k t db = M.lookup k db >>= getIfNotExpired t

getIfNotExpired :: UTCTime -> StoredVal v -> Maybe v
getIfNotExpired currentTime MkStoredVal{..} = case expiration of
    Nothing -> Just value
    Just expiry ->
        if currentTime `diffUTCTime` storedWhen < expiry
            then Just value
            else Nothing

milliSecontsToNominalDiffTime :: Integer -> NominalDiffTime
milliSecontsToNominalDiffTime = secondsToNominalDiffTime . (/ 1000) . fromIntegral
