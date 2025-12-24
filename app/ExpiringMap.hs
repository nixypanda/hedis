{-# LANGUAGE RecordWildCards #-}

module ExpiringMap (empty, insert, lookup, ExpiringMap) where

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
type Expiry = Int

empty :: ExpiringMap k v
empty = M.empty

insert :: (Ord k) => k -> v -> UTCTime -> Maybe Expiry -> ExpiringMap k v -> ExpiringMap k v
insert k value storedWhen maybeExpiry db = do
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

milliSecontsToNominalDiffTime :: Int -> NominalDiffTime
milliSecontsToNominalDiffTime = secondsToNominalDiffTime . (/ 1000) . fromIntegral
