module Store.StringStore (
    StringStore,
    emptySTM,
    setSTM,
    getSTM,
) where

import Control.Concurrent.STM (STM, TVar, modifyTVar', newTVar, readTVar)
import Data.ByteString (ByteString)
import Data.Time (NominalDiffTime, UTCTime)

import Command (Key)
import StoreBackend.ExpiringMap (ExpiringMap)
import StoreBackend.ExpiringMap qualified as EM

type StringStore = ExpiringMap Key ByteString

emptySTM :: STM (TVar StringStore)
emptySTM = newTVar EM.empty

setSTM :: TVar StringStore -> Key -> ByteString -> UTCTime -> Maybe NominalDiffTime -> STM ()
setSTM tv key val now mexpiry = modifyTVar' tv (EM.insert key val now mexpiry)

getSTM :: TVar StringStore -> Key -> UTCTime -> STM (Maybe ByteString)
getSTM tv key now = do
    m <- readTVar tv
    pure (EM.lookup key now m)
