module Store.PubSubStore where

import Control.Concurrent.STM (STM, TQueue, TVar, newTVar)
import Control.Concurrent.STM.TQueue (newTQueue)
import Data.ByteString (ByteString)
import Data.Map (Map)
import Data.Map qualified as M

import Protocol.Command (Key)

type PubSubStore = Map Key (TQueue ByteString)

emptySTM :: STM (TVar PubSubStore)
emptySTM = newTVar M.empty

addChannel :: Key -> PubSubStore -> STM PubSubStore
addChannel k pss = do
    case M.lookup k pss of
        Nothing -> do
            newQ <- newTQueue
            pure $ M.insert k newQ pss
        Just _ -> pure pss
