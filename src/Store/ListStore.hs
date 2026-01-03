module Store.ListStore (
    ListStore,
    emptySTM,
    runListStoreSTM,
    blpopSTM,
    -- testing
    rpushSTM,
    lpushSTM,
    lpopSTM,
    lpopsSTM,
    lrangeSTM,
    llenSTM,
) where

import Control.Concurrent.STM (STM, TVar, newTVar, readTVar, retry, writeTVar)
import Data.ByteString (ByteString)

import Protocol.Command
import Protocol.Result
import Store.TypeStore (TypeIndex)
import Store.TypeStore qualified as TS
import StoreBackend.ListMap (ListMap, Range)
import StoreBackend.ListMap qualified as LM
import StoreBackend.TypeIndex (ValueType (..))

type ListStore = ListMap Key ByteString

emptySTM :: STM (TVar ListStore)
emptySTM = newTVar LM.empty

rpushSTM :: TVar ListStore -> Key -> [ByteString] -> STM Int
rpushSTM tv key xs = do
    m <- readTVar tv
    let m' = LM.append key xs m
        len = LM.lookupCount key m'
    writeTVar tv m'
    pure len

lpushSTM :: TVar ListStore -> Key -> [ByteString] -> STM Int
lpushSTM tv key xs = do
    m <- readTVar tv
    let m' = LM.revPrepend key xs m
        len = LM.lookupCount key m'
    writeTVar tv m'
    pure len

lpopSTM :: TVar ListStore -> Key -> STM (Maybe ByteString)
lpopSTM tv key = do
    m <- readTVar tv
    case LM.leftPop key m of
        Nothing -> pure Nothing
        Just (x, m') -> do
            writeTVar tv m'
            pure $ Just x

lpopsSTM :: TVar ListStore -> Key -> Int -> STM [ByteString]
lpopsSTM tv key n = do
    m <- readTVar tv
    let (xs, m') = LM.leftPops key n m
    writeTVar tv m'
    pure xs

blpopSTM :: TVar ListStore -> Key -> STM [ByteString]
blpopSTM tv key = do
    m <- readTVar tv
    case LM.leftPop key m of
        Nothing -> retry
        Just (x, m') -> do
            writeTVar tv m'
            pure [key, x]

lrangeSTM :: TVar ListStore -> Key -> Range -> STM [ByteString]
lrangeSTM tv key range = do
    m <- readTVar tv
    pure (LM.list key range m)

llenSTM :: TVar ListStore -> Key -> STM Int
llenSTM tv key = do
    m <- readTVar tv
    pure (LM.lookupCount key m)

runListStoreSTM :: TVar TypeIndex -> TVar ListStore -> ListCmd -> STM Success
runListStoreSTM tvTypeIndex tvListMap cmd = case cmd of
    CmdRPush key xs -> do
        count <- TS.setIfAvailable tvTypeIndex key VList *> rpushSTM tvListMap key xs
        pure $ ReplyResp $ RespInt count
    CmdLPush key xs -> do
        count <- TS.setIfAvailable tvTypeIndex key VList *> lpushSTM tvListMap key xs
        pure $ ReplyResp $ RespInt count
    CmdLPop key Nothing -> do
        val <- lpopSTM tvListMap key
        pure $ ReplyResp $ RespBulk val
    CmdLPop key (Just mLen) -> do
        vals <- lpopsSTM tvListMap key mLen
        pure $ ReplyStrings vals
    CmdLRange key range -> do
        vals <- lrangeSTM tvListMap key range
        pure $ ReplyStrings vals
    CmdLLen key -> do
        len <- llenSTM tvListMap key
        pure $ ReplyResp $ RespInt len
