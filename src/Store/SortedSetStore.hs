module Store.SortedSetStore (SortedSetStore, emptySTM, runSortedSetStoreSTM) where

import Control.Concurrent.STM (STM, TVar, newTVar, readTVar, writeTVar)
import Data.ByteString (ByteString)
import Data.Map qualified as M
import Data.String (IsString (fromString))
import Data.Word (Word64)

import Protocol.Command
import Protocol.Result
import Store.TypeStore (TypeIndex)
import Store.TypeStore qualified as TS
import StoreBackend.ListMap (Range (..))
import StoreBackend.SortedSetMap (SortedSetMap)
import StoreBackend.SortedSetMap qualified as SSS
import StoreBackend.TypeIndex (ValueType (..))

data Score = ZScore Double | GeoScore Word64
    deriving (Eq, Ord)

showPretty :: Score -> ByteString
showPretty (ZScore score) = fromString $ show score
showPretty (GeoScore geo) = fromString $ show geo

type SortedSetStore = SortedSetMap ByteString ByteString Score

emptySTM :: STM (TVar SortedSetStore)
emptySTM = newTVar M.empty

zaddSTM :: ByteString -> Double -> ByteString -> TVar SortedSetStore -> STM Int
zaddSTM key score' val tv = do
    m <- readTVar tv
    let m' = SSS.insert key (ZScore score') val m
    writeTVar tv m'
    pure $ SSS.count key m' - SSS.count key m

zcardSTM :: ByteString -> TVar SortedSetStore -> STM Int
zcardSTM key tv = SSS.count key <$> readTVar tv

zscoreSTM :: ByteString -> ByteString -> TVar SortedSetStore -> STM (Maybe Score)
zscoreSTM key val tv = SSS.score key val <$> readTVar tv

zrankSTM :: ByteString -> ByteString -> TVar SortedSetStore -> STM (Maybe Int)
zrankSTM key val tv = SSS.rank key val <$> readTVar tv

zremSTM :: ByteString -> ByteString -> TVar SortedSetStore -> STM (Maybe ByteString)
zremSTM key val tv = do
    m <- readTVar tv
    let res = SSS.lookup key val m
        m' = SSS.remove key val m
    writeTVar tv m'
    pure res

zrangeSTM :: ByteString -> Int -> Int -> TVar SortedSetStore -> STM [ByteString]
zrangeSTM key start end tv = SSS.range key start end <$> readTVar tv

runSortedSetStoreSTM :: TVar TypeIndex -> TVar SortedSetStore -> SortedSetCmd -> STM CommandResult
runSortedSetStoreSTM tvTypeIndex tvZSet cmd =
    case cmd of
        CmdZAdd key sc val -> RInt <$> (TS.setIfAvailable tvTypeIndex key VSortedSet *> zaddSTM key sc val tvZSet)
        CmdZRank key val -> RIntOrNil <$> zrankSTM key val tvZSet
        CmdZRange key (MkRange start end) -> RArraySimple <$> zrangeSTM key start end tvZSet
        CmdZCard key -> RInt <$> zcardSTM key tvZSet
        CmdZScore key val -> do
            mSc <- zscoreSTM key val tvZSet
            pure $ RBulk (showPretty <$> mSc)
        CmdZRem key val -> RInt . maybe 0 (const 1) <$> zremSTM key val tvZSet
