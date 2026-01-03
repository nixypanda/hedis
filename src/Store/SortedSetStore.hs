module Store.SortedSetStore (
    SortedSetStore,
    emptySTM,
    runSortedSetStoreSTM,
    runGeoStoreSTM,
) where

import Control.Concurrent.STM (STM, TVar, newTVar, readTVar, writeTVar)
import Data.ByteString (ByteString)
import Data.Map qualified as M
import Data.String (IsString (fromString))
import GHC.Float (castDoubleToWord64)

import Geo.Encoding (decode, encode)
import Geo.Types (Coordinates, coordinatesAreValid, haversineDistance)
import Protocol.Command
import Protocol.Result
import Store.TypeStore (TypeIndex)
import Store.TypeStore qualified as TS
import StoreBackend.ListMap (Range (..))
import StoreBackend.SortedSetMap (SortedSetMap)
import StoreBackend.SortedSetMap qualified as SSS
import StoreBackend.TypeIndex (ValueType (..))

showPretty :: ZScore -> ByteString
showPretty (ZScore score) = fromString $ show score
showPretty (GeoScore geo) = fromString $ show geo

type SortedSetStore = SortedSetMap ByteString ByteString ZScore

emptySTM :: STM (TVar SortedSetStore)
emptySTM = newTVar M.empty

zaddSTM :: ByteString -> ZScore -> ByteString -> TVar SortedSetStore -> STM Int
zaddSTM key score' val tv = do
    m <- readTVar tv
    let m' = SSS.insert key score' val m
    writeTVar tv m'
    pure $ SSS.count key m' - SSS.count key m

zcardSTM :: ByteString -> TVar SortedSetStore -> STM Int
zcardSTM key tv = SSS.count key <$> readTVar tv

zscoreSTM :: ByteString -> ByteString -> TVar SortedSetStore -> STM (Maybe ZScore)
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
        CmdZAdd key sc val ->
            RInt
                <$> (TS.setIfAvailable tvTypeIndex key VSortedSet *> zaddSTM key sc val tvZSet)
        CmdZRank key val -> RIntOrNil <$> zrankSTM key val tvZSet
        CmdZRange key (MkRange start end) -> RArraySimple <$> zrangeSTM key start end tvZSet
        CmdZCard key -> RInt <$> zcardSTM key tvZSet
        CmdZScore key val -> do
            mSc <- zscoreSTM key val tvZSet
            pure $ RBulk (showPretty <$> mSc)
        CmdZRem key val -> RInt . maybe 0 (const 1) <$> zremSTM key val tvZSet

--

geoPosSTM :: TVar SortedSetStore -> Key -> ByteString -> STM (Maybe Coordinates)
geoPosSTM tvZSet key val = do
    res <- zscoreSTM key val tvZSet
    pure (fmap zscoreToGeo res)

zscoreToGeo :: ZScore -> Coordinates
zscoreToGeo (GeoScore s) = decode s
zscoreToGeo (ZScore s) = decode . castDoubleToWord64 $ s

getDistSTM :: TVar SortedSetStore -> Key -> ByteString -> ByteString -> STM (Maybe Double)
getDistSTM tvZSet key1 val1 val2 = do
    firstScore <- zscoreSTM key1 val1 tvZSet
    secondScore <- zscoreSTM key1 val2 tvZSet
    let firstCoords = zscoreToGeo <$> firstScore
        secondCoords = zscoreToGeo <$> secondScore
    pure $ haversineDistance <$> firstCoords <*> secondCoords

runGeoStoreSTM ::
    TVar TypeIndex -> TVar SortedSetStore -> GeoCmd -> STM (Either CommandError CommandResult)
runGeoStoreSTM tvTypeIndex tvZSet cmd =
    case cmd of
        CmdGeoAdd key coords v -> do
            if coordinatesAreValid coords
                then do
                    let score = encode coords
                    res <-
                        TS.setIfAvailable tvTypeIndex key VSortedSet
                            *> zaddSTM key (GeoScore score) v tvZSet
                    pure $ Right $ RInt res
                else pure $ Left $ RInvalidLatLong coords
        CmdGeoPos key vals -> do
            res <- mapM (geoPosSTM tvZSet key) vals
            pure $ Right $ RCoordinates res
        CmdGeoDist key1 val1 val2 -> Right . RDoubleOrNil <$> getDistSTM tvZSet key1 val1 val2
        CmdGeoSearchByLonLatByRadius key origin radius -> do
            -- The sorted set implementation does not support fast range queries
            -- so, just filtering on all the elements.
            options <- SSS.range' key 0 (-1) <$> readTVar tvZSet
            let filtered = map snd . filter ((< radius) . haversineDistance origin . zscoreToGeo . fst) $ options
            pure $ Right $ RArraySimple filtered
