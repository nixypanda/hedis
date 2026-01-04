module Store.SortedSet (
    SortedSetStore,
    ZScore (..),
    emptySTM,
    zaddSTM,
    zcardSTM,
    zscoreSTM,
    zrankSTM,
    zremSTM,
    zrangeSTM,
    geoPosSTM,
    getDistSTM,
    geoAddSTM,
    searchFromLonLatByRadiusSTM,
    showPretty,
) where

import Control.Concurrent.STM (STM, TVar, newTVar, readTVar, writeTVar)
import Data.ByteString (ByteString)
import Data.Map qualified as M
import Data.String (IsString (fromString))
import Data.Word (Word64)
import GHC.Float (castDoubleToWord64)

import Geo.Encoding (decode, encode)
import Geo.Types (Coordinates, coordinatesAreValid, haversineDistance)
import Store.Backend.SortedSetMap (SortedSetMap)
import Store.Backend.SortedSetMap qualified as SSS
import Store.Backend.TypeIndex (ValueType (..))
import Store.Type qualified as TS

type Key = ByteString

data ZScore = ZScore Double | GeoScore Word64
    deriving (Eq, Ord, Show)

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

geoAddSTM :: TVar TS.TypeIndex -> TVar SortedSetStore -> Key -> Coordinates -> ByteString -> STM (Either Coordinates Int)
geoAddSTM tvTypeIndex tvZSet key coords val =
    if coordinatesAreValid coords
        then do
            let score = encode coords
            res <- TS.setIfAvailable tvTypeIndex key VSortedSet *> zaddSTM key (GeoScore score) val tvZSet
            pure $ Right res
        else pure $ Left coords

searchFromLonLatByRadiusSTM :: (Ord k) => TVar (SortedSetMap k v ZScore) -> k -> Coordinates -> Double -> STM [v]
searchFromLonLatByRadiusSTM tvZSet key origin radius = do
    options <- SSS.range' key 0 (-1) <$> readTVar tvZSet
    let filtered = map snd . filter ((< radius) . haversineDistance origin . zscoreToGeo . fst) $ options
    pure filtered
