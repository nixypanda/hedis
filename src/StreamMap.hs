module StreamMap (
    ConcreteStreamId,
    StreamId (..),
    IdSeq (..),
    StreamMap,
    StreamMapError (..),
    Value (..),
    XRange (..),
    XStart (..),
    XEnd (..),
    XStreamId,
    (!),
    empty,
    fromList,
    insert,
    query,
    queryEx,
) where

import Data.List (unsnoc)
import Data.Map (Map)
import Data.Map qualified as M
import Data.Maybe (fromJust, fromMaybe)
import Data.Time (UTCTime)

import Time (utcToMillis)

-- Stream Id

data StreamId = AutoId | ExplicitId Int IdSeq
    deriving (Show, Eq)

data IdSeq = Seq Int | SeqAuto
    deriving (Show, Eq)

type ConcreteStreamId = (Int, Int)

data Value k v = MkValue
    { streamId :: !ConcreteStreamId
    , vals :: ![(k, v)]
    }
    deriving (Eq, Ord, Show)

-- Stream Error

data StreamMapError = BaseStreamId | NotLargerId
    deriving (Show, Eq)

-- Xrange

type XStreamId = (Int, Maybe Int)

data XStart = XS XStreamId | XMinus deriving (Show, Eq)
data XEnd = XE XStreamId | XPlus deriving (Show, Eq)

data XRange = MkXrange {start :: !XStart, end :: !XEnd}
    deriving (Show, Eq)

xrangeToConcrete :: XRange -> (ConcreteStreamId, ConcreteStreamId)
xrangeToConcrete (MkXrange{..}) = (xStartToConcrete start, xEndToConcrete end)

xEndToConcrete :: XEnd -> ConcreteStreamId
xEndToConcrete XPlus = endId
xEndToConcrete (XE (ts, Just ms)) = (ts, ms)
xEndToConcrete (XE (ts, Nothing)) = baseEnd ts

xStartToConcrete :: XStart -> ConcreteStreamId
xStartToConcrete XMinus = baseId
xStartToConcrete (XS (ts, Just ms)) = (ts, ms)
xStartToConcrete (XS (ts, Nothing)) = baseStart ts

baseId :: ConcreteStreamId
baseId = (0, 0)

endId :: ConcreteStreamId
endId = (maxBound, maxBound)

baseStart :: Int -> ConcreteStreamId
baseStart ts = (ts, 0)

baseEnd :: Int -> ConcreteStreamId
baseEnd ts = (ts, maxBound)

-- Stream Map

empty :: StreamMap k ik a
empty = M.empty

fromList :: (Ord k) => [(k, a)] -> Map k a
fromList = M.fromList

(!) :: (Ord k) => Map k a -> k -> a
m ! i = fromJust $ M.lookup i m

type StreamMap k ik v = Map k [Value ik v]

insert :: (Ord k) => k -> StreamId -> [(ik, v)] -> UTCTime -> StreamMap k ik v -> Either StreamMapError (ConcreteStreamId, StreamMap k ik v)
insert key sid values time sMap
    | streamId == baseId = Left BaseStreamId
    | Nothing <- mxs = okInsert [] streamId
    | Just [] <- mxs = okInsert [] streamId
    | Just xs <- mxs, Just (_, lastVal) <- unsnoc xs, lastVal.streamId >= streamId = Left NotLargerId
    | Just xs <- mxs = okInsert xs streamId
  where
    mxs = M.lookup key sMap
    oldSeq :: (Int, Int)
    oldSeq =
        case mxs >>= unsnoc of
            Just (_, v) -> v.streamId
            Nothing -> (0, 0)

    streamId :: ConcreteStreamId
    streamId =
        case sid of
            AutoId -> let ts = utcToMillis time in if ts == fst oldSeq then (ts, snd oldSeq + 1) else (ts, 0)
            ExplicitId ts (Seq i) -> (ts, i)
            ExplicitId ts SeqAuto -> if ts == fst oldSeq then (ts, snd oldSeq + 1) else (ts, 0)

    okInsert xs sid' =
        Right
            ( sid'
            , M.insert key (xs ++ [MkValue sid' values]) sMap
            )

query :: (Ord k) => k -> XRange -> StreamMap k ik v -> [Value ik v]
query key range sMap = takeWhile (\elm -> elm.streamId <= e) $ dropWhile (\elm -> elm.streamId < s) vals
  where
    (s, e) = xrangeToConcrete range
    vals = fromMaybe [] $ M.lookup key sMap

queryEx :: (Ord k) => k -> ConcreteStreamId -> StreamMap k ik v -> [Value ik v]
queryEx key streamId sMap = dropWhile (\elm -> elm.streamId <= streamId) $ fromMaybe [] $ M.lookup key sMap
