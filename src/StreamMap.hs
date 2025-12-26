module StreamMap where

import Data.List (unsnoc)
import Data.Map (Map)
import Data.Map qualified as M
import Data.Time (UTCTime, nominalDiffTimeToSeconds)
import Data.Time.Clock.POSIX (utcTimeToPOSIXSeconds)

data StreamId = AutoId | ExplicitId Int IdSeq
    deriving (Show, Eq)

data IdSeq = Seq Int | SeqAuto
    deriving (Show, Eq)

type ConcreteStreamId = (Int, Int)

data Value k v = MkValue
    { streamId :: ConcreteStreamId
    , vals :: [(k, v)]
    }
    deriving (Eq, Ord)

data StreamMapError = BaseStreamId | NotLargerId

baseId :: (Int, Int)
baseId = (0, 0)

empty :: Map k a
empty = M.empty

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

utcToMillis :: UTCTime -> Int
utcToMillis = floor . (* 1000) . nominalDiffTimeToSeconds . utcTimeToPOSIXSeconds
