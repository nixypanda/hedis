module StreamMap where

import Data.Map (Map)
import Data.Map qualified as M
import Data.Time (UTCTime)

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

empty :: Map k a
empty = M.empty

type StreamMap k ik v = Map k [Value ik v]

insert :: (Ord k) => k -> StreamId -> [(ik, v)] -> UTCTime -> StreamMap k ik v -> (ConcreteStreamId, StreamMap k ik v)
insert key sid values time sMap = (cid, M.insert key [MkValue cid values] sMap)
  where
    cid = convertToConcreteStreamId sid

convertToConcreteStreamId :: StreamId -> ConcreteStreamId
convertToConcreteStreamId AutoId = undefined
convertToConcreteStreamId (ExplicitId _ SeqAuto) = undefined
convertToConcreteStreamId (ExplicitId ts (Seq i)) = (ts, i)
