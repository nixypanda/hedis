module StreamMap where

import Data.List (unsnoc)
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

data StreamMapError = BaseStreamId | NotLargerId

baseId :: (Int, Int)
baseId = (0, 0)

empty :: Map k a
empty = M.empty

type StreamMap k ik v = Map k [Value ik v]

insert :: (Ord k) => k -> StreamId -> [(ik, v)] -> UTCTime -> StreamMap k ik v -> Either StreamMapError (ConcreteStreamId, StreamMap k ik v)
insert key sid values time sMap
    | streamId == baseId = Left BaseStreamId
    | Nothing <- M.lookup key sMap = okInsert []
    | Just [] <- M.lookup key sMap = okInsert []
    | Just xs <- M.lookup key sMap, Just (_, lastVal) <- unsnoc xs, lastVal.streamId >= streamId = Left NotLargerId
    | Just xs <- M.lookup key sMap = okInsert xs
  where
    streamId = convertToConcreteStreamId sid time
    okInsert xs =
        Right
            ( streamId
            , M.insert key (xs ++ [MkValue streamId values]) sMap
            )

convertToConcreteStreamId :: StreamId -> UTCTime -> ConcreteStreamId
convertToConcreteStreamId AutoId _ = undefined
convertToConcreteStreamId (ExplicitId _ SeqAuto) _ = undefined
convertToConcreteStreamId (ExplicitId ts (Seq i)) _ = (ts, i)
