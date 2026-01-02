module StoreBackend.SortedSetMap (
    SortedSetMap,
    insert,
    count,
    lookup,
    remove,
    range,
    range',
    score,
    rank,
) where

import Data.Map (Map)
import Data.Set (Set)

import Data.Map.Strict qualified as M
import Data.Set qualified as S

import Prelude hiding (lookup)

type Set' v s = Set (s, v)
type Index v s = Map v s

type SortedSetMap k v s = Map k (Set' v s, Index v s)

insert :: (Ord k, Ord v, Ord s) => k -> s -> v -> SortedSetMap k v s -> SortedSetMap k v s
insert key score' val =
    M.alter go key
  where
    go Nothing =
        Just (S.singleton (score', val), M.singleton val score')
    go (Just (s, idx)) =
        let s' =
                case M.lookup val idx of
                    Nothing -> S.insert (score', val) s
                    Just oldSc -> S.insert (score', val) $ S.delete (oldSc, val) s
            idx' = M.insert val score' idx
         in Just (s', idx')

count :: (Ord k) => k -> SortedSetMap k v s -> Int
count key store = maybe 0 (S.size . fst) (M.lookup key store)

lookup :: (Ord k, Ord v) => k -> v -> SortedSetMap k v s -> Maybe v
lookup key val store = M.lookup key store >>= \(_, idx) -> val <$ M.lookup val idx

score :: (Ord k, Ord v) => k -> v -> SortedSetMap k v s -> Maybe s
score key val store =
    M.lookup key store >>= \(_, idx) ->
        M.lookup val idx

rank :: (Ord k, Ord v, Ord s) => k -> v -> SortedSetMap k v s -> Maybe Int
rank key val store = do
    (s, idx) <- M.lookup key store
    sc <- M.lookup val idx
    let before =
            S.takeWhileAntitone (< (sc, val)) s
    pure (S.size before)

remove :: (Ord k, Ord v, Ord s) => k -> v -> SortedSetMap k v s -> SortedSetMap k v s
remove key val store = case M.lookup key store of
    Nothing -> store
    Just (s, i) -> case M.lookup val i of
        Nothing -> store
        Just sc -> M.insert key (S.delete (sc, val) s, M.delete val i) store

range :: (Ord k) => k -> Int -> Int -> SortedSetMap k v s -> [v]
range key start end store =
    case M.lookup key store of
        Nothing -> []
        Just (s, _) -> map snd . slice start end . S.toAscList $ s

range' :: (Ord k) => k -> Int -> Int -> SortedSetMap k v s -> [(s, v)]
range' key start end store =
    case M.lookup key store of
        Nothing -> []
        Just (s, _) -> slice start end . S.toAscList $ s

normalize :: Int -> Int -> Int
normalize len i
    | i < 0 = max 0 (len + i)
    | otherwise = i

slice :: Int -> Int -> [a] -> [a]
slice start stop xs =
    let len = length xs
        start' = normalize len start
        stop' = normalize len stop
        count' = stop' - start' + 1
     in if count' <= 0
            then []
            else take count' (drop start' xs)
