{-# LANGUAGE OverloadedStrings #-}

module Command (
    Command (..),
    Key,
    respToCmd,
    CommandResult (..),
    resultToResp,
) where

import Data.ByteString (ByteString)
import Data.String (IsString (fromString))
import Data.Time (NominalDiffTime, secondsToNominalDiffTime)

import Parsers (readConcreteStreamId, readFloatBS, readIntBS, readXAddStreamId, readXRange, readXReadStreamId)
import Resp (Resp (..))
import Store.StreamMap (ConcreteStreamId, StreamMapError (..), XAddStreamId, XReadStreamId (..))
import Store.StreamMap qualified as SM
import Time (millisToNominalDiffTime)

type Key = ByteString

data Command
    = Ping
    | Echo ByteString
    | Type Key
    | Set Key ByteString (Maybe NominalDiffTime)
    | Get Key
    | Rpush Key [ByteString]
    | Lpush Key [ByteString]
    | Lrange Key Int Int
    | Llen Key
    | Lpop Key (Maybe Int)
    | Blpop Key NominalDiffTime
    | Xadd Key XAddStreamId [(ByteString, ByteString)]
    | XRange Key SM.XRange
    | XRead [(Key, ConcreteStreamId)]
    | XReadBlock NominalDiffTime Key XReadStreamId
    deriving (Show, Eq)

-- Conversion (from Resp)

extractBulk :: Resp -> Either String ByteString
extractBulk (BulkStr x) = Right x
extractBulk r = Left $ "Invalit Type" <> show r

chunksOf2 :: [a] -> Either String [(a, a)]
chunksOf2 [] = Right []
chunksOf2 (x : y : xs) = ((x, y) :) <$> chunksOf2 xs
chunksOf2 _ = Left "Invalid chunk of 2"

respToCmd :: Resp -> Either String Command
respToCmd (Array 1 [BulkStr "PING"]) = pure Ping
respToCmd (Array 2 [BulkStr "ECHO", BulkStr xs]) = pure $ Echo xs
respToCmd (Array 2 [BulkStr "TYPE", BulkStr key]) = pure $ Type key
respToCmd (Array 5 [BulkStr "SET", BulkStr key, BulkStr val, BulkStr "PX", BulkStr t]) = do
    expiry <- readIntBS t
    let expiry' = millisToNominalDiffTime expiry
    pure $ Set key val (Just expiry')
respToCmd (Array 3 [BulkStr "SET", BulkStr key, BulkStr val]) = pure $ Set key val Nothing
respToCmd (Array 2 [BulkStr "GET", BulkStr key]) = pure $ Get key
respToCmd (Array 2 [BulkStr "LLEN", BulkStr key]) = pure $ Llen key
respToCmd (Array 2 [BulkStr "LPOP", BulkStr key]) = pure $ Lpop key Nothing
respToCmd (Array 3 [BulkStr "LPOP", BulkStr key, BulkStr len]) = Lpop key . Just <$> readIntBS len
respToCmd (Array 4 [BulkStr "LRANGE", BulkStr key, BulkStr start, BulkStr stop]) = Lrange key <$> readIntBS start <*> readIntBS stop
respToCmd (Array 3 [BulkStr "BLPOP", BulkStr key, BulkStr tout]) = Blpop key . secondsToNominalDiffTime . realToFrac <$> readFloatBS tout
respToCmd (Array _ ((BulkStr "RPUSH") : (BulkStr key) : vals)) = Rpush key <$> mapM extractBulk vals
respToCmd (Array _ ((BulkStr "LPUSH") : (BulkStr key) : vals)) = Lpush key <$> mapM extractBulk vals
respToCmd (Array _ ((BulkStr "XADD") : (BulkStr key) : (BulkStr sId) : vals)) = do
    vals' <- mapM extractBulk vals
    chunked <- chunksOf2 vals'
    sId' <- readXAddStreamId sId
    pure $ Xadd key sId' chunked
respToCmd (Array 4 [BulkStr "XRANGE", BulkStr key, BulkStr s, BulkStr e]) = XRange key <$> readXRange s e
respToCmd (Array _ (BulkStr "XREAD" : BulkStr "streams" : vals)) = do
    vals' <- mapM extractBulk vals
    let (keys, ids) = splitAt (length vals' `div` 2) vals'
    ids' <- mapM readConcreteStreamId ids
    pure $ XRead $ zip keys ids'
respToCmd (Array 6 [BulkStr "XREAD", BulkStr "block", BulkStr t, BulkStr "streams", BulkStr key, BulkStr sid]) = do
    t' <- millisToNominalDiffTime <$> readIntBS t
    sid' <- readXReadStreamId sid
    pure $ XReadBlock t' key sid'
respToCmd r = Left $ "Conversion Error" <> show r

-- Conversion (to Resp)

data CommandResult
    = RSimple ByteString
    | RBulk (Maybe ByteString)
    | RInt Int
    | RArray [ByteString]
    | RNullArray
    | RKeyValues [(Key, [SM.Value ByteString ByteString])]
    | RStreamId ConcreteStreamId
    | RStreamValues [SM.Value ByteString ByteString]
    | RStreamError StreamMapError
    deriving (Show, Eq)

resultToResp :: CommandResult -> Resp
resultToResp (RSimple s) = Str s
resultToResp (RBulk Nothing) = NullBulk
resultToResp (RBulk (Just b)) = BulkStr b
resultToResp (RInt n) = Int n
resultToResp (RArray xs) = listToResp xs
resultToResp (RStreamId sid) = streamIdToResp sid
resultToResp (RStreamValues vals) = arrayToResp vals
resultToResp (RKeyValues kvs) = Array (length kvs) $ map keyValsToResp kvs
resultToResp RNullArray = NullArray
resultToResp (RStreamError e) = streamMapErrorToResp e

listToResp :: [ByteString] -> Resp
listToResp xs = Array (length xs) $ map BulkStr xs

streamIdToResp :: ConcreteStreamId -> Resp
streamIdToResp (ms, i) = BulkStr . fromString $ show ms <> "-" <> show i

arrayToResp :: [SM.Value ByteString ByteString] -> Resp
arrayToResp ar = Array (length ar) $ map valueToResp ar
  where
    valueToResp :: SM.Value ByteString ByteString -> Resp
    valueToResp v = Array 2 [streamIdToResp v.streamId, listToResp $ vals v.vals]
    vals :: [(ByteString, ByteString)] -> [ByteString]
    vals = concatMap (\(k, v) -> [k, v])

keyValsToResp :: (ByteString, [SM.Value ByteString ByteString]) -> Resp
keyValsToResp (k, vs) = Array 2 [BulkStr k, arrayToResp vs]

streamMapErrorToResp :: StreamMapError -> Resp
streamMapErrorToResp BaseStreamId = StrErr "ERR The ID specified in XADD must be greater than 0-0"
streamMapErrorToResp NotLargerId = StrErr "ERR The ID specified in XADD is equal or smaller than the target stream top item"
