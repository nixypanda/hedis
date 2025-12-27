{-# LANGUAGE OverloadedStrings #-}

module Command (
    Command (..),
    Expiry,
    Key,
    respToCommand,
    CommandResult (..),
    resultToResp,
) where

import Data.ByteString (ByteString)
import Data.String (IsString (fromString))
import Data.Time (NominalDiffTime, secondsToNominalDiffTime)

import Parsers (readConcreteStreamId, readDollarStreamId, readFloatBS, readIntBS, readStreamId, readXRange)
import Resp (Resp (..))
import Store.StreamMap (ConcreteStreamId, StreamId, StreamMapError (..), XRStreamId (..))
import Store.StreamMap qualified as SM
import Time (millisToNominalDiffTime)

type Key = ByteString
type Expiry = Int

data Command
    = Ping
    | Echo ByteString
    | Type Key
    | Set Key ByteString (Maybe Expiry)
    | Get Key
    | Rpush Key [ByteString]
    | Lpush Key [ByteString]
    | Lrange Key Int Int
    | Llen Key
    | Lpop Key (Maybe Int)
    | Blpop Key NominalDiffTime
    | Xadd Key StreamId [(ByteString, ByteString)]
    | XRange Key SM.XRange
    | XRead [(Key, ConcreteStreamId)]
    | XReadBlock NominalDiffTime Key XRStreamId
    deriving (Show, Eq)

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
resultToResp (RKeyValues kvs) = arrayOfArrayToResp kvs
resultToResp RNullArray = NullArray
resultToResp (RStreamError e) = streamMapErrorToResp e

-- Conversion (from Resp)

extractBulk :: Resp -> Either String ByteString
extractBulk (BulkStr x) = Right x
extractBulk r = Left $ "Invalit Type" <> show r

chunksOf2 :: [a] -> Either String [(a, a)]
chunksOf2 [] = Right []
chunksOf2 (x : y : xs) = ((x, y) :) <$> chunksOf2 xs
chunksOf2 _ = Left "Invalid chunk of 2"

respToCommand :: Resp -> Either String Command
respToCommand (Array 1 [BulkStr "PING"]) = pure Ping
respToCommand (Array 2 [BulkStr "ECHO", BulkStr xs]) = pure $ Echo xs
respToCommand (Array 2 [BulkStr "TYPE", BulkStr key]) = pure $ Type key
respToCommand (Array 5 [BulkStr "SET", BulkStr key, BulkStr val, BulkStr "PX", BulkStr t]) = Set key val . Just <$> readIntBS t
respToCommand (Array 3 [BulkStr "SET", BulkStr key, BulkStr val]) = pure $ Set key val Nothing
respToCommand (Array 2 [BulkStr "GET", BulkStr key]) = pure $ Get key
respToCommand (Array 2 [BulkStr "LLEN", BulkStr key]) = pure $ Llen key
respToCommand (Array 2 [BulkStr "LPOP", BulkStr key]) = pure $ Lpop key Nothing
respToCommand (Array 3 [BulkStr "LPOP", BulkStr key, BulkStr len]) = Lpop key . Just <$> readIntBS len
respToCommand (Array 4 [BulkStr "LRANGE", BulkStr key, BulkStr start, BulkStr stop]) = Lrange key <$> readIntBS start <*> readIntBS stop
respToCommand (Array 3 [BulkStr "BLPOP", BulkStr key, BulkStr tout]) = Blpop key . secondsToNominalDiffTime . realToFrac <$> readFloatBS tout
respToCommand (Array _ ((BulkStr "RPUSH") : (BulkStr key) : vals)) = Rpush key <$> mapM extractBulk vals
respToCommand (Array _ ((BulkStr "LPUSH") : (BulkStr key) : vals)) = Lpush key <$> mapM extractBulk vals
respToCommand (Array _ ((BulkStr "XADD") : (BulkStr key) : (BulkStr sId) : vals)) = do
    vals' <- mapM extractBulk vals
    chunked <- chunksOf2 vals'
    sId' <- readStreamId sId
    pure $ Xadd key sId' chunked
respToCommand (Array 4 [BulkStr "XRANGE", BulkStr key, BulkStr s, BulkStr e]) = XRange key <$> readXRange s e
respToCommand (Array _ (BulkStr "XREAD" : BulkStr "streams" : vals)) = do
    vals' <- mapM extractBulk vals
    let (keys, ids) = splitAt (length vals' `div` 2) vals'
    ids' <- mapM readConcreteStreamId ids
    pure $ XRead $ zip keys ids'
respToCommand (Array 6 [BulkStr "XREAD", BulkStr "block", BulkStr t, BulkStr "streams", BulkStr key, BulkStr sid]) = do
    t' <- millisToNominalDiffTime <$> readIntBS t
    sid' <- readDollarStreamId sid
    pure $ XReadBlock t' key sid'
respToCommand r = Left $ "Conversion Error" <> show r

-- Conversion (to Resp)

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

arrayOfArrayToResp :: [(Key, [SM.Value ByteString ByteString])] -> Resp
arrayOfArrayToResp ar = Array (length ar) $ map keyValsToResp ar

keyValsToResp :: (ByteString, [SM.Value ByteString ByteString]) -> Resp
keyValsToResp (k, vs) = Array 2 [BulkStr k, arrayToResp vs]

streamMapErrorToResp :: StreamMapError -> Resp
streamMapErrorToResp BaseStreamId = StrErr "ERR The ID specified in XADD must be greater than 0-0"
streamMapErrorToResp NotLargerId = StrErr "ERR The ID specified in XADD is equal or smaller than the target stream top item"
