{-# LANGUAGE OverloadedStrings #-}

module Command (
    Command (..),
    CmdSTM (..),
    CmdIO (..),
    CmdTransaction (..),
    Key,
    CommandResult (..),
    CommandError (..),
    CmdReplication (..),
    TransactionError (..),
    SubInfo (..),
    isWriteCmd,
    respToCmd,
    resultToResp,
    cmdToResp,
) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (IsString (fromString))
import Data.Time (NominalDiffTime, secondsToNominalDiffTime)

import Parsers (
    readConcreteStreamId,
    readFloatBS,
    readIntBS,
    readXAddStreamId,
    readXRange,
    readXReadStreamId,
 )
import Resp (Resp (..))
import StoreBackend.ListMap (Range (..))
import StoreBackend.StreamMap (
    ConcreteStreamId,
    StreamMapError (..),
    XAddStreamId,
    XRange,
    XReadStreamId (..),
 )
import StoreBackend.StreamMap qualified as SM
import Time (millisToNominalDiffTime, nominalDiffTimeToMillis)

type Key = ByteString

data SubInfo = IReplication deriving (Show, Eq)

data CmdSTM
    = CmdPing
    | CmdEcho ByteString
    | CmdType Key
    | CmdSet Key ByteString (Maybe NominalDiffTime)
    | CmdGet Key
    | CmdIncr Key
    | CmdRPush Key [ByteString]
    | CmdLPush Key [ByteString]
    | CmdLPop Key (Maybe Int)
    | CmdLRange Key Range
    | CmdLLen Key
    | CmdXAdd Key XAddStreamId [(ByteString, ByteString)]
    | CmdXRange Key XRange
    | CmdXRead [(Key, ConcreteStreamId)]
    deriving (Show, Eq)

data CmdIO
    = CmdBLPop Key NominalDiffTime
    | CmdXReadBlock Key XReadStreamId NominalDiffTime
    deriving (Show, Eq)

data CmdTransaction = Multi | Exec | Discard
    deriving (Show, Eq)

data Command
    = RedSTM CmdSTM
    | RedIO CmdIO
    | RedTrans CmdTransaction
    | RedInfo (Maybe SubInfo)
    | RedRepl CmdReplication
    deriving (Show, Eq)

data CmdReplication
    = CmdReplConfListen Int
    | CmdReplConfCapabilities
    | CmdPSync ByteString Int
    | CmdFullResync ByteString Int
    deriving (Show, Eq)

isWriteCmd :: CmdSTM -> Bool
isWriteCmd (CmdSet{}) = True
isWriteCmd (CmdIncr{}) = True
isWriteCmd (CmdRPush{}) = True
isWriteCmd (CmdLPush{}) = True
isWriteCmd (CmdLPop{}) = True
isWriteCmd (CmdXRange{}) = True
-- how to handle blpop?????????
isWriteCmd _ = False

-- Conversion (from Resp)

extractBulk :: Resp -> Either String ByteString
extractBulk (BulkStr x) = Right x
extractBulk r = Left $ "Invalit Type" <> show r

chunksOf2 :: [a] -> Either String [(a, a)]
chunksOf2 [] = Right []
chunksOf2 (x : y : xs) = ((x, y) :) <$> chunksOf2 xs
chunksOf2 _ = Left "Invalid chunk of 2"

respToCmd :: Resp -> Either String Command
respToCmd (Array 1 [BulkStr "PING"]) = pure $ RedSTM CmdPing
respToCmd (Array 2 [BulkStr "ECHO", BulkStr xs]) = pure $ RedSTM $ CmdEcho xs
-- Type Index
respToCmd (Array 2 [BulkStr "TYPE", BulkStr key]) = pure $ RedSTM $ CmdType key
-- String Store
respToCmd (Array 5 [BulkStr "SET", BulkStr key, BulkStr val, BulkStr "PX", BulkStr t]) = do
    expiry <- readIntBS t
    let expiry' = millisToNominalDiffTime expiry
    pure $ RedSTM $ CmdSet key val (Just expiry')
respToCmd (Array 3 [BulkStr "SET", BulkStr key, BulkStr val]) =
    pure $ RedSTM $ CmdSet key val Nothing
respToCmd (Array 2 [BulkStr "GET", BulkStr key]) = pure $ RedSTM $ CmdGet key
respToCmd (Array 2 [BulkStr "INCR", BulkStr key]) = pure $ RedSTM $ CmdIncr key
-- List Store
respToCmd (Array 2 [BulkStr "LLEN", BulkStr key]) = pure $ RedSTM $ CmdLLen key
respToCmd (Array 2 [BulkStr "LPOP", BulkStr key]) = pure $ RedSTM $ CmdLPop key Nothing
respToCmd (Array 3 [BulkStr "LPOP", BulkStr key, BulkStr len]) =
    RedSTM . CmdLPop key . Just <$> readIntBS len
respToCmd (Array 4 [BulkStr "LRANGE", BulkStr key, BulkStr st, BulkStr stop]) = do
    start <- readIntBS st
    end <- readIntBS stop
    pure $ RedSTM $ CmdLRange key (MkRange{..})
respToCmd (Array 3 [BulkStr "BLPOP", BulkStr key, BulkStr tout]) =
    RedIO . CmdBLPop key . secondsToNominalDiffTime . realToFrac <$> readFloatBS tout
respToCmd (Array _ ((BulkStr "RPUSH") : (BulkStr key) : vals)) =
    RedSTM . CmdRPush key <$> mapM extractBulk vals
respToCmd (Array _ ((BulkStr "LPUSH") : (BulkStr key) : vals)) =
    RedSTM . CmdLPush key <$> mapM extractBulk vals
-- Stream Store
respToCmd (Array _ ((BulkStr "XADD") : (BulkStr key) : (BulkStr sId) : vals)) = do
    vals' <- mapM extractBulk vals
    chunked <- chunksOf2 vals'
    sId' <- readXAddStreamId sId
    pure $ RedSTM $ CmdXAdd key sId' chunked
respToCmd (Array 4 [BulkStr "XRANGE", BulkStr key, BulkStr s, BulkStr e]) =
    RedSTM . CmdXRange key <$> readXRange s e
respToCmd (Array _ (BulkStr "XREAD" : BulkStr "streams" : vals)) = do
    vals' <- mapM extractBulk vals
    let (keys, ids) = splitAt (length vals' `div` 2) vals'
    ids' <- mapM readConcreteStreamId ids
    pure $ RedSTM $ CmdXRead $ zip keys ids'
respToCmd
    ( Array
            6
            [BulkStr "XREAD", BulkStr "block", BulkStr t, BulkStr "streams", BulkStr key, BulkStr sid]
        ) = do
        t' <- millisToNominalDiffTime <$> readIntBS t
        sid' <- readXReadStreamId sid
        pure $ RedIO $ CmdXReadBlock key sid' t'
-- Transaactions
respToCmd (Array 1 [BulkStr "MULTI"]) = pure $ RedTrans Multi
respToCmd (Array 1 [BulkStr "EXEC"]) = pure $ RedTrans Exec
respToCmd (Array 1 [BulkStr "DISCARD"]) = pure $ RedTrans Discard
-- Replication
respToCmd (Array 2 [BulkStr "INFO", BulkStr "replication"]) = pure $ RedInfo (Just IReplication)
respToCmd (Array 3 [BulkStr "REPLCONF", BulkStr "listening-port", BulkStr port]) = RedRepl . CmdReplConfListen <$> readIntBS port
respToCmd (Array 3 [BulkStr "REPLCONF", BulkStr "capa", BulkStr "psync2"]) = pure $ RedRepl CmdReplConfCapabilities
respToCmd (Array 3 [BulkStr "PSYNC", BulkStr sid, BulkStr offset]) = do
    offset' <- readIntBS offset
    pure $ RedRepl $ CmdPSync sid offset'
-- Unhandled
respToCmd r = Left $ "Conversion Error" <> show r

-- Conversion (to Resp)

cmdToResp :: Command -> Resp
cmdToResp (RedSTM CmdPing) = Array 1 [BulkStr "PING"]
cmdToResp (RedSTM (CmdEcho xs)) = Array 2 [BulkStr "ECHO", BulkStr xs]
cmdToResp (RedSTM (CmdType key)) = Array 2 [BulkStr "TYPE", BulkStr key]
cmdToResp (RedSTM (CmdSet key val (Just t))) = Array 5 [BulkStr "SET", BulkStr key, BulkStr val, BulkStr "PX", BulkStr $ fromString $ show $ nominalDiffTimeToMillis t]
cmdToResp (RedSTM (CmdSet key val Nothing)) = Array 3 [BulkStr "SET", BulkStr key, BulkStr val]
cmdToResp (RedSTM (CmdIncr key)) = Array 2 [BulkStr "INCR", BulkStr key]
cmdToResp (RedSTM (CmdRPush key xs)) = Array (length xs + 2) (BulkStr "RPUSH" : BulkStr key : map BulkStr xs)
cmdToResp (RedSTM (CmdLPush key xs)) = Array (length xs + 2) (BulkStr "LPUSH" : BulkStr key : map BulkStr xs)
cmdToResp (RedSTM (CmdLPop key Nothing)) = Array 2 [BulkStr "LPOP", BulkStr key]
cmdToResp (RedSTM (CmdLPop key (Just len))) = Array 3 [BulkStr "LPOP", BulkStr key, BulkStr $ fromString $ show len]
cmdToResp (RedRepl (CmdReplConfListen port)) =
    Array 3 [BulkStr "REPLCONF", BulkStr "listening-port", BulkStr $ fromString $ show port]
cmdToResp (RedRepl CmdReplConfCapabilities) = Array 3 [BulkStr "REPLCONF", BulkStr "capa", BulkStr "psync2"]
cmdToResp (RedRepl (CmdPSync sId s)) = Array 3 [BulkStr "PSYNC", BulkStr sId, BulkStr $ fromString $ show s]
cmdToResp (RedRepl (CmdFullResync sId s)) = Str $ BS.intercalate " " ["FULLRESYNC", sId, fromString $ show s]
cmdToResp r = error $ "Not implemented: " <> show r

data CommandResult
    = RSimple ByteString
    | RBulk (Maybe ByteString)
    | RInt Int
    | RArrayNull
    | RArraySimple [ByteString]
    | RArrayKeyValues [(Key, [SM.Value ByteString ByteString])]
    | RStreamId ConcreteStreamId
    | RArrayStreamValues [SM.Value ByteString ByteString]
    | RArray [CommandResult]
    | RErr CommandError
    | RCmd Command
    deriving (Show, Eq)

data CommandError
    = RStreamError StreamMapError
    | RIncrError
    | RTxErr TransactionError
    deriving (Show, Eq)

data TransactionError
    = RExecWithoutMulti
    | RNotSupportedInTx
    | RMultiInMulti
    | RDiscardWithoutMulti
    deriving (Show, Eq)

resultToResp :: CommandResult -> Resp
resultToResp (RSimple s) = Str s
resultToResp (RBulk Nothing) = NullBulk
resultToResp (RBulk (Just b)) = BulkStr b
resultToResp (RInt n) = Int n
resultToResp RArrayNull = NullArray
resultToResp (RArray ar) = arrayMap resultToResp ar
resultToResp (RArraySimple xs) = arrayMap BulkStr xs
resultToResp (RArrayStreamValues vals) = arrayMap valueToResp vals
resultToResp (RArrayKeyValues kvs) = arrayMap arrayKeyValsToResp kvs
resultToResp (RStreamId sid) = streamIdToResp sid
resultToResp (RErr e) = errorToResp e
resultToResp (RCmd c) = cmdToResp c

arrayMap :: (a -> Resp) -> [a] -> Resp
arrayMap f xs = Array (length xs) (map f xs)

streamIdToResp :: ConcreteStreamId -> Resp
streamIdToResp (ms, i) = BulkStr . fromString $ show ms <> "-" <> show i

valueToResp :: SM.Value ByteString ByteString -> Resp
valueToResp v = Array 2 [streamIdToResp v.streamId, arrayMap BulkStr $ vals v.vals]
  where
    vals :: [(ByteString, ByteString)] -> [ByteString]
    vals = concatMap (\(k, v') -> [k, v'])

arrayKeyValsToResp :: (ByteString, [SM.Value ByteString ByteString]) -> Resp
arrayKeyValsToResp (k, vs) = Array 2 [BulkStr k, arrayMap valueToResp vs]

-- error conversions

errorToResp :: CommandError -> Resp
errorToResp (RStreamError e) = streamMapErrorToResp e
errorToResp RIncrError = StrErr "ERR value is not an integer or out of range"
errorToResp (RTxErr txErr) = txErrorToResp txErr

txErrorToResp :: TransactionError -> Resp
txErrorToResp RExecWithoutMulti = StrErr "ERR EXEC without MULTI"
txErrorToResp RNotSupportedInTx = StrErr "ERR command not supported in transaction"
txErrorToResp RMultiInMulti = StrErr "ERR MULTI inside MULTI"
txErrorToResp RDiscardWithoutMulti = StrErr "ERR DISCARD without MULTI"

streamMapErrorToResp :: StreamMapError -> Resp
streamMapErrorToResp BaseStreamId =
    StrErr "ERR The ID specified in XADD must be greater than 0-0"
streamMapErrorToResp NotLargerId =
    StrErr "ERR The ID specified in XADD is equal or smaller than the target stream top item"
