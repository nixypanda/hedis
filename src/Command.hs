{-# LANGUAGE OverloadedStrings #-}

module Command (
    Command (..),
    CmdSTM (..),
    CmdIO (..),
    CmdTransaction (..),
    Key,
    CmdReplication (..),
    SubInfo (..),
    StringCmd (..),
    ListCmd (..),
    StreamCmd (..),
    isWriteCmd,
    respToCmd,
    cmdToResp,
    cmdBytes,
    cmdSTMBytes,
    respBytes,
) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (IsString (fromString))
import Data.Time (NominalDiffTime, secondsToNominalDiffTime)

import Parsers (readFloatBS, readIntBS)
import Resp (Resp (..), encode)
import Store.StreamStoreParsing (
    readConcreteStreamId,
    readXAddStreamId,
    readXRange,
    readXReadStreamId,
    showStreamId,
    showXRange,
    showXaddId,
 )
import StoreBackend.ListMap (Range (..))
import StoreBackend.StreamMap (ConcreteStreamId, XAddStreamId (..), XRange (..), XReadStreamId (..))
import Time (millisToNominalDiffTime, nominalDiffTimeToMillis)

type Key = ByteString

data SubInfo = IReplication deriving (Show, Eq)

data CmdSTM
    = CmdPing
    | CmdEcho ByteString
    | CmdType Key
    | STMString StringCmd
    | STMList ListCmd
    | STMStream StreamCmd
    deriving (Show, Eq)

data StringCmd
    = CmdSet Key ByteString (Maybe NominalDiffTime)
    | CmdGet Key
    | CmdIncr Key
    deriving (Show, Eq)

data ListCmd
    = CmdRPush Key [ByteString]
    | CmdLPush Key [ByteString]
    | CmdLPop Key (Maybe Int)
    | CmdLRange Key Range
    | CmdLLen Key
    deriving (Show, Eq)

data StreamCmd
    = CmdXAdd Key XAddStreamId [(ByteString, ByteString)]
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
    | CmdReplConfGetAck
    | CmdWait Int NominalDiffTime
    | CmdReplConfAck Int
    deriving (Show, Eq)

isWriteCmd :: CmdSTM -> Bool
isWriteCmd (STMString CmdSet{}) = True
isWriteCmd (STMString CmdIncr{}) = True
isWriteCmd (STMList CmdRPush{}) = True
isWriteCmd (STMList CmdLPush{}) = True
isWriteCmd (STMList CmdLPop{}) = True
isWriteCmd (STMStream CmdXAdd{}) = True
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
    pure $ RedSTM $ STMString $ CmdSet key val (Just expiry')
respToCmd (Array 3 [BulkStr "SET", BulkStr key, BulkStr val]) =
    pure $ RedSTM $ STMString $ CmdSet key val Nothing
respToCmd (Array 2 [BulkStr "GET", BulkStr key]) = pure $ RedSTM $ STMString $ CmdGet key
respToCmd (Array 2 [BulkStr "INCR", BulkStr key]) = pure $ RedSTM $ STMString $ CmdIncr key
-- List Store
respToCmd (Array 2 [BulkStr "LLEN", BulkStr key]) = pure $ RedSTM $ STMList $ CmdLLen key
respToCmd (Array 2 [BulkStr "LPOP", BulkStr key]) = pure $ RedSTM $ STMList $ CmdLPop key Nothing
respToCmd (Array 3 [BulkStr "LPOP", BulkStr key, BulkStr len]) =
    RedSTM . STMList . CmdLPop key . Just <$> readIntBS len
respToCmd (Array 4 [BulkStr "LRANGE", BulkStr key, BulkStr st, BulkStr stop]) = do
    start <- readIntBS st
    end <- readIntBS stop
    pure $ RedSTM $ STMList $ CmdLRange key (MkRange{..})
respToCmd (Array 3 [BulkStr "BLPOP", BulkStr key, BulkStr tout]) =
    RedIO . CmdBLPop key . secondsToNominalDiffTime . realToFrac <$> readFloatBS tout
respToCmd (Array _ ((BulkStr "RPUSH") : (BulkStr key) : vals)) =
    RedSTM . STMList . CmdRPush key <$> mapM extractBulk vals
respToCmd (Array _ ((BulkStr "LPUSH") : (BulkStr key) : vals)) =
    RedSTM . STMList . CmdLPush key <$> mapM extractBulk vals
-- Stream Store
respToCmd (Array _ ((BulkStr "XADD") : (BulkStr key) : (BulkStr sId) : vals)) = do
    vals' <- mapM extractBulk vals
    chunked <- chunksOf2 vals'
    sId' <- readXAddStreamId sId
    pure $ RedSTM $ STMStream $ CmdXAdd key sId' chunked
respToCmd (Array 4 [BulkStr "XRANGE", BulkStr key, BulkStr s, BulkStr e]) =
    RedSTM . STMStream . CmdXRange key <$> readXRange s e
respToCmd (Array _ (BulkStr "XREAD" : BulkStr "streams" : vals)) = do
    vals' <- mapM extractBulk vals
    let (keys, ids) = splitAt (length vals' `div` 2) vals'
    ids' <- mapM readConcreteStreamId ids
    pure $ RedSTM $ STMStream $ CmdXRead $ zip keys ids'
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
respToCmd (Array 3 [BulkStr "REPLCONF", BulkStr "GETACK", BulkStr "*"]) = pure $ RedRepl CmdReplConfGetAck
respToCmd (Array 3 [BulkStr "REPLCONF", BulkStr "ACK", BulkStr n]) = RedRepl . CmdReplConfAck <$> readIntBS n
respToCmd (Array 3 [BulkStr "PSYNC", BulkStr sid, BulkStr offset]) = do
    offset' <- readIntBS offset
    pure $ RedRepl $ CmdPSync sid offset'
respToCmd (Array 3 [BulkStr "WAIT", BulkStr n, BulkStr t]) = do
    n' <- readIntBS n
    t' <- millisToNominalDiffTime <$> readIntBS t
    pure $ RedRepl $ CmdWait n' t'
-- Unhandled
respToCmd r = Left $ "Conversion Error" <> show r

-- Conversion (to Resp)

cmdToResp :: Command -> Resp
cmdToResp (RedSTM CmdPing) = Array 1 [BulkStr "PING"]
cmdToResp (RedSTM (CmdEcho xs)) = Array 2 [BulkStr "ECHO", BulkStr xs]
cmdToResp (RedSTM (CmdType key)) = Array 2 [BulkStr "TYPE", BulkStr key]
cmdToResp (RedSTM (STMString cmd)) = stringStoreCmdToResp cmd
cmdToResp (RedSTM (STMList cmd)) = listStmCmdToResp cmd
cmdToResp (RedSTM (STMStream cmd)) = streamStmCmdToResp cmd
cmdToResp (RedRepl (CmdReplConfListen port)) =
    Array 3 [BulkStr "REPLCONF", BulkStr "listening-port", BulkStr $ fromString $ show port]
cmdToResp (RedRepl CmdReplConfCapabilities) = Array 3 [BulkStr "REPLCONF", BulkStr "capa", BulkStr "psync2"]
cmdToResp (RedRepl (CmdPSync sId s)) = Array 3 [BulkStr "PSYNC", BulkStr sId, BulkStr $ fromString $ show s]
cmdToResp (RedRepl CmdReplConfGetAck) = Array 3 [BulkStr "REPLCONF", BulkStr "GETACK", BulkStr "*"]
cmdToResp r = error $ "Not implemented: " <> show r

stringStoreCmdToResp :: StringCmd -> Resp
stringStoreCmdToResp (CmdSet key val (Just t)) = Array 5 [BulkStr "SET", BulkStr key, BulkStr val, BulkStr "PX", BulkStr $ fromString $ show $ nominalDiffTimeToMillis t]
stringStoreCmdToResp (CmdSet key val Nothing) = Array 3 [BulkStr "SET", BulkStr key, BulkStr val]
stringStoreCmdToResp (CmdIncr key) = Array 2 [BulkStr "INCR", BulkStr key]
stringStoreCmdToResp (CmdGet key) = Array 2 [BulkStr "GET", BulkStr key]

listStmCmdToResp :: ListCmd -> Resp
listStmCmdToResp (CmdRPush key xs) = Array (length xs + 2) (BulkStr "RPUSH" : BulkStr key : map BulkStr xs)
listStmCmdToResp (CmdLPush key xs) = Array (length xs + 2) (BulkStr "LPUSH" : BulkStr key : map BulkStr xs)
listStmCmdToResp (CmdLPop key Nothing) = Array 2 [BulkStr "LPOP", BulkStr key]
listStmCmdToResp (CmdLPop key (Just len)) = Array 3 [BulkStr "LPOP", BulkStr key, BulkStr $ fromString $ show len]
listStmCmdToResp (CmdLLen key) = Array 2 [BulkStr "LLEN", BulkStr key]
listStmCmdToResp (CmdLRange start stop) = Array 4 [BulkStr "LRANGE", BulkStr $ fromString $ show start, BulkStr $ fromString $ show stop]

streamStmCmdToResp :: StreamCmd -> Resp
streamStmCmdToResp (CmdXAdd key sid kvs) =
    Array (3 + length kvs * 2) $ BulkStr "XADD" : BulkStr key : BulkStr (showXaddId sid) : concatMap (\(k, v) -> [BulkStr k, BulkStr v]) kvs
streamStmCmdToResp (CmdXRange key xr) =
    Array 4 $ BulkStr "XRANGE" : BulkStr key : map BulkStr [s, e]
  where
    (s, e) = showXRange xr
streamStmCmdToResp (CmdXRead xs) =
    Array (2 + length keys + length ids) $ BulkStr "XREAD" : BulkStr "streams" : map BulkStr keys ++ map (BulkStr . showStreamId) ids
  where
    (keys, ids) = unzip xs

---

cmdBytes :: Command -> Int
cmdBytes = respBytes . cmdToResp

cmdSTMBytes :: CmdSTM -> Int
cmdSTMBytes = respBytes . cmdToResp . RedSTM

respBytes :: Resp -> Int
respBytes = BS.length . encode
