{-# LANGUAGE OverloadedStrings #-}

module Resp.Result (resultToResp, respToResult) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (IsString (fromString))
import Text.Parsec (anyChar, char, manyTill, string)
import Text.Parsec.ByteString (Parser)

import CommandResult
import Parsers (intParser, parseBS, readIntBS)
import Resp.Core (Resp (..))
import StoreBackend.StreamMap (ConcreteStreamId, StreamMapError (..))
import StoreBackend.StreamMap qualified as SM
import StoreBackend.TypeIndex (ValueType (..))

resultToResp :: CommandResult -> Resp
resultToResp ResPong = Str "PONG"
resultToResp ResOk = Str "OK"
resultToResp (ResType Nothing) = Str "none"
resultToResp (ResType (Just t)) = valueTypeToResp t
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
resultToResp (RRepl r) = replResultToResp r

replResultToResp :: ReplResult -> Resp
replResultToResp ReplOk = Str "OK"
replResultToResp (ReplConfAck n) = Array 3 [BulkStr "REPLCONF", BulkStr "ACK", BulkStr $ fromString $ show n]
replResultToResp (ResFullResync sId s) = Str $ BS.intercalate " " ["FULLRESYNC", sId, fromString $ show s]

valueTypeToResp :: ValueType -> Resp
valueTypeToResp VString = Str "string"
valueTypeToResp VList = Str "list"
valueTypeToResp VStream = Str "stream"

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

respToResult :: Resp -> Either String CommandResult
respToResult (Str "PONG") = Right ResPong
respToResult (Str "OK") = Right ResOk
respToResult (Str s)
    | "FULLRESYNC " `BS.isPrefixOf` s = RRepl <$> parseBS fullresyncParser s
    | otherwise = Right $ RSimple s
respToResult (Array 3 [BulkStr "REPLCONF", BulkStr "ACK", BulkStr n]) = RRepl . ReplConfAck <$> readIntBS n
respToResult _ = error "TODO"

fullresyncParser :: Parser ReplResult
fullresyncParser = do
    _ <- string "FULLRESYNC "
    sId <- fromString <$> manyTill anyChar (char ' ')
    ResFullResync sId <$> intParser

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
