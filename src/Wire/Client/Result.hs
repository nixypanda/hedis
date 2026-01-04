{-# LANGUAGE OverloadedStrings #-}

module Wire.Client.Result (cmdResultToResp, respToResult, errorToResp, resultToResp) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (IsString (fromString))
import Text.Parsec (anyChar, char, manyTill, string)
import Text.Parsec.ByteString (Parser)

import Auth.Types (UserFlags (..), UserProperty (..), sha256Hex)
import Geo.Types (Coordinates (..))
import Parsers (intParser, parseBS, readIntBS)
import Protocol.Result
import Resp.Core (Resp (..))
import StoreBackend.StreamMap (ConcreteStreamId, StreamMapError (..))
import StoreBackend.StreamMap qualified as SM
import StoreBackend.TypeIndex (ValueType (..))
import Wire.Client.Command (cmdToPretty)

resultToResp :: Result -> Resp
resultToResp (ResultOk r) = cmdResultToResp r
resultToResp (ResultErr e) = errorToResp e
resultToResp (ResultTx er) = Array (length er) (map (either errorToResp cmdResultToResp) er)

cmdResultToResp :: Success -> Resp
cmdResultToResp ReplyPong = Str "PONG"
cmdResultToResp ReplyPongSubscribed = Array 2 [BulkStr "pong", BulkStr ""]
cmdResultToResp ReplyOk = Str "OK"
cmdResultToResp (ResType Nothing) = Str "none"
cmdResultToResp (ResType (Just t)) = valueTypeToResp t
cmdResultToResp (ReplyInt Nothing) = NullBulk
cmdResultToResp (ReplyInt (Just n)) = Int n
cmdResultToResp (ReplyDouble Nothing) = NullBulk
cmdResultToResp (ReplyDouble (Just n)) = BulkStr $ fromString $ show n
cmdResultToResp (ReplyStrings xs) = arrayMap BulkStr xs
cmdResultToResp (ReplyStreamValues vals) = arrayMap valueToResp vals
cmdResultToResp (ReplyStreamKeyValues kvs) = arrayMap arrayKeyValsToResp kvs
cmdResultToResp (ReplyStreamId sid) = streamIdToResp sid
cmdResultToResp (ReplySubscribed chan n) = Array 3 [BulkStr "subscribe", BulkStr chan, Int n]
cmdResultToResp (ReplyUnsubscribed chan n) = Array 3 [BulkStr "unsubscribe", BulkStr chan, Int n]
cmdResultToResp (ReplyCoordinates coords) = Array (length coords) (map (maybe NullArray coordsToRes) coords)
cmdResultToResp (ReplyUserProperty up) = userPropertiesToResp up
cmdResultToResp (ReplyResp rr) = respReplyToResp rr
cmdResultToResp (ReplyReplication r) = replResultToResp r

respReplyToResp :: RespReply -> Resp
respReplyToResp (RespBulk Nothing) = NullBulk
respReplyToResp (RespBulk (Just b)) = BulkStr b
respReplyToResp (RespInt n) = Int n
respReplyToResp RespArrayNull = NullArray
respReplyToResp (RespSimple s) = Str s

userPropertiesToResp :: UserProperty -> Resp
userPropertiesToResp (MkUserProperty{..}) =
    Array 4 [BulkStr "flags", flagsToRep flags, BulkStr "passwords", arrayMap (BulkStr . sha256Hex) passwords]
  where
    flagsToRep (MkUserFlags{..}) = if nopass then Array 1 [BulkStr "nopass"] else Array 0 []

coordsToRes :: Coordinates -> Resp
coordsToRes (MkCoordinates lat long) = Array 2 [BulkStr $ fromString $ show long, BulkStr $ fromString $ show lat]

replResultToResp :: ReplReply -> Resp
replResultToResp ReplOk = Str "OK"
replResultToResp (ReplConfAck n) = Array 3 [BulkStr "REPLCONF", BulkStr "ACK", BulkStr $ fromString $ show n]
replResultToResp (ReplFullResync sId s) = Str $ BS.intercalate " " ["FULLRESYNC", sId, fromString $ show s]

valueTypeToResp :: ValueType -> Resp
valueTypeToResp VString = Str "string"
valueTypeToResp VList = Str "list"
valueTypeToResp VStream = Str "stream"
valueTypeToResp VChannel = Str "channel"
valueTypeToResp VSortedSet = Str "sortedset"

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

----

respToResult :: Resp -> Either String Result
respToResult (Str "PONG") = Right $ ResultOk ReplyPong
respToResult (Str "OK") = Right $ ResultOk ReplyOk
respToResult (Str s)
    | "FULLRESYNC " `BS.isPrefixOf` s = ResultOk . ReplyReplication <$> parseBS fullresyncParser s
    | otherwise = Right $ ResultOk $ ReplyResp $ RespSimple s
respToResult (Int i) = Right $ ResultOk $ ReplyResp $ RespInt i
respToResult (Array 3 [BulkStr "REPLCONF", BulkStr "ACK", BulkStr n]) = ResultOk . ReplyReplication . ReplConfAck <$> readIntBS n
respToResult (StrErr s) = respToErr s
respToResult NullArray = Right $ ResultOk $ ReplyResp RespArrayNull
respToResult NullBulk = Right $ ResultOk $ ReplyResp $ RespBulk Nothing
respToResult (BulkStr s) = Right $ ResultOk $ ReplyResp $ RespBulk $ Just s
respToResult r = error $ "TODO: " <> show r

fullresyncParser :: Parser ReplReply
fullresyncParser = do
    _ <- string "FULLRESYNC "
    sId <- fromString <$> manyTill anyChar (char ' ')
    ReplFullResync sId <$> intParser

-- error conversions

strStreamErrBaseStreamId, strStreamErrNotLargerId, strRExecWithoutMulti, strRNotSupportedInTx, strRMultiInMulti, strRDiscardWithoutMulti, strIncrError, strWaitOnRplica :: ByteString
strStreamErrBaseStreamId = "ERR The ID specified in XADD must be greater than 0-0"
strStreamErrNotLargerId = "ERR The ID specified in XADD is equal or smaller than the target stream top item"
strRExecWithoutMulti = "ERR EXEC without MULTI"
strRNotSupportedInTx = "ERR command not supported in transaction"
strRMultiInMulti = "ERR MULTI inside MULTI"
strRDiscardWithoutMulti = "ERR DISCARD without MULTI"
strIncrError = "ERR value is not an integer or out of range"
strWaitOnRplica = "WAIT cannot be used on a replica"

errorToResp :: Failure -> Resp
errorToResp (ErrStream e) = streamMapErrorToResp e
errorToResp ErrIncr = StrErr strIncrError
errorToResp (ErrTx txErr) = txErrorToResp txErr
errorToResp (ErrCmdNotAllowedInMode cmd mode) = StrErr $ "ERR Can't execute '" <> cmdToPretty cmd <> "'in " <> modeToPretty mode <> "mode"
errorToResp (ErrInvalidCoords (MkCoordinates lat long)) = StrErr $ "ERR invalid longitude,latitude pair " <> fromString (show long) <> "," <> fromString (show lat)
errorToResp (ErrAuth err) = authErrorToResp err
errorToResp ErrWaitOnRplica = StrErr strWaitOnRplica

authErrorToResp :: AuthError -> Resp
authErrorToResp AuthWrongPassword = StrErr "WRONGPASS invalid username-password pair or user is disabled."
authErrorToResp AuthNoAuth = StrErr "NOAUTH Authentication required."

txErrorToResp :: TransactionError -> Resp
txErrorToResp TxExecWithoutMulti = StrErr strRExecWithoutMulti
txErrorToResp TxNotSupported = StrErr strRNotSupportedInTx
txErrorToResp TxMultiInMulti = StrErr strRMultiInMulti
txErrorToResp TxDiscardWithoutMulti = StrErr strRDiscardWithoutMulti

streamMapErrorToResp :: StreamMapError -> Resp
streamMapErrorToResp BaseStreamId = StrErr strStreamErrBaseStreamId
streamMapErrorToResp NotLargerId = StrErr strStreamErrNotLargerId

respToErr :: ByteString -> Either String Result
respToErr s
    | s == strStreamErrBaseStreamId = Right $ ResultErr $ ErrStream BaseStreamId
    | s == strStreamErrNotLargerId = Right $ ResultErr $ ErrStream NotLargerId
    | s == strRExecWithoutMulti = Right $ ResultErr $ ErrTx TxExecWithoutMulti
    | s == strRNotSupportedInTx = Right $ ResultErr $ ErrTx TxNotSupported
    | s == strRMultiInMulti = Right $ ResultErr $ ErrTx TxMultiInMulti
    | s == strRDiscardWithoutMulti = Right $ ResultErr $ ErrTx TxDiscardWithoutMulti
    | s == strIncrError = Right $ ResultErr ErrIncr
    | s == strWaitOnRplica = Right $ ResultErr ErrWaitOnRplica
    | otherwise = Left $ "unknown error: " <> show s
