{-# LANGUAGE OverloadedStrings #-}

module Rdb.Parser (
    respEncodeRdbParser,
) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Text.Parsec qualified as P
import Text.Parsec.ByteString qualified as PB

import Parsers (intParser)
import Rdb.Type
import Prelude hiding (take)

respEncodeRdbParser :: PB.Parser RespEncodedRdb
respEncodeRdbParser = do
    _ <- P.char '$'
    len <- intParser
    _ <- P.crlf
    payload <- takeBytes len
    case BS.take 5 payload of
        "REDIS" -> pure ()
        _ -> fail "Invalid RDB magic"

    pure $
        MkRespEncodedRdb{..}

takeBytes :: Int -> PB.Parser BS.ByteString
takeBytes n = do
    input <- P.getInput
    let (h, t) = BS.splitAt n input
    if BS.length h == n
        then P.setInput t >> pure h
        else P.parserFail "unexpected end of input while reading bytes"
