{-# LANGUAGE OverloadedStrings #-}

module RDB (RDBEncoded, rdbParser) where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Text.Parsec (char, crlf, getInput, parserFail, setInput)
import Text.Parsec.ByteString (Parser)

import Parsers (intParser)

data RDBEncoded = MkRDBEncoded
    { len :: !Int
    , payload :: !ByteString
    }
    deriving (Show, Eq)

rdbParser :: Parser RDBEncoded
rdbParser = do
    _ <- char '$'
    len <- intParser
    _ <- crlf
    payload <- takeBytes len
    case BS.take 5 payload of
        "REDIS" -> pure ()
        _ -> fail "Invalid RDB magic"

    pure $
        MkRDBEncoded{..}

takeBytes :: Int -> Parser BS.ByteString
takeBytes n = do
    input <- getInput
    let (h, t) = BS.splitAt n input
    if BS.length h == n
        then setInput t >> pure h
        else parserFail "unexpected end of input while reading bytes"
