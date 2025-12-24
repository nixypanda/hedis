{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE OverloadedStrings #-}

module Resp (Resp (..), encode, decode) where

import Control.Monad (replicateM)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (fromString)
import Text.Parsec (
    ParseError,
    anyChar,
    char,
    count,
    crlf,
    digit,
    many1,
    manyTill,
    option,
    parse,
    (<|>),
 )
import Text.Parsec.ByteString (Parser)

-- RESP

data Resp
    = Str ByteString
    | Array Int [Resp]
    | BulkStr ByteString
    | Int Int
    | NullBulk
    deriving (Show)

-- parsing

bytes :: Parser ByteString
bytes = fromString <$> manyTill anyChar (char '\r')

resp :: Parser Resp
resp = do
    t <- anyChar
    case t of
        '+' -> Str <$> bytes <* crlf
        '*' -> array
        '$' -> bulkString
        ':' -> Int <$> signedIntParser <* crlf
        _ -> fail $ "invalid type tag: " ++ show t

signedIntParser :: Parser Int
signedIntParser = do
    sign <- option '+' (char '+' <|> char '-')
    digits <- many1 digit
    let intValue = read digits :: Int
    return $ if sign == '-' then -intValue else intValue

array :: Parser Resp
array = do
    n <- signedIntParser <* crlf
    if n >= 0
        then Array n <$> replicateM n resp
        else fail "negative array length"

bulkString :: Parser Resp
bulkString = do
    n <- signedIntParser <* crlf
    if
        | n >= 0 -> BulkStr . fromString <$> count n anyChar <* crlf
        | n == -1 -> pure NullBulk
        | otherwise -> fail "negative bulk length"

decode :: ByteString -> Either ParseError Resp
decode = parse resp ""

crlf' :: ByteString
crlf' = "\r\n"

-- encoding

encode :: Resp -> Either String ByteString
encode (Str x) = Right $ "+" <> x <> crlf'
encode (BulkStr x) = Right $ "$" <> fromString (show $ BS.length x) <> crlf' <> x <> crlf'
encode (Int x) = Right $ ":" <> fromString (show x) <> crlf'
encode NullBulk = Right "$-1\r\n"
encode r = Left $ "Don't know how to encode this" <> fromString (show r)
