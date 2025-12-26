{-# LANGUAGE OverloadedStrings #-}

module Parsers (
    bytes,
    signedIntParser,
    signedFloatParser,
    readIntBS,
    readFloatBS,
    readStreamId,
    readXRange,
    readConcreteStreamId,
) where

import Control.Applicative ((<|>))
import Control.Monad (join)
import Data.ByteString (ByteString)
import Data.Functor (($>))
import Data.String (fromString)
import StreamMap (
    ConcreteStreamId,
    IdSeq (..),
    StreamId (..),
    XEnd (..),
    XRange (..),
    XStart (..),
    XStreamId,
 )
import Text.Parsec (
    anyChar,
    char,
    digit,
    eof,
    many1,
    manyTill,
    option,
    optionMaybe,
    parse,
    spaces,
 )
import Text.Parsec.ByteString (Parser)

bytes :: Parser ByteString
bytes = fromString <$> manyTill anyChar (char '\r')

intParser :: Parser Int
intParser = fromIntegral <$> integer

signedIntParser :: Parser Int
signedIntParser = do
    sign <- option '+' (char '+' <|> char '-')
    digits <- many1 digit
    let intValue = read digits :: Int
    return $ if sign == '-' then -intValue else intValue

signedFloatParser :: Parser Float
signedFloatParser = do
    sign <- optionMaybe (char '-')
    i <- many1 digit
    f <- option "" (char '.' *> many1 digit)
    let s = maybe "" pure sign <> i <> if null f then "" else '.' : f
    pure (read s)

parseBS :: Parser a -> ByteString -> Either String a
parseBS p bs =
    case parse (p <* eof) "<resp-arg>" bs of
        Left err -> Left (show err)
        Right x -> Right x

readIntBS :: ByteString -> Either String Int
readIntBS = parseBS signedIntParser

readFloatBS :: ByteString -> Either String Float
readFloatBS = parseBS signedFloatParser

-- Redis stream id

streamIdParser :: Parser StreamId
streamIdParser = autoId <|> explicitId

autoId :: Parser StreamId
autoId = char '*' $> AutoId

explicitId :: Parser StreamId
explicitId = do
    ts <- intParser
    _ <- char '-'
    ExplicitId ts <$> seqParser

seqParser :: Parser IdSeq
seqParser = (char '*' $> SeqAuto) <|> (Seq <$> intParser)

integer :: Parser Integer
integer = read <$> many1 digit

readStreamId :: ByteString -> Either String StreamId
readStreamId = parseBS streamIdParser

-- Xrange

xStreamId :: Parser XStreamId
xStreamId = do
    ts <- intParser
    mSeq <- optionMaybe (char '-' *> optionMaybe intParser)
    pure (ts, join mSeq)

xStartParser :: Parser XStart
xStartParser = (char '-' $> XMinus) <|> (XS <$> xStreamId)

xEndParser :: Parser XEnd
xEndParser = (char '+' $> XPlus) <|> (XE <$> xStreamId)

xRangeParser :: Parser XRange
xRangeParser = do
    s <- xStartParser
    spaces
    e <- xEndParser
    eof
    pure (MkXrange s e)

readXRange :: ByteString -> ByteString -> Either String XRange
readXRange s e = parseBS xRangeParser (s <> " " <> e)

-- Concreate StreamId

concreteStreamIdParser :: Parser ConcreteStreamId
concreteStreamIdParser = do
    ts <- intParser
    _ <- char '-'
    (,) ts <$> intParser

readConcreteStreamId :: ByteString -> Either String ConcreteStreamId
readConcreteStreamId = parseBS concreteStreamIdParser
