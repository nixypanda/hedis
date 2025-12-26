module Parsers (bytes, signedIntParser, signedFloatParser, readIntBS, readFloatBS, readStreamId) where

import Control.Applicative ((<|>))
import Data.ByteString (ByteString)
import Data.Functor (($>))
import Data.String (fromString)
import Text.Parsec (anyChar, char, digit, eof, many1, manyTill, option, optionMaybe, parse)
import Text.Parsec.ByteString (Parser)

import StreamMap (IdSeq (..), StreamId (..))

bytes :: Parser ByteString
bytes = fromString <$> manyTill anyChar (char '\r')

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
    ts <- integer
    _ <- char '-'
    ExplicitId (fromIntegral ts) <$> seqParser

seqParser :: Parser IdSeq
seqParser = (char '*' $> SeqAuto) <|> (Seq . fromIntegral <$> integer)

integer :: Parser Integer
integer = read <$> many1 digit

readStreamId :: ByteString -> Either String StreamId
readStreamId = parseBS streamIdParser
