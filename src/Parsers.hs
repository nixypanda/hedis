module Parsers (bytes, signedIntParser, signedFloatParser) where

import Control.Applicative ((<|>))
import Data.ByteString (ByteString)
import Data.String (fromString)
import Text.Parsec (anyChar, char, digit, many1, manyTill, option, optionMaybe)
import Text.Parsec.ByteString (Parser)

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
