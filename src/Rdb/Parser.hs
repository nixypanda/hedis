{-# LANGUAGE OverloadedStrings #-}

{- HLINT ignore "Use <$>" -}

module Rdb.Parser where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Numeric (showHex)
import Text.Parsec qualified as P
import Text.Parsec.ByteString qualified as PB

import Data.Attoparsec.ByteString
import Data.Attoparsec.Combinator (lookAhead)
import Data.Bits (Bits (..), shiftR)
import Data.Word (Word64)
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

-----------------------------------------------------------------

rdbParser :: Parser RdbData
rdbParser = do
    header <- headerParser
    metadata <- metadataParser
    dbs <- dbParser
    checksum <- crc64Parser
    pure $ MkRdbData{..}

headerParser :: Parser RdbHeader
headerParser = do
    magic <- string "REDIS"
    version <- take 4
    pure $ MkRdbHeader{..}

metadataParser :: Parser [RdbMetadata]
metadataParser = go []
  where
    go acc = do
        m <- metadataEntry
        case m of
            Nothing -> pure (reverse acc)
            Just x -> go (x : acc)

auxField :: Parser RdbMetadata
auxField = do
    key <- rdbString
    val <- rdbString
    pure (MkMetaAux key val)

metadataEntry :: Parser (Maybe RdbMetadata)
metadataEntry = do
    opcode <- peekWord8

    case opcode of
        Just 0xFA -> anyWord8 *> (Just <$> auxField)
        Just 0xFB -> fail "unsupported metadata entry type"
        -- Metadata ends here
        Just 0xFE -> pure Nothing
        Just 0xFF -> pure Nothing
        _ -> fail ("unexpected opcode in metadata: " <> show opcode)

dbParser :: Parser [RdbDatabase]
dbParser = go []
  where
    go acc = do
        m <- dbEntry
        case m of
            Nothing -> pure (reverse acc)
            Just x -> go (x : acc)

dbEntry :: Parser (Maybe RdbDatabase)
dbEntry = do
    opcode <- peekWord8
    case opcode of
        Just 0xFF -> pure Nothing
        _ -> fail "unsupported db entry type"

crc64Parser :: Parser Word64
crc64Parser = do
    code <- word8 0xFF
    anyWord64Be

-- helpers
--
-- >>> shiftR 0x09 6
-- 0

rdbLen :: Parser (Either MetaVal Int)
rdbLen = do
    b <- anyWord8
    case b `shiftR` 6 of
        0b00 -> pure . Right $ fromIntegral (b .&. 0x3F)
        0b01 -> do
            b2 <- anyWord8
            pure . Right $ (fromIntegral (b .&. 0x3F) `shiftL` 8) .|. fromIntegral b2
        0b10 -> Right <$> getInt32Be
        0b11 -> case b .&. 0x3F of
            0 -> Left . MVInt <$> getInt8Be
            1 -> Left . MVInt <$> getInt16Le
            2 -> Left . MVInt <$> getInt32Le
            r -> do
                rest <- lookAhead takeByteString
                fail $ "special encoding not supported: - " <> show r <> "remainging: " <> show (hexDump rest)
        _ -> fail "impossible"

rdbString :: Parser MetaVal
rdbString = do
    len <- rdbLen
    case len of
        Right len' -> MVString <$> take len'
        Left mval -> pure mval

getInt8Be :: Parser Int
getInt8Be = fromIntegral <$> anyWord8

getInt16Be :: Parser Int
getInt16Be = do
    b1 <- fromIntegral <$> anyWord8
    b2 <- fromIntegral <$> anyWord8
    return $ (b1 `shiftL` 8) .|. b2

getInt32Be :: Parser Int
getInt32Be = do
    b1 <- fromIntegral <$> anyWord8
    b2 <- fromIntegral <$> anyWord8
    b3 <- fromIntegral <$> anyWord8
    b4 <- fromIntegral <$> anyWord8
    return $ (b1 `shiftL` 24) .|. (b2 `shiftL` 16) .|. (b3 `shiftL` 8) .|. b4

hexDump :: ByteString -> String
hexDump =
    unwords . map toHex . BS.unpack
  where
    toHex b =
        let s = showHex b ""
         in if length s == 1 then '0' : s else s

getInt16Le :: Parser Int
getInt16Le = do
    b1 <- anyWord8
    b2 <- anyWord8
    pure $ fromIntegral b1 .|. (fromIntegral b2 `shiftL` 8)

getInt32Le :: Parser Int
getInt32Le = do
    b1 <- anyWord8
    b2 <- anyWord8
    b3 <- anyWord8
    b4 <- anyWord8
    pure $ fromIntegral b1 .|. (fromIntegral b2 `shiftL` 8) .|. (fromIntegral b3 `shiftL` 16) .|. (fromIntegral b4 `shiftL` 24)

anyWord64Be :: Parser Word64
anyWord64Be = do
    b1 <- anyWord8
    b2 <- anyWord8
    b3 <- anyWord8
    b4 <- anyWord8
    b5 <- anyWord8
    b6 <- anyWord8
    b7 <- anyWord8
    b8 <- anyWord8
    pure $
        (fromIntegral b1 `shiftL` 56)
            .|. (fromIntegral b2 `shiftL` 48)
            .|. (fromIntegral b3 `shiftL` 40)
            .|. (fromIntegral b4 `shiftL` 32)
            .|. (fromIntegral b5 `shiftL` 24)
            .|. (fromIntegral b6 `shiftL` 16)
            .|. (fromIntegral b7 `shiftL` 8)
            .|. fromIntegral b8
