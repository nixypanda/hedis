{-# LANGUAGE OverloadedStrings #-}

{- HLINT ignore "Use <$>" -}

module Rdb.Parser where

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Text.Parsec qualified as P
import Text.Parsec.ByteString qualified as PB

import Data.Attoparsec.ByteString
import Data.Attoparsec.Combinator (lookAhead)
import Data.Bits (Bits (..), shiftR)
import Data.Word (Word64, Word8)
import Parsers (intParser)
import Rdb.Binary
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

-- header

headerParser :: Parser RdbHeader
headerParser = do
    magic <- string "REDIS"
    version <- take 4
    pure $ MkRdbHeader{..}

-- metadata

metadataParser :: Parser [RdbMetadata]
metadataParser = manyUntilNothing metadataEntry

metadataEntry :: Parser (Maybe RdbMetadata)
metadataEntry = do
    opcode <- peekRdbOpcode

    case opcode of
        OpAux -> anyWord8 *> (Just <$> auxField)
        OpResizeDB -> fail "unsupported metadata entry type"
        -- Metadata ends here
        OpSelectDB -> pure Nothing -- database
        OpEOF -> pure Nothing -- empty file
        _ -> fail ("unexpected opcode in metadata: " <> show opcode)

auxField :: Parser RdbMetadata
auxField = do
    k_len <- rdbLenOnly
    key <- take k_len
    val <- rdbString
    pure (MkMetaAux key val)

-- database

dbParser :: Parser [RdbDatabase]
dbParser = manyUntilNothing dbEntry

dbEntry :: Parser (Maybe RdbDatabase)
dbEntry = do
    opcode <- peekRdbOpcode
    case opcode of
        OpSelectDB -> do
            _ <- anyWord8
            identifier <- rdbLenOnly
            store <- storeParser
            pure . Just $ MkRdbDatabase identifier store
        OpEOF -> pure Nothing
        _ -> fail $ "unsupported db entry type: " <> show opcode

storeParser :: Parser RdbStore
storeParser = do
    opcode <- peekRdbOpcode
    case opcode of
        OpResizeDB -> do
            _ <- anyWord8
            size <- rdbLenOnly
            expirySize <- rdbLenOnly
            kv <- kvEntry
            let table = [kv]
            pure $ MkHashStore $ MkHashTable{..}
        _ -> fail $ "unsupported db entry type: " <> show opcode

--

kvEntry :: Parser (ByteString, ByteString)
kvEntry = do
    opcode <- peekRdbOpcode
    case opcode of
        OpTypeString -> do
            _ <- anyWord8
            k_len <- rdbLenOnly
            k <- take k_len
            v_len <- rdbLenOnly
            v <- take v_len
            pure (k, v)
        _ -> fail $ "unsupported kv entry type: " <> show opcode

-- footer (crc64)

crc64Parser :: Parser Word64
crc64Parser = do
    opcode <- peekRdbOpcode
    case opcode of
        OpEOF -> do
            _ <- anyWord8
            anyWord64Be
        o -> fail $ "unexpected opcode in footer: " <> show o

--

peekOpcode :: Parser Word8
peekOpcode =
    peekWord8 >>= maybe (fail "unexpected EOF") pure

peekRdbOpcode :: Parser RdbOpcode
peekRdbOpcode = do
    b <- peekOpcode
    pure $ case b of
        0xFA -> OpAux
        0xFB -> OpResizeDB
        0xFE -> OpSelectDB
        0xFF -> OpEOF
        0x00 -> OpTypeString
        x -> OpUnknown x

rdbLenOnly :: Parser Int
rdbLenOnly = do
    b <- anyWord8
    case b `shiftR` 6 of
        0b00 -> pure $ fromIntegral (b .&. 0x3F)
        0b01 -> do
            b2 <- anyWord8
            pure $ (fromIntegral (b .&. 0x3F) `shiftL` 8) .|. fromIntegral b2
        0b10 -> getInt32Be
        _ -> fail $ "special encoding not supported: " <> show b

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

manyUntilNothing :: Parser (Maybe a) -> Parser [a]
manyUntilNothing p = go []
  where
    go acc = do
        r <- p
        case r of
            Nothing -> pure (reverse acc)
            Just x -> go (x : acc)
