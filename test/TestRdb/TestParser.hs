{-# LANGUAGE OverloadedStrings #-}

module TestRdb.TestParser (tests) where

import Test.Tasty
import Test.Tasty.HUnit

import Data.Attoparsec.ByteString (parseOnly)
import Data.ByteString qualified as BS

import Data.ByteString (ByteString)
import Rdb.Parser
import Rdb.Type

headerBS :: BS.ByteString
headerBS = BS.pack [0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31]

meta1BS :: BS.ByteString
meta1BS = BS.pack [0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30]

meta2BS :: BS.ByteString
meta2BS = BS.pack [0x0a, 0x72, 0x65, 0x64, 0x69, 0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40]

meta3BS :: BS.ByteString
meta3BS = BS.pack [0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 0x6d, 0x08, 0xbc, 0x65]

meta4BS :: BS.ByteString
meta4BS = BS.pack [0x08, 0x75, 0x73, 0x65, 0x64, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 0x10, 0x00]

meta5BS :: BS.ByteString
meta5BS = BS.pack [0x08, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00]

meta1Res :: RdbMetadata
meta1Res = MkMetaAux (MVString "redis-ver") (MVString "7.2.0")

meta2Res :: RdbMetadata
meta2Res = MkMetaAux (MVString "redis-bits") (MVInt 64)

meta3Res :: RdbMetadata
meta3Res = MkMetaAux (MVString "ctime") (MVInt 1706821741)

meta4Res :: RdbMetadata
meta4Res = MkMetaAux (MVString "used-mem") (MVInt 1098928)

meta5Res :: RdbMetadata
meta5Res = MkMetaAux (MVString "aof-base") (MVInt 0)

auxFieldPrefix :: BS.ByteString
auxFieldPrefix = BS.pack [0xFA]

metaSectionEnd :: ByteString
metaSectionEnd = BS.pack [0xFE]

tests :: TestTree
tests =
    testGroup
        "RDB parser"
        [ testCase "parses header and AUX metadata from dump.rdb" $ do
            bs <- BS.readFile "test/data/empty.rdb"

            case parseOnly rdbParser bs of
                Left err ->
                    assertFailure ("RDB parse failed: " <> err)
                Right rdb -> do
                    print rdb
                    rdb.header.version @?= "0011"
        , testCase "rdb header parsing" $ case parseOnly headerParser headerBS of
            Left err -> assertFailure ("Header parse failed: " <> err)
            Right hdr -> hdr.version @?= "0011"
        , testCase "aux field in metadata parsing" $ case parseOnly auxField meta1BS of
            Left err -> assertFailure ("Aux field parse failed: " <> err)
            Right res -> res @?= meta1Res
        , testCase "aux field in metadata parsing" $ case parseOnly auxField meta2BS of
            Left err -> assertFailure ("Aux field parse failed: " <> err)
            Right res -> res @?= meta2Res
        , testCase "aux field in metadata parsing" $ case parseOnly auxField meta3BS of
            Left err -> assertFailure ("Aux field parse failed: " <> err)
            Right res -> res @?= meta3Res
        , testCase "aux field in metadata parsing" $ case parseOnly auxField meta4BS of
            Left err -> assertFailure ("Aux field parse failed: " <> err)
            Right res -> res @?= meta4Res
        , testCase "aux field in metadata parsing" $ case parseOnly auxField meta5BS of
            Left err -> assertFailure ("Aux field parse failed: " <> err)
            Right res -> res @?= meta5Res
        , testCase "Full metadata parser" $
            let input = BS.concat [auxFieldPrefix, BS.intercalate auxFieldPrefix [meta1BS, meta2BS, meta3BS, meta4BS, meta5BS], metaSectionEnd]
             in case parseOnly metadataParser input of
                    Left err -> assertFailure ("Aux field parse failed: " <> err)
                    Right res -> res @?= [meta1Res, meta2Res, meta3Res, meta4Res, meta5Res]
        ]
