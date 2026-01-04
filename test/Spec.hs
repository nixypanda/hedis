module Main (main) where

import Test.Tasty (TestTree, defaultMain, testGroup)

import TestGeo.TestEncoding qualified
import TestParsers qualified
import TestRdb.TestParser qualified
import TestResp.TestCore qualified
import TestStore.TestBackend.TestExpiringMap qualified
import TestStore.TestBackend.TestListMap qualified
import TestStore.TestBackend.TestSortedSetMap qualified
import TestStore.TestBackend.TestStreamMap qualified
import TestStore.TestList qualified
import TestStore.TestStreamStoreParsing qualified
import TestWire.TestClient.TestCommand qualified

tests :: TestTree
tests =
    testGroup
        "redis tests"
        [ TestStore.TestBackend.TestExpiringMap.tests
        , TestStore.TestBackend.TestListMap.tests
        , TestStore.TestBackend.TestStreamMap.tests
        , TestStore.TestBackend.TestSortedSetMap.tests
        , TestStore.TestStreamStoreParsing.tests
        , TestStore.TestList.tests
        , TestParsers.tests
        , TestResp.TestCore.tests
        , TestRdb.TestParser.tests
        , TestGeo.TestEncoding.tests
        , TestWire.TestClient.TestCommand.tests
        ]

main :: IO ()
main = defaultMain tests
