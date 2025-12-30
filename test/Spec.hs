module Main (main) where

import Test.Tasty (TestTree, defaultMain, testGroup)

import TestParsers qualified
import TestResp.TestCore qualified
import TestStore.TestListStore qualified
import TestStore.TestStreamStoreParsing qualified
import TestStoreBackend.TestExpiringMap qualified
import TestStoreBackend.TestListMap qualified
import TestStoreBackend.TestStreamMap qualified

tests :: TestTree
tests =
    testGroup
        "redis tests"
        [ TestStoreBackend.TestExpiringMap.tests
        , TestStoreBackend.TestListMap.tests
        , TestStoreBackend.TestStreamMap.tests
        , TestParsers.tests
        , TestResp.TestCore.tests
        , TestStore.TestStreamStoreParsing.tests
        , TestStore.TestListStore.tests
        ]

main :: IO ()
main = defaultMain tests
