module Main (main) where

import Test.Tasty (TestTree, defaultMain, testGroup)

import TestParsers qualified
import TestStore.TestExpiringMap qualified
import TestStore.TestListMap qualified
import TestStore.TestStreamMap qualified

tests :: TestTree
tests =
    testGroup
        "redis tests"
        [ TestStore.TestExpiringMap.tests
        , TestStore.TestListMap.tests
        , TestStore.TestStreamMap.tests
        , TestParsers.tests
        ]

main :: IO ()
main = defaultMain tests
