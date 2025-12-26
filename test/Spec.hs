module Main (main) where

import Test.Tasty (TestTree, defaultMain, testGroup)

import TestExpiringMap qualified
import TestListMap qualified
import TestParsers qualified

tests :: TestTree
tests =
    testGroup
        "redis tests"
        [ TestExpiringMap.tests
        , TestListMap.tests
        , TestParsers.tests
        ]

main :: IO ()
main = defaultMain tests
