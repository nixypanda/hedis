{-# LANGUAGE OverloadedStrings #-}

module TestResp.TestCommand (tests) where

import Hedgehog (Property, evalEither, forAll, property, (===))
import Test.Tasty
import Test.Tasty.Hedgehog

import Gen.Command (genCommand)
import Resp.Command

--------------------------------------------------------------------------------
-- Tasty entry
--------------------------------------------------------------------------------

tests :: TestTree
tests =
    testGroup
        "Command <-> RESP duality"
        [ testProperty
            "respToCmd . cmdToResp == Right"
            prop_cmd_resp_dual
        ]

--------------------------------------------------------------------------------
-- Property
--------------------------------------------------------------------------------

prop_cmd_resp_dual :: Property
prop_cmd_resp_dual = property $ do
    cmd <- forAll genCommand

    let resp = cmdToResp cmd
    decoded <- evalEither (respToCmd resp)

    decoded === cmd
