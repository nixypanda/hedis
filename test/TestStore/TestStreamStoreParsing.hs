{-# LANGUAGE OverloadedStrings #-}

module TestStore.TestStreamStoreParsing where

import Hedgehog (Gen, Property, annotate, failure, forAll, property, (===))
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testProperty)

import Test.Tasty.HUnit (testCase, (@?=))

import Store.StreamStoreParsing
import StoreBackend.StreamMap

tests :: TestTree
tests =
    testGroup
        "StreamStoreParsing"
        [ streamIdTests
        , testProperty "readXAddStreamId <-> showXaddId" prop_XAddRoundtrip
        , testProperty "readXRange <-> showXRange" prop_XRangeRoundtrip
        , testProperty "readConcreteStreamId <-> showStreamId" prop_ConcreteStreamIdRoundtrip
        ]

streamIdTests :: TestTree
streamIdTests =
    testGroup
        "StreamId parser"
        [ testCase "*" $ readXAddStreamId "*" @?= Right AutoId
        , testCase "0-1" $ readXAddStreamId "0-1" @?= Right (ExplicitId 0 (Seq 1))
        , testCase "1526919030474-0" $ readXAddStreamId "1526919030474-0" @?= Right (ExplicitId 1526919030474 (Seq 0))
        , testCase "1-*" $ readXAddStreamId "1-*" @?= Right (ExplicitId 1 SeqAuto)
        , testCase "1526919030474-*" $ readXAddStreamId "1526919030474-*" @?= Right (ExplicitId 1526919030474 SeqAuto)
        ]

genXAddStreamId :: Gen XAddStreamId
genXAddStreamId =
    Gen.choice
        [ pure AutoId
        , ExplicitId <$> genTs <*> genSeq
        ]
  where
    genTs = Gen.int (Range.linear 0 10_000_000)
    genSeq =
        Gen.choice
            [ pure SeqAuto
            , Seq <$> Gen.int (Range.linear 0 10_000_000)
            ]

genConcreteStreamId :: Gen ConcreteStreamId
genConcreteStreamId = (,) <$> Gen.int (Range.linear 0 10_000_000) <*> Gen.int (Range.linear 0 10_000_000)

genXReadStreamId :: Gen XReadStreamId
genXReadStreamId = Gen.choice [pure Dollar, Concrete <$> genConcreteStreamId]

genXRange :: Gen XRange
genXRange = do
    start <- Gen.choice [pure XMinus, XS <$> genStreamId]
    end <- Gen.choice [pure XPlus, XE <$> genStreamId]
    pure (MkXrange start end)
  where
    genStreamId =
        (,)
            <$> Gen.int (Range.linear 0 10_000_000)
            <*> Gen.choice [pure Nothing, Just <$> Gen.int (Range.linear 0 10_000_000)]

prop_XAddRoundtrip :: Property
prop_XAddRoundtrip = property $ do
    sid <- forAll genXAddStreamId
    let bs = showXaddId sid

    case readXAddStreamId bs of
        Right sid' -> sid' === sid
        Left err -> do
            annotate err
            failure

prop_XRangeRoundtrip :: Property
prop_XRangeRoundtrip = property $ do
    xr <- forAll genXRange
    let (s, e) = showXRange xr

    case readXRange s e of
        Right xr' -> xr' === xr
        Left err -> do
            annotate err
            failure

prop_ConcreteStreamIdRoundtrip :: Property
prop_ConcreteStreamIdRoundtrip = property $ do
    sid <- forAll genConcreteStreamId
    let bs = showStreamId sid

    case readConcreteStreamId bs of
        Right sid' -> sid' === sid
        Left err -> do
            annotate err
            failure
