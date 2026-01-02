module TestGeo.TestEncoding (tests) where

import Hedgehog
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Tasty
import Test.Tasty.Hedgehog

import Geo.Encoding
import Geo.Types (Coordinates (..), eqWith)

genCoordinates :: Gen Coordinates
genCoordinates =
    MkCoordinates
        <$> Gen.double (Range.linearFrac minLatitude maxLatitude)
        <*> Gen.double (Range.linearFrac minLongitude maxLongitude)

prop_encodeDecodeDual :: Property
prop_encodeDecodeDual =
    property $ do
        coords <- forAll genCoordinates
        let decoded = decode (encode coords)
        annotateShow coords
        annotateShow decoded
        assert $ eqWith 1e-5 coords decoded

tests :: TestTree
tests =
    testGroup
        "Geo.Encoding"
        [ testProperty
            "encode/decode are duals within precision"
            prop_encodeDecodeDual
        ]
