{-# OPTIONS_GHC -Wno-type-defaults #-}

module Geo.Encoding (encode, decode) where

import Data.Bits (shiftL, shiftR, (.&.), (.|.))
import Data.Word (Word32, Word64)

import Geo.Types

latRange :: Double
latRange = maxLatitude - minLatitude

longRange :: Double
longRange = maxLongitude - minLongitude

spreadInt32ToInt64 :: Word32 -> Word64
spreadInt32ToInt64 v =
    let v' = fromIntegral v
        v1 = (v' .|. (v' `shiftL` 16)) .&. 0x0000FFFF0000FFFF
        v2 = (v1 .|. (v1 `shiftL` 8)) .&. 0x00FF00FF00FF00FF
        v3 = (v2 .|. (v2 `shiftL` 4)) .&. 0x0F0F0F0F0F0F0F0F
        v4 = (v3 .|. (v3 `shiftL` 2)) .&. 0x3333333333333333
     in (v4 .|. (v4 `shiftL` 1)) .&. 0x5555555555555555

interleave :: Word32 -> Word32 -> Word64
interleave x y =
    let xSpread = spreadInt32ToInt64 x
        ySpread = spreadInt32ToInt64 y
        yShifted = ySpread `shiftL` 1
     in xSpread .|. yShifted

encode :: Coordinates -> Word64
encode MkCoordinates{..} =
    let
        -- Normalize to the range 0-2^26
        normalizedLatitude = (2 ^ 26) * (latitude - minLatitude) / latRange
        normalizedLongitude = (2 ^ 26) * (longitude - minLongitude) / longRange

        -- Truncate to integers
        latInt = truncate normalizedLatitude :: Word32
        lonInt = truncate normalizedLongitude :: Word32
     in
        interleave latInt lonInt

compactInt64ToInt32 :: Word64 -> Word32
compactInt64ToInt32 v =
    let v1 = v .&. 0x5555555555555555
        v2 = (v1 .|. (v1 `shiftR` 1)) .&. 0x3333333333333333
        v3 = (v2 .|. (v2 `shiftR` 2)) .&. 0x0F0F0F0F0F0F0F0F
        v4 = (v3 .|. (v3 `shiftR` 4)) .&. 0x00FF00FF00FF00FF
        v5 = (v4 .|. (v4 `shiftR` 8)) .&. 0x0000FFFF0000FFFF
     in fromIntegral $ (v5 .|. (v5 `shiftR` 16)) .&. 0x00000000FFFFFFFF

convertGridNumbersToCoordinates :: Word32 -> Word32 -> Coordinates
convertGridNumbersToCoordinates latNum longNum =
    let
        -- Calculate the grid boundaries
        gridLatMin = minLatitude + latRange * (fromIntegral latNum / (2 ^ 26))
        gridLatMax = minLatitude + latRange * (fromIntegral (latNum + 1) / (2 ^ 26))
        gridLongMin = minLongitude + longRange * (fromIntegral longNum / (2 ^ 26))
        gridLongMax = minLongitude + longRange * (fromIntegral (longNum + 1) / (2 ^ 26))

        -- Calculate the center point of the grid cell
        lat = (gridLatMin + gridLatMax) / 2
        lon = (gridLongMin + gridLongMax) / 2
     in
        MkCoordinates lat lon

decode :: Word64 -> Coordinates
decode geoCode =
    let
        -- Align bits of both latitude and longitude to take even-numbered position
        y = geoCode `shiftR` 1
        x = geoCode

        -- Compact bits back to 32-bit ints
        gridLatitudeNumber = compactInt64ToInt32 x
        gridLongitudeNumber = compactInt64ToInt32 y
     in
        convertGridNumbersToCoordinates gridLatitudeNumber gridLongitudeNumber
