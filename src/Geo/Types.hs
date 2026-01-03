module Geo.Types where

data Coordinates = MkCoordinates
    { latitude :: Double
    , longitude :: Double
    }
    deriving (Show, Eq)

eqWith :: Double -> Coordinates -> Coordinates -> Bool
eqWith eps (MkCoordinates lat1 lon1) (MkCoordinates lat2 lon2) =
    abs (lat1 - lat2) < eps && abs (lon1 - lon2) < eps

minLatitude :: Double
minLatitude = -85.05112878

maxLatitude :: Double
maxLatitude = 85.05112878

minLongitude :: Double
minLongitude = -180.0

maxLongitude :: Double
maxLongitude = 180.0

coordinatesAreValid :: Coordinates -> Bool
coordinatesAreValid (MkCoordinates lat long) =
    minLatitude <= lat && lat <= maxLatitude && minLongitude <= long && long <= maxLongitude

-- Redis uses ~6372797.560856!
earthRadiusMeters :: Double
earthRadiusMeters = 6372797.560856

degToRad :: Double -> Double
degToRad d = d * pi / 180

haversineDistance :: Coordinates -> Coordinates -> Double
haversineDistance (MkCoordinates lat1 lon1) (MkCoordinates lat2 lon2) =
    earthRadiusMeters * c
  where
    φ1 = degToRad lat1
    φ2 = degToRad lat2
    φΔ = degToRad (lat2 - lat1)
    λΔ = degToRad (lon2 - lon1)

    a = sin (φΔ / 2) ^ (2 :: Int) + cos φ1 * cos φ2 * sin (λΔ / 2) ^ (2 :: Int)

    c = 2 * atan2 (sqrt a) (sqrt (1 - a))
