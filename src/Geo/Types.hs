module Geo.Types where

data Coordinates = MkCoordinates
    { latitude :: Double
    , longitude :: Double
    }
    deriving (Show, Eq)

eqWith :: Double -> Coordinates -> Coordinates -> Bool
eqWith eps (MkCoordinates lat1 lon1) (MkCoordinates lat2 lon2) =
    abs (lat1 - lat2) < eps
        && abs (lon1 - lon2) < eps

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
