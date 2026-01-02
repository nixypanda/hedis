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
