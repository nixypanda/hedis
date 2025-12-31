module Wire.Class (ToResp (..), FromResp (..)) where

import Resp.Core (Resp)

class ToResp a where
    toResp :: a -> Resp

class FromResp a where
    fromResp :: Resp -> Either String a
