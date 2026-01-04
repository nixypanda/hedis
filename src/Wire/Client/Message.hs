{-# LANGUAGE OverloadedStrings #-}

module Wire.Client.Message (msgToResp, respToMsg) where

import Resp.Core
import Types.Message

msgToResp :: Message -> Resp
msgToResp (MkMessage chan msg) = Array 3 [BulkStr "message", BulkStr chan, BulkStr msg]

respToMsg :: Resp -> Either String Message
respToMsg (Array 3 [BulkStr "message", BulkStr chan, BulkStr msg]) = Right $ MkMessage chan msg
respToMsg r = Left $ "Invalid message: " <> show r
