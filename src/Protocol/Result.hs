{-# LANGUAGE OverloadedStrings #-}

module Protocol.Result (
    Success (..),
    Failure (..),
    TransactionError (..),
    ReplReply (..),
    RespReply (..),
    Result (..),
    Mode (..),
    AuthError (..),
    resFromMaybe,
    resFromEither,
    modeToPretty,
) where

import Data.ByteString (ByteString)

import Auth.Types (UserProperty)
import Geo.Types (Coordinates)
import Protocol.Command (Command, Key)
import StoreBackend.StreamMap (ConcreteStreamId, StreamMapError (..))
import StoreBackend.StreamMap qualified as SM
import StoreBackend.TypeIndex (ValueType (..))

data Result
    = ResultOk Success
    | ResultErr Failure
    | ResultTx [Either Failure Success]
    | ResultIgnore
    deriving (Show, Eq)

resFromMaybe :: Maybe Success -> Result
resFromMaybe = maybe ResultIgnore ResultOk

resFromEither :: Either Failure Success -> Result
resFromEither = either ResultErr ResultOk

data Success
    = ReplyResp RespReply
    | ReplyOk
    | ReplyPong
    | ReplyPongSubscribed
    | ReplyDouble (Maybe Double)
    | ReplyInt (Maybe Int)
    | ResType (Maybe ValueType)
    | ReplyStreamId ConcreteStreamId
    | ReplySubscribed Key Int
    | ReplyUnsubscribed Key Int
    | ReplyStrings [ByteString]
    | ReplyStreamKeyValues [(Key, [SM.Value ByteString ByteString])]
    | ReplyStreamValues [SM.Value ByteString ByteString]
    | ReplyUserProperty UserProperty
    | ReplyCoordinates [Maybe Coordinates]
    | ReplyReplication ReplReply
    deriving (Show, Eq)

data RespReply
    = RespBulk (Maybe ByteString)
    | RespInt Int
    | RespArrayNull
    | RespSimple ByteString
    deriving (Show, Eq)

data ReplReply
    = ReplOk
    | ReplConfAck Int
    | ReplFullResync ByteString Int
    deriving (Show, Eq)

data Failure
    = ErrStream StreamMapError
    | ErrIncr
    | ErrTx TransactionError
    | ErrCmdNotAllowedInMode Command Mode
    | ErrInvalidCoords Coordinates
    | ErrAuth AuthError
    deriving (Show, Eq)

data AuthError
    = AuthWrongPassword
    | AuthNoAuth
    deriving (Show, Eq)

data TransactionError
    = TxExecWithoutMulti
    | TxNotSupported
    | TxMultiInMulti
    | TxDiscardWithoutMulti
    deriving (Show, Eq)

data Mode = ModeSubscribed deriving (Show, Eq)

modeToPretty :: Mode -> ByteString
modeToPretty ModeSubscribed = "subscribed"
