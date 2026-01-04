{-# LANGUAGE OverloadedStrings #-}

module Types.Result (
    Success (..),
    Failure (..),
    TransactionError (..),
    ReplReply (..),
    RespReply (..),
    Result (..),
    Mode (..),
    AuthError (..),
    resFromEither,
    modeToPretty,
) where

import Data.ByteString (ByteString)

import Auth (UserProperty)
import Geo.Types (Coordinates)
import StoreBackend.StreamMap (ConcreteStreamId, StreamMapError (..))
import StoreBackend.StreamMap qualified as SM
import StoreBackend.TypeIndex (ValueType (..))
import Types.Command (Command, Key)

data Result
    = ResultOk Success
    | ResultErr Failure
    | ResultTx [Either Failure Success]
    deriving (Show, Eq)

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
    | ErrWaitOnRplica
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
