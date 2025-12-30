module CommandResult (
    CommandResult (..),
    CommandError (..),
    TransactionError (..),
    ReplResult (..),
) where

import Data.ByteString (ByteString)

import Command (Key)
import StoreBackend.StreamMap (ConcreteStreamId, StreamMapError (..))
import StoreBackend.StreamMap qualified as SM
import StoreBackend.TypeIndex (ValueType (..))

data CommandResult
    = RBulk (Maybe ByteString)
    | RInt Int
    | RArrayNull
    | RArraySimple [ByteString]
    | RArrayKeyValues [(Key, [SM.Value ByteString ByteString])]
    | RStreamId ConcreteStreamId
    | RArrayStreamValues [SM.Value ByteString ByteString]
    | RArray [CommandResult]
    | RRepl ReplResult
    | ResPong
    | ResOk
    | RSimple ByteString
    | ResType (Maybe ValueType)
    | -- wtf
      RErr CommandError
    deriving (Show, Eq)

data ReplResult
    = ReplOk
    | ReplConfAck Int
    | ResFullResync ByteString Int
    deriving (Show, Eq)

data CommandError
    = RStreamError StreamMapError
    | RIncrError
    | RTxErr TransactionError
    deriving (Show, Eq)

data TransactionError
    = RExecWithoutMulti
    | RNotSupportedInTx
    | RMultiInMulti
    | RDiscardWithoutMulti
    deriving (Show, Eq)
