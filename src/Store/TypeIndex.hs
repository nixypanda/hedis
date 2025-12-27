{-# LANGUAGE OverloadedStrings #-}

module Store.TypeIndex (ValueType (..), RequiredType (..), toString, checkAvailable) where

import Data.ByteString (ByteString)

data ValueType = VString | VList | VStream deriving (Eq)

instance Show ValueType where
    show :: ValueType -> String
    show VString = "string"
    show VList = "list"
    show VStream = "stream"

toString :: ValueType -> ByteString
toString VString = "string"
toString VList = "list"
toString VStream = "stream"

data RequiredType = AbsentOr ValueType | MustBe ValueType

checkAvailable :: Maybe ValueType -> RequiredType -> Bool
checkAvailable mTy req =
    case (mTy, req) of
        (Nothing, AbsentOr _) -> True
        (Just t, AbsentOr t') | t == t' -> True
        (Just t, MustBe t') | t == t' -> True
        _ -> False
