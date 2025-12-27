{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}

module Redis (Env (..), Redis (..), RedisError (..), handleRequest, mkNewEnv) where

import Control.Concurrent.STM (TVar, atomically)
import Control.Exception (Exception)
import Control.Monad.Except (ExceptT (..), MonadError, liftEither)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (MonadReader, ReaderT, asks)
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString (ByteString)
import Data.List (singleton)
import Data.String (fromString)
import Data.Time (getCurrentTime)
import System.Log.FastLogger (
    LogType' (..),
    TimedFastLogger,
    ToLogStr (toLogStr),
    defaultBufSize,
    newTimeCache,
    newTimedFastLogger,
    simpleTimeFormat,
 )
import System.Timeout (timeout)
import Text.Parsec (ParseError)
import Time (nominalDiffTimeToMicros)

import Command (Command (..), CommandResult (..), respToCmd, resultToResp)
import Resp (decode, encode)
import Store.ListStore (ListStore)
import Store.ListStore qualified as LS
import Store.StreamStore (StreamStore)
import Store.StreamStore qualified as StS
import Store.StringStore (StringStore)
import Store.StringStore qualified as SS
import Store.TypeStore (IncorrectType, TypeIndex)
import Store.TypeStore qualified as TS
import StoreBackend.TypeIndex (ValueType (..))

-- Types

data RedisError
    = ParsingError ParseError
    | EncodeError String
    | EmptyBuffer
    | Unimplemented String
    | ConversionError String
    | InvalidCommand String
    | WrongType IncorrectType
    deriving (Show, Exception)

data Env = MkEnv
    { stringStore :: TVar StringStore
    , listStore :: TVar ListStore
    , typeIndex :: TVar TypeIndex
    , streamStore :: TVar StreamStore
    , envLogger :: TimedFastLogger
    }

newtype Redis a = MkRedis {runRedis :: ReaderT Env (ExceptT RedisError IO) a}
    deriving newtype (Functor, Applicative, Monad, MonadError RedisError, MonadIO, MonadReader Env)

mkNewEnv :: IO Env
mkNewEnv = do
    stringStore <- atomically SS.emptySTM
    listStore <- atomically LS.emptySTM
    typeIndex <- atomically TS.emptySTM
    streamStore <- atomically StS.emptySTM
    timeCache <- newTimeCache simpleTimeFormat
    (envLogger, _cleanup) <- newTimedFastLogger timeCache (LogStderr defaultBufSize)
    pure MkEnv{..}

-- Execution

runCmd :: Command -> Redis CommandResult
runCmd cmd = do
    tvListMap <- asks listStore
    tvStringMap <- asks stringStore
    tvTypeIndex <- asks typeIndex
    tvStreamMap <- asks streamStore
    now <- liftIO getCurrentTime
    case cmd of
        Ping -> pure $ RSimple "PONG"
        Echo xs -> pure $ RBulk (Just xs)
        -- type index
        Type x -> do
            ty <- liftIO $ atomically $ TS.getTypeSTM tvTypeIndex x
            pure $ maybe (RSimple "none") (RSimple . fromString . show) ty
        -- String Store
        Set key val mexpiry -> do
            liftIO $ atomically (TS.setIfAvailable tvTypeIndex key VString *> SS.setSTM tvStringMap key val now mexpiry)
            pure $ RSimple "OK"
        Get key -> do
            val <- liftIO $ atomically (SS.getSTM tvStringMap key now)
            pure $ RBulk val
        -- List Store
        Rpush key xs -> do
            count <- liftIO (atomically (TS.setIfAvailable tvTypeIndex key VList *> LS.rpushSTM tvListMap key xs))
            pure $ RInt count
        Lpush key xs -> do
            count <- liftIO (atomically (TS.setIfAvailable tvTypeIndex key VList *> LS.lpushSTM tvListMap key xs))
            pure $ RInt count
        Lpop key Nothing -> do
            val <- liftIO (atomically (LS.lpopSTM tvListMap key))
            pure $ RBulk val
        Lpop key (Just mLen) -> do
            vals <- liftIO (atomically (LS.lpopsSTM tvListMap key mLen))
            pure $ RArray vals
        Blpop key 0 -> do
            vals <- liftIO $ atomically (LS.blpopSTM tvListMap key)
            pure $ RArray vals
        Blpop key tout -> do
            val <- liftIO (timeout (nominalDiffTimeToMicros tout) (atomically (LS.blpopSTM tvListMap key)))
            pure $ maybe RNullArray RArray val
        Lrange key range -> do
            vals <- liftIO (atomically (LS.lrangeSTM tvListMap key range))
            pure $ RArray vals
        Llen key -> do
            len <- liftIO (atomically (LS.llenSTM tvListMap key))
            pure $ RInt len
        -- Stream Store
        Xadd key sId chunked -> do
            val <- liftIO $ atomically (TS.setIfAvailable tvTypeIndex key VStream *> StS.xAddSTM tvStreamMap key sId chunked now)
            pure $ either RStreamError RStreamId val
        XRange key range -> do
            vals <- liftIO (atomically (StS.xRangeSTM tvStreamMap key range))
            pure $ RStreamValues vals
        XRead keys -> do
            vals <- liftIO (atomically (StS.xReadSTM tvStreamMap keys))
            pure $ RKeyValues vals
        XReadBlock 0 key sid -> do
            sid' <- liftIO $ atomically (StS.xResolveStreamIdSTM tvStreamMap key sid)
            vals <- liftIO (atomically (StS.xReadBlockSTM tvStreamMap key sid'))
            pure $ (RKeyValues . singleton) vals
        XReadBlock tout key sid -> do
            sid' <- liftIO $ atomically (StS.xResolveStreamIdSTM tvStreamMap key sid)
            vals <- liftIO (timeout (nominalDiffTimeToMicros tout) (atomically (StS.xReadBlockSTM tvStreamMap key sid')))
            pure $ maybe RNullArray (RKeyValues . singleton) vals

-- Command execution

handleRequest :: ByteString -> Redis ByteString
handleRequest bs = do
    logInfo ("Received: " <> show bs)
    resp <- liftEither $ first ParsingError $ decode bs
    logDebug ("Decoded: " <> show resp)
    cmd <- liftEither $ first ConversionError $ respToCmd resp
    logDebug ("Command: " <> show cmd)
    result <- runCmd cmd
    logDebug ("Execution result: " <> show result)
    let resultResp = resultToResp result
    encoded <- liftEither $ first EncodeError $ encode resultResp
    logInfo ("Encoded: " <> show encoded)
    pure encoded

logInfo :: (MonadReader Env m, MonadIO m) => String -> m ()
logInfo msg = do
    logger <- asks envLogger
    liftIO $ logger (\t -> toLogStr (show t <> "[INFO] " <> msg <> "\n"))

logDebug :: (MonadReader Env m, MonadIO m) => String -> m ()
logDebug msg = do
    logger <- asks envLogger
    liftIO $ logger (\t -> toLogStr (show t <> "[DEBUG] " <> msg <> "\n"))
