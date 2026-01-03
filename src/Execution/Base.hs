{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Execution.Base (runCmdSTM, runCmdIO, runServerInfoCmds, runConfigInfoCmds) where

import Control.Concurrent.STM (STM, atomically)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (asks)
import Data.List (singleton)
import Data.String (IsString (..))
import Data.Time (UTCTime)

import Protocol.Command
import Protocol.Result
import Replication.Config (HasRdbConfig (..), replicationInfo)
import Store.ListStore qualified as LS
import Store.SortedSetStore qualified as SSS
import Store.StreamStore qualified as StS
import Store.StringStore qualified as SS
import Store.TypeStore qualified as TS
import Time (timeout')
import Types.Redis

runCmdSTM ::
    (HasStores r) => Env r -> UTCTime -> CmdSTM -> STM (Either Failure Success)
runCmdSTM env now cmd = do
    let tvListMap = getListStore env
        tvStringMap = getStringStore env
        tvTypeIndex = getTypeIndex env
        tvStreamMap = getStreamStore env
        tvSortedSetMap = getSortedSetStore env
    case cmd of
        CmdPing -> pure $ Right ReplyPong
        CmdEcho xs -> pure $ Right $ ReplyResp $ RespBulk (Just xs)
        -- type index
        CmdType x -> do
            ty <- TS.getTypeSTM tvTypeIndex x
            pure $ Right $ ResType ty
        CmdKeys -> do
            keys <- TS.getKeysSTM tvTypeIndex
            pure $ Right $ ReplyStrings keys
        STMString c -> SS.runStringStoreSTM tvTypeIndex tvStringMap now c
        STMList c -> Right <$> LS.runListStoreSTM tvTypeIndex tvListMap c
        STMStream c -> StS.runStreamStoreSTM tvTypeIndex tvStreamMap now c
        STMSortedSet c -> Right <$> SSS.runSortedSetStoreSTM tvTypeIndex tvSortedSetMap c
        STMGeo c -> SSS.runGeoStoreSTM tvTypeIndex tvSortedSetMap c

runCmdIO :: (HasStores r) => CmdIO -> Redis r Success
runCmdIO cmd = do
    tvListMap <- asks getListStore
    tvStreamMap <- asks getStreamStore
    case cmd of
        -- List Store
        CmdBLPop key 0 -> do
            vals <- liftIO $ atomically (LS.blpopSTM tvListMap key)
            pure $ ReplyStrings vals
        CmdBLPop key t -> do
            val <- liftIO (timeout' t (atomically (LS.blpopSTM tvListMap key)))
            pure $ maybe (ReplyResp RespArrayNull) ReplyStrings val
        -- Stream Store
        CmdXReadBlock key sid 0 -> do
            sid' <- liftIO $ atomically (StS.xResolveStreamIdSTM tvStreamMap key sid)
            vals <- liftIO (atomically (StS.xReadBlockSTM tvStreamMap key sid'))
            pure $ (ReplyStreamKeyValues . singleton) vals
        CmdXReadBlock key sid tout -> do
            sid' <- liftIO $ atomically (StS.xResolveStreamIdSTM tvStreamMap key sid)
            vals <- liftIO (timeout' tout (atomically (StS.xReadBlockSTM tvStreamMap key sid')))
            pure $ maybe (ReplyResp RespArrayNull) (ReplyStreamKeyValues . singleton) vals

runServerInfoCmds :: (HasReplication r) => Env r -> Maybe SubInfo -> STM Success
runServerInfoCmds env cmd = do
    case cmd of
        Just IReplication -> do
            info <- replicationInfo $ getReplication env
            pure $ ReplyResp $ RespBulk $ Just info
        _ -> error "not handled"

runConfigInfoCmds :: Env r -> SubConfig -> Success
runConfigInfoCmds env cmd = case env of
    EnvMaster _ ms -> f ms
    EnvReplica _ rs -> f rs
  where
    f :: (HasRdbConfig r) => r -> Success
    f s = case cmd of
        ConfigDir -> ReplyStrings ["dir", fromString $ rdbDir s]
        ConfigDbFilename -> ReplyStrings ["dbfilename", fromString $ rdbFilename s]
