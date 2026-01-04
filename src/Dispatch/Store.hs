{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

module Dispatch.Store (runCmdSTM, runCmdIO, runServerInfoCmds, runConfigInfoCmds) where

import Control.Concurrent.STM
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Reader (asks)
import Data.Bifunctor (bimap)
import Data.List (singleton)
import Data.String (IsString (..))
import Data.Time (UTCTime)

import Store.Backend.ListMap (Range (..))
import Store.Backend.TypeIndex (ValueType (..))
import Store.List qualified as LS
import Store.SortedSet qualified as SSS
import Store.Stream qualified as StS
import Store.String qualified as SS
import Store.Type qualified as TS
import Time (timeout')
import Types.Command
import Types.Redis
import Types.Replication
import Types.Result

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
        STMString c -> runStringStoreSTM tvTypeIndex tvStringMap now c
        STMList c -> Right <$> runListStoreSTM tvTypeIndex tvListMap c
        STMStream c -> runStreamStoreSTM tvTypeIndex tvStreamMap now c
        STMSortedSet c -> Right <$> runSortedSetStoreSTM tvTypeIndex tvSortedSetMap c
        STMGeo c -> runGeoStoreSTM tvTypeIndex tvSortedSetMap c

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

runListStoreSTM :: TVar TS.TypeIndex -> TVar LS.ListStore -> ListCmd -> STM Success
runListStoreSTM tvTypeIndex tvListMap cmd = case cmd of
    CmdRPush key xs -> do
        count <- TS.setIfAvailable tvTypeIndex key VList *> LS.rpushSTM tvListMap key xs
        pure $ ReplyResp $ RespInt count
    CmdLPush key xs -> do
        count <- TS.setIfAvailable tvTypeIndex key VList *> LS.lpushSTM tvListMap key xs
        pure $ ReplyResp $ RespInt count
    CmdLPop key Nothing -> do
        val <- LS.lpopSTM tvListMap key
        pure $ ReplyResp $ RespBulk val
    CmdLPop key (Just mLen) -> do
        vals <- LS.lpopsSTM tvListMap key mLen
        pure $ ReplyStrings vals
    CmdLRange key range -> do
        vals <- LS.lrangeSTM tvListMap key range
        pure $ ReplyStrings vals
    CmdLLen key -> do
        len <- LS.llenSTM tvListMap key
        pure $ ReplyResp $ RespInt len

runSortedSetStoreSTM :: TVar TS.TypeIndex -> TVar SSS.SortedSetStore -> SortedSetCmd -> STM Success
runSortedSetStoreSTM tvTypeIndex tvZSet cmd =
    case cmd of
        CmdZAdd key sc val ->
            ReplyResp . RespInt
                <$> (TS.setIfAvailable tvTypeIndex key VSortedSet *> SSS.zaddSTM key sc val tvZSet)
        CmdZRank key val -> ReplyInt <$> SSS.zrankSTM key val tvZSet
        CmdZRange key (MkRange start end) -> ReplyStrings <$> SSS.zrangeSTM key start end tvZSet
        CmdZCard key -> ReplyResp . RespInt <$> SSS.zcardSTM key tvZSet
        CmdZScore key val -> do
            mSc <- SSS.zscoreSTM key val tvZSet
            pure $ ReplyResp $ RespBulk (SSS.showPretty <$> mSc)
        CmdZRem key val -> ReplyResp . RespInt . maybe 0 (const 1) <$> SSS.zremSTM key val tvZSet

runGeoStoreSTM ::
    TVar TS.TypeIndex -> TVar SSS.SortedSetStore -> GeoCmd -> STM (Either Failure Success)
runGeoStoreSTM tvTypeIndex tvZSet cmd =
    case cmd of
        CmdGeoAdd key coords val ->
            bimap ErrInvalidCoords (ReplyResp . RespInt) <$> SSS.geoAddSTM tvTypeIndex tvZSet key coords val
        CmdGeoPos key vals -> do
            res <- mapM (SSS.geoPosSTM tvZSet key) vals
            pure $ Right $ ReplyCoordinates res
        CmdGeoDist key1 val1 val2 ->
            Right . ReplyDouble <$> SSS.getDistSTM tvZSet key1 val1 val2
        CmdGeoSearchByLonLatByRadius key origin radius ->
            Right . ReplyStrings <$> SSS.searchFromLonLatByRadiusSTM tvZSet key origin radius

runStreamStoreSTM :: TVar TS.TypeIndex -> TVar StS.StreamStore -> UTCTime -> StreamCmd -> STM (Either Failure Success)
runStreamStoreSTM tvTypeIndex tvStreamMap now cmd = case cmd of
    CmdXAdd key sId ks -> do
        val <- TS.setIfAvailable tvTypeIndex key VStream *> StS.xAddSTM tvStreamMap key sId ks now
        pure $ bimap ErrStream ReplyStreamId val
    CmdXRange key range -> do
        vals <- StS.xRangeSTM tvStreamMap key range
        pure $ Right $ ReplyStreamValues vals
    CmdXRead keys -> do
        vals <- StS.xReadSTM tvStreamMap keys
        pure $ Right $ ReplyStreamKeyValues vals

runStringStoreSTM ::
    TVar TS.TypeIndex -> TVar SS.StringStore -> UTCTime -> StringCmd -> STM (Either Failure Success)
runStringStoreSTM tvTypeIndex tvStringMap now cmd = case cmd of
    CmdSet key val mexpiry -> do
        TS.setIfAvailable tvTypeIndex key VString *> SS.setSTM tvStringMap key val now mexpiry
        pure $ Right ReplyOk
    CmdGet key -> do
        val <- SS.getSTM tvStringMap key now
        pure $ Right $ ReplyResp $ RespBulk val
    CmdIncr key -> do
        val <- SS.incrSTM tvStringMap key now
        pure $ bimap (const ErrIncr) (ReplyResp . RespInt) val
