module Replication.Command (runAndReplicateIO, runAndReplicateSTM) where

import Control.Concurrent.STM (STM, atomically, modifyTVar, readTVar, writeTQueue)
import Control.Monad.IO.Class (MonadIO (..))
import Data.String (fromString)
import Data.Time (UTCTime)

import Command
import CommandResult
import Execution.Base (runCmdIO, runCmdSTM)
import Redis
import Replication.Config (MasterState (..), ReplicaConn (rcQueue), Replication (..))
import Resp.Command (cmdSTMBytes, cmdToResp)
import Resp.Core (encode)

replicateIOCmdAs :: CmdIO -> CommandResult -> Maybe CmdSTM
replicateIOCmdAs (CmdBLPop k 0) (RArraySimple _) = Just $ STMList $ CmdLPop k (Just 1)
replicateIOCmdAs (CmdBLPop _ 0) _ = error "Incorrect combination"
replicateIOCmdAs (CmdBLPop _ _) RArrayNull = Nothing
replicateIOCmdAs (CmdBLPop k _) (RArraySimple _) = Just $ STMList $ CmdLPop k (Just 1)
replicateIOCmdAs (CmdBLPop{}) _ = error "Incorrect combination"
replicateIOCmdAs (CmdXReadBlock{}) _ = Nothing

replicateSTMCmdAs :: CmdSTM -> CommandResult -> Maybe CmdSTM
replicateSTMCmdAs (STMString c) cr = STMString <$> replicateStringCmd c cr
replicateSTMCmdAs (STMList c) cr = STMList <$> replicateListCmd c cr
replicateSTMCmdAs (STMStream c) cr = STMStream <$> replicateStreamCmd c cr
replicateSTMCmdAs CmdPing _ = Nothing
replicateSTMCmdAs (CmdEcho{}) _ = Nothing
replicateSTMCmdAs (CmdType{}) _ = Nothing

replicateStreamCmd :: StreamCmd -> CommandResult -> Maybe StreamCmd
replicateStreamCmd c@(CmdXAdd{}) _ = Just c
replicateStreamCmd (CmdXRead{}) _ = Nothing
replicateStreamCmd (CmdXRange{}) _ = Nothing

replicateListCmd :: ListCmd -> CommandResult -> Maybe ListCmd
replicateListCmd c@(CmdRPush{}) _ = Just c
replicateListCmd c@(CmdLPush{}) _ = Just c
replicateListCmd c@(CmdLPop{}) _ = Just c
replicateListCmd (CmdLRange{}) _ = Nothing
replicateListCmd (CmdLLen{}) _ = Nothing

replicateStringCmd :: StringCmd -> CommandResult -> Maybe StringCmd
replicateStringCmd c@(CmdSet{}) _ = Just c
replicateStringCmd (CmdGet{}) _ = Nothing
replicateStringCmd (CmdIncr k) (RInt v) = Just $ CmdSet k (fromString $ show v) Nothing
replicateStringCmd (CmdIncr _) (RErr _) = Nothing
replicateStringCmd c@(CmdIncr _) cr = error $ "Incorrect combination: Command - " <> show c <> ", result - " <> show cr

runAndReplicateSTM :: (HasReplication r, HasStores r) => Env r -> UTCTime -> CmdSTM -> STM CommandResult
runAndReplicateSTM env now cmd = do
    res <- runCmdSTM env now cmd
    case replicateSTMCmdAs cmd res of
        Nothing -> pure res
        Just cmd' -> do
            propagateWrite (getReplication env) cmd'
            modifyTVar (getOffset env) (+ cmdSTMBytes cmd')
            pure res

-- The run and adding to queue is not atomic
runAndReplicateIO :: (HasStores r, HasReplication r) => Env r -> CmdIO -> Redis r CommandResult
runAndReplicateIO env cmd = do
    res <- runCmdIO cmd
    case replicateIOCmdAs cmd res of
        Nothing -> pure res
        Just cmd' -> do
            liftIO $ atomically $ do
                propagateWrite (getReplication env) cmd'
                modifyTVar (getOffset env) (+ cmdSTMBytes cmd')
            pure res

propagateWrite :: Replication -> CmdSTM -> STM ()
propagateWrite repli cmd =
    case repli of
        MkReplicationMaster (MkMasterState{..}) -> do
            registry <- readTVar replicaRegistry
            mapM_ (\rc -> writeTQueue rc.rcQueue $ encode $ cmdToResp $ RedSTM cmd) registry
        MkReplicationReplica _ ->
            pure () -- replicas never propagate
