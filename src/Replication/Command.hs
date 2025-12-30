module Replication.Command (replicateIOCmdAs, replicateSTMCmdAs) where

import Data.String (fromString)

import Command
import CommandResult

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
