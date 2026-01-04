module Gen.Command (genCommand) where

import Hedgehog (Gen)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range

import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS
import Data.Time (NominalDiffTime)

import Data.String (fromString)
import Gen.Stream (genConcreteStreamId, genXAddStreamId, genXRange, genXReadStreamId)
import Store.Backend.ListMap qualified as LM
import Types.Command

--------------------------------------------------------------------------------
-- Generators
--------------------------------------------------------------------------------

genKey :: Gen ByteString
genKey =
    BS.pack <$> Gen.list (Range.linear 1 16) (Gen.element ['a' .. 'z'])

genBS :: Gen ByteString
genBS =
    fromString <$> Gen.list (Range.linear 0 32) Gen.alphaNum

genExpiry :: Gen (Maybe NominalDiffTime)
genExpiry =
    Gen.maybe (fromIntegral <$> Gen.int (Range.linear 1 10_000))

--------------------------------------------------------------------------------
-- String commands
--------------------------------------------------------------------------------

genStringCmd :: Gen StringCmd
genStringCmd =
    Gen.choice
        [ CmdSet <$> genKey <*> genBS <*> genExpiry
        , CmdGet <$> genKey
        , CmdIncr <$> genKey
        ]

--------------------------------------------------------------------------------
-- List commands
--------------------------------------------------------------------------------

genRange :: Gen LM.Range
genRange =
    LM.MkRange
        <$> Gen.int (Range.linear (-10) 10)
        <*> Gen.int (Range.linear (-10) 10)

genListCmd :: Gen ListCmd
genListCmd =
    Gen.choice
        [ CmdRPush <$> genKey <*> Gen.list (Range.linear 0 8) genBS
        , CmdLPush <$> genKey <*> Gen.list (Range.linear 0 8) genBS
        , CmdLPop <$> genKey <*> Gen.maybe (Gen.int (Range.linear 1 8))
        , CmdLRange <$> genKey <*> genRange
        , CmdLLen <$> genKey
        ]

--------------------------------------------------------------------------------
-- Stream commands
--------------------------------------------------------------------------------

genStreamCmd :: Gen StreamCmd
genStreamCmd =
    Gen.choice
        [ CmdXAdd
            <$> genKey
            <*> genXAddStreamId
            <*> Gen.list (Range.linear 0 8) ((,) <$> genBS <*> genBS)
        , CmdXRange <$> genKey <*> genXRange
        , CmdXRead
            <$> Gen.list
                (Range.linear 1 4)
                ((,) <$> genKey <*> genConcreteStreamId)
        ]

--------------------------------------------------------------------------------
-- STM commands
--------------------------------------------------------------------------------

genCmdSTM :: Gen CmdSTM
genCmdSTM =
    Gen.choice
        [ pure CmdPing
        , CmdEcho <$> genBS
        , CmdType <$> genKey
        , STMString <$> genStringCmd
        , STMList <$> genListCmd
        , STMStream <$> genStreamCmd
        ]

--------------------------------------------------------------------------------
-- IO commands
--------------------------------------------------------------------------------

genCmdIO :: Gen CmdIO
genCmdIO =
    Gen.choice
        [ CmdBLPop <$> genKey <*> genTimeout
        , CmdXReadBlock <$> genKey <*> genXReadStreamId <*> genTimeout
        ]

genTimeout :: Gen NominalDiffTime
genTimeout =
    fromIntegral <$> Gen.int (Range.linear 0 10_000)

--------------------------------------------------------------------------------
-- Transactions
--------------------------------------------------------------------------------

genTransaction :: Gen CmdTransaction
genTransaction =
    Gen.element [CmdMulti, CmdExec, CmdDiscard]

--------------------------------------------------------------------------------
-- Command
--------------------------------------------------------------------------------

genCommand :: Gen Command
genCommand =
    Gen.choice
        [ RedSTM <$> genCmdSTM
        , RedIO <$> genCmdIO
        , RedTrans <$> genTransaction
        , RedInfo <$> Gen.maybe (Gen.choice [pure IReplication])
        , CmdWait
            <$> Gen.int (Range.linear 0 10)
            <*> genTimeout
        ]
