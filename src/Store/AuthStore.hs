{-# LANGUAGE OverloadedStrings #-}

module Store.AuthStore (AuthStore, emptySTM, getUserSTM, addPasswordSTM) where

import Control.Concurrent.STM (STM, TVar, modifyTVar', newTVar, readTVar)
import Data.Map (Map)
import Data.Map qualified as M

import Auth.Types (Sha256 (..), UserFlags (..), UserProperty (..))
import Protocol.Command (Key)

type AuthStore = Map Key UserProperty

emptySTM :: STM (TVar AuthStore)
emptySTM =
    newTVar $
        M.singleton "default" $
            MkUserProperty
                { flags = (MkUserFlags{nopass = True})
                , passwords = []
                }

getUserSTM :: Key -> TVar AuthStore -> STM (Maybe UserProperty)
getUserSTM userKey tv = M.lookup userKey <$> readTVar tv

addPasswordSTM :: Key -> Sha256 -> TVar AuthStore -> STM ()
addPasswordSTM userKey pwdHash tv = do
    modifyTVar' tv $ \store ->
        M.alter (Just . updateUser) userKey store
  where
    updateUser :: Maybe UserProperty -> UserProperty
    updateUser Nothing =
        MkUserProperty
            { flags = MkUserFlags{nopass = False}
            , passwords = [pwdHash]
            }
    updateUser (Just up) =
        up
            { flags = (flags up){nopass = False}
            , passwords = pwdHash : passwords up
            }
