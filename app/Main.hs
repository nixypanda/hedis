{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad (forever, replicateM)
import Control.Monad.Except (ExceptT (..), MonadError (throwError), liftEither, runExceptT, withExceptT)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.State (StateT (runStateT), lift)
import Data.Bifunctor (Bifunctor (first))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Map (Map)
import Data.Map qualified as M
import Data.String (fromString)
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, recv, send, serve)
import System.IO (BufferMode (NoBuffering), hPutStrLn, hSetBuffering, stderr, stdout)
import Text.Parsec (ParseError)

import Data.Maybe (fromMaybe)
import Resp

-- Command execution

type MemDB = Map ByteString ByteString
data RedisError
    = ParsingError ParseError
    | EncodeError String
    | EmptyBuffer
    | Unimplemented String
    deriving (Show)

newtype Redis a = MkRedis {runRedis :: StateT MemDB (ExceptT RedisError IO) a}
    deriving (Functor, Applicative, Monad, MonadError RedisError, MonadIO)

runCmd :: Resp -> Redis Resp
runCmd (Array 1 [BulkStr "PING"]) = pure $ Str "PONG"
runCmd (Array 2 [BulkStr "ECHO", BulkStr xs]) = pure $ BulkStr xs
runCmd r = throwError $ Unimplemented $ "Unknown command: " <> show r

handleRequest :: ByteString -> Redis ByteString
handleRequest bs = do
    liftIO $ print ("Received: " <> bs)
    resp <- liftEither $ first ParsingError $ decode bs
    liftIO $ print ("Decoded: " <> show resp)
    result <- runCmd resp
    liftIO $ print ("Execution: " <> show result)
    encoded <- liftEither $ first EncodeError $ encode result
    liftIO $ print ("Encoded: " <> encoded)
    pure encoded

-- network

bufferSize :: Int
bufferSize = 1024

clientLoop :: Socket -> Redis ()
clientLoop socket = do
    mbs <- recv socket bufferSize
    ebs <- maybe (throwError EmptyBuffer) pure mbs
    encoded <- handleRequest ebs
    send socket encoded
    clientLoop socket

main :: IO ()
main = do
    -- Disable output buffering
    hSetBuffering stdout NoBuffering
    hSetBuffering stderr NoBuffering

    -- You can use print statements as follows for debugging, they'll be visible when running tests.
    hPutStrLn stderr "Logs from your program will appear here"

    let port = "6379"
    putStrLn $ "Redis server listening on port " ++ port
    serve HostAny port $ \(socket, address) -> do
        putStrLn $ "successfully connected client: " ++ show address
        errOrRes <- runExceptT $ runStateT (runRedis $ clientLoop socket) M.empty
        case errOrRes of
            Left err -> print err
            Right _ -> pure ()
        print "Closing connection"
        closeSock socket
