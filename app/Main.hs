{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad (forever, replicateM)
import Control.Monad.Except (ExceptT (..), runExceptT, withExceptT)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (fromString)
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, recv, send, serve)
import System.IO (BufferMode (NoBuffering), hPutStrLn, hSetBuffering, stderr, stdout)

import Resp

-- Command execution

runCmd :: Resp -> Resp
runCmd (Array 1 [BulkStr "PING"]) = Str "PONG"
runCmd (Array 2 [BulkStr "ECHO", BulkStr xs]) = BulkStr xs

data RespError
    = ParseError String
    | EncodeError String
    deriving (Show)

handleRequest :: Socket -> ByteString -> ExceptT RespError IO ()
handleRequest socket bs = do
    resp <- withExceptT (ParseError . show) $ ExceptT (pure (decode bs))
    liftIO $ print ("RESP: Parsing Success " <> show resp)
    let result = runCmd resp
    encoded <- withExceptT EncodeError $ ExceptT (pure (encode result))

    liftIO $ do
        print ("RESP: Encoding Success " <> encoded)
        send socket encoded

-- network

bufferSize :: Int
bufferSize = 1024

clientLoop :: Socket -> IO ()
clientLoop socket = do
    mbs <- recv socket bufferSize
    case mbs of
        Nothing -> print ("Empty buffer" :: String)
        Just bs -> do
            runExceptT (handleRequest socket bs) >>= either print pure
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
        clientLoop socket
        print "Closing connection"
        closeSock socket
