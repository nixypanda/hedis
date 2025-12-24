{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, recv, send, serve)
import System.IO (BufferMode (NoBuffering), hPutStrLn, hSetBuffering, stderr, stdout)

bufferSize :: Int
bufferSize = 1024

clientLoop :: Socket -> IO ()
clientLoop socket = do
    redisCmdStr <- recv socket bufferSize
    print redisCmdStr
    case redisCmdStr of
        Nothing -> pure ()
        Just redisCmdStr' -> do
            send socket "+PONG\r\n"
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
