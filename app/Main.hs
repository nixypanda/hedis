{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad (replicateM)
import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.String (fromString)
import Network.Simple.TCP (HostPreference (HostAny), Socket, closeSock, recv, send, serve)
import System.IO (BufferMode (NoBuffering), hPutStrLn, hSetBuffering, stderr, stdout)
import Text.Parsec (
    ParseError,
    anyChar,
    char,
    count,
    crlf,
    digit,
    many1,
    manyTill,
    option,
    parse,
    (<|>),
 )
import Text.Parsec.ByteString (Parser)
import Text.Parsec.Char (crlf)

-- RESP

data Resp = Str ByteString | Array Int [Resp] | BulkStr ByteString
    deriving (Show)

-- parsing

bytes :: Parser ByteString
bytes = fromString <$> manyTill anyChar (char '\r')

resp :: Parser Resp
resp = do
    t <- anyChar
    case t of
        '+' -> Str <$> bytes <* crlf
        '*' -> array
        '$' -> bulkString
        _ -> fail $ "invalid type tag: " ++ show t

signedIntParser :: Parser Int
signedIntParser = do
    sign <- option '+' (char '+' <|> char '-')
    digits <- many1 digit
    let intValue = read digits :: Int
    return $ if sign == '-' then -intValue else intValue

array :: Parser Resp
array = do
    n <- signedIntParser <* crlf
    if n >= 0
        then Array (fromIntegral n) <$> replicateM n resp
        else fail "negative array length"

bulkString :: Parser Resp
bulkString = do
    n <- signedIntParser <* crlf
    if n >= 0
        then BulkStr . fromString <$> count n anyChar <* crlf
        else fail "negative bulk length"

decode :: ByteString -> Either ParseError Resp
decode = parse resp ""

crlf' :: ByteString
crlf' = "\r\n"

-- encoding

encode :: Resp -> Either ByteString ByteString
encode (Str x) = Right $ "+" <> x <> crlf'
encode (BulkStr x) = Right $ "$" <> fromString (show $ BS.length x) <> crlf' <> x <> crlf'
encode r = Left $ "Don't know how to encode this" <> fromString (show r)

-- Command execution

runCmd :: Resp -> Resp
runCmd (Array 1 [BulkStr "PING"]) = Str "PONG"
runCmd (Array 2 [BulkStr "ECHO", BulkStr xs]) = BulkStr xs

-- network

bufferSize :: Int
bufferSize = 1024

clientLoop :: Socket -> IO ()
clientLoop socket = do
    redisCmdStr <- recv socket bufferSize
    case redisCmdStr of
        Nothing -> do
            print "Empty buffer"
            pure ()
        Just redisCmdStr' -> do
            print $ "String received: " <> redisCmdStr'
            case decode redisCmdStr' of
                Left err -> do
                    print $ "RESP: Parsing error " <> show err
                Right resp -> do
                    print $ "RESP: Parsing Success" <> show resp
                    let result = runCmd resp
                    case encode result of
                        Left err -> do
                            print $ "RESP: Encoding error" <> err
                        Right respStr -> do
                            print $ "RESP: Encoding Success" <> respStr
                            send socket respStr
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
