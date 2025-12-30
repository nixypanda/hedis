module RespSession where

import Control.Concurrent.STM
import Data.ByteString (ByteString)
import Network.Simple.TCP (Socket, recv, send)

import Control.Exception (Exception, IOException, throwIO)
import Parsers (parseWithRemainder)
import RDB (RDBEncoded, rdbParser)
import Resp

data BufferError = EmptyBuffer

data RespError
    = RespEOF
    | RespParseError String
    | RespIOError IOException
    deriving (Show)

instance Exception RespError

data RespConn = RespConn
    { rcSocket :: Socket
    , rcBuffer :: TVar ByteString
    }

sendResp :: RespConn -> Resp -> IO ()
sendResp RespConn{..} r =
    send rcSocket (encode r)

recvResp :: RespConn -> IO Resp
recvResp conn@RespConn{..} = do
    buf <- readTVarIO rcBuffer
    case parseWithRemainder resp buf of
        Right (respP, rest) -> do
            atomically $ writeTVar rcBuffer rest
            pure respP
        Left _ -> do
            mbs <- recv rcSocket 4096
            case mbs of
                Nothing -> throwIO RespEOF
                Just bs -> do
                    atomically $ modifyTVar' rcBuffer (<> bs)
                    recvResp conn

recvRdb :: RespConn -> IO RDBEncoded
recvRdb conn@RespConn{..} = do
    buf <- readTVarIO rcBuffer
    case parseWithRemainder rdbParser buf of
        Right (rdb, rest) -> do
            atomically $ writeTVar rcBuffer rest
            pure rdb
        Left _ -> do
            mbs <- recv rcSocket 4096
            case mbs of
                Nothing -> throwIO RespEOF
                Just bs -> do
                    atomically $ modifyTVar' rcBuffer (<> bs)
                    recvRdb conn

newtype RespClient = RespClient
    { rcConn :: RespConn
    }

mkRespConn :: Socket -> IO RespConn
mkRespConn sock = do
    buf <- newTVarIO mempty
    pure $ RespConn sock buf

respCall :: RespClient -> Resp -> IO Resp
respCall RespClient{..} req = do
    sendResp rcConn req
    recvResp rcConn
