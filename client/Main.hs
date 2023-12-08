module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Data.List qualified as L
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)
import Network.Wreq as NW
import Network.HTTP.Client (HttpException (..), HttpExceptionContent (..))
import Shared (webServerPort, SqlRequest (..), SqlErrorResponse (..), SqlSuccessResponse (..))
import Data.Aeson
import Control.Lens
import qualified Data.ByteString.Lazy as BSL
import Control.Exception as E
import Lib1 qualified

type Repl a = HaskelineT IO a

webServerUrl :: String
webServerUrl = "http://localhost:" ++ show webServerPort

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Merry christmas!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to the sql executor! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = [
              "select", "*", "from", "show", "table",
              "tables", "insert", "into", "values",
              "set", "update", "delete"
              ]
  return $ Prelude.filter (L.isPrefixOf n) names

cmd :: String -> Repl ()
cmd inp = do
  s <- terminalWidth <$> liftIO size
  result <- liftIO $ cmd' s
  case result of
    Left err -> liftIO $ putStrLn $ "Error: " ++ err
    Right table -> liftIO $ putStrLn table
  where
    terminalWidth :: (Integral n) => Maybe (Window n) -> n
    terminalWidth = maybe 80 width
    cmd' :: Integer -> IO (Either String String)
    cmd' s = do
      result <- (Right <$> postWith defaults webServerUrl (toJSON (SqlRequest { query = inp }))) `E.catch` handleHttpException
      case result of
        Left err -> return $ Left err
        Right res -> do
          let body = decode $ res ^. responseBody :: Maybe SqlSuccessResponse
          case body of
            Nothing -> return $ Left "Unexpected body received from web server in successful response."
            Just res' -> return $ Right $ Lib1.renderDataFrameAsTable s $ dataFrame res'
    handleHttpException :: HttpException -> IO (Either String (Response BSL.ByteString))
    handleHttpException (HttpExceptionRequest _ (StatusCodeException _ bs)) = do
      let res = decodeStrict bs :: Maybe SqlErrorResponse
      case res of
        Nothing -> return $ Left "Unexpected body received from web server in error response."
        Just res' -> return $ Left $ errorMessage res'
    handleHttpException (HttpExceptionRequest _ (ConnectionFailure _)) = do
      return $ Left $ "Please ensure that web server (" ++ webServerUrl ++ ") is running."
    handleHttpException _ = 
      return $ Left "HTTP exception occurred."

main :: IO ()
main = do
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final