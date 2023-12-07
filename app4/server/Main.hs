{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Web.Spock
import Web.Spock.Config
import Data.Text.Encoding (decodeUtf8)
import Shared (webServerPort, SqlRequest (..), SqlErrorResponse (..), SqlSuccessResponse (..))
import Network.HTTP.Types.Status
import DataFrame (DataFrame (..))
import InMemoryTables (tableEmployees)
import Data.Time (UTCTime, getCurrentTime)
import Lib3 qualified
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))
import System.Directory (listDirectory, doesFileExist)
import System.FilePath (dropExtension, pathSeparator, takeExtension)

type ApiAction a = SpockAction () () () a

main :: IO ()
main = do
  cfg <- defaultSpockCfg () PCNoDatabase ()
  runSpock webServerPort (spock cfg app)

app :: SpockM () () () ()
app = do
  post root $ do
    mreq <- jsonBody :: ApiAction (Maybe SqlRequest)
    case mreq of
      Nothing -> do
        setStatus badRequest400
        json SqlErrorResponse { errorMessage = "Request body format is incorrect." }
      Just req -> do
        result <- liftIO $ runExecuteIO $ Lib3.executeSql $ query req
        case result of
          Left err -> do
            setStatus badRequest400
            json SqlErrorResponse { errorMessage = err }
          Right df -> do
            json SqlSuccessResponse { dataFrame = df }

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
  next <- runStep step
  runExecuteIO next
  where
    runStep :: Lib3.ExecutionAlgebra a -> IO a
    runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
    runStep (Lib3.LoadTable tableName next) = do
      let filePath = getTableFilePath tableName
      fileExists <- doesFileExist filePath
      if fileExists then do
        fileContent <- readFile filePath
        return $ next $ Lib3.parseTable fileContent
      else
        return $ next $ Left $ "Table '" ++ tableName ++ "' does not exist."
    runStep (Lib3.SaveTable table next) = do
      case Lib3.serializeTable table of
        Left err -> error err
        Right serializedTable -> writeFile (getTableFilePath $ fst table) serializedTable >>= return . next
    runStep (Lib3.GetTableNames next) = do
      files <- listDirectory dbDirectory
      let tableNames = foldl (\acc fileName -> if takeExtension fileName == dbFormat then dropExtension fileName : acc else acc) [] files
      return $ next tableNames

    dbDirectory :: String
    dbDirectory = "db"
    dbFormat :: String
    dbFormat = ".yaml"
    getTableFilePath :: String -> String
    getTableFilePath tableName = dbDirectory ++ [pathSeparator] ++ tableName ++ dbFormat
  