{-# LANGUAGE OverloadedStrings #-}
module Main (main) where

import Web.Spock
import Web.Spock.Config
import Data.Text.Encoding (decodeUtf8)
import Shared (webServerPort, SqlRequest (..), SqlErrorResponse (..), SqlSuccessResponse (..))
import Network.HTTP.Types.Status
import DataFrame (DataFrame (..))
import InMemoryTables (TableName)
import Data.Time (UTCTime, getCurrentTime)
import Lib2 qualified
import Lib3 qualified
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))
import System.Directory (listDirectory, doesFileExist)
import System.FilePath (dropExtension, pathSeparator, takeExtension)
import Control.Concurrent.STM.TVar (TVar, newTVarIO, readTVarIO, writeTVar)
import Control.Concurrent.STM (readTVar, atomically)
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (Concurrently (..), runConcurrently)

type ConcurrentTable = TVar (TableName, DataFrame)
type ConcurrentDb = TVar [ConcurrentTable]
data AppState = AppState { db :: ConcurrentDb }
type ApiAction a = SpockAction () () AppState a

dbDirectory :: String
dbDirectory = "db"

dbFormat :: String
dbFormat = ".yaml"

getDbTables :: IO [String]
getDbTables = do
  filesInDir <- listDirectory dbDirectory
  return $ foldl (\acc fileName -> if takeExtension fileName == dbFormat then dropExtension fileName : acc else acc) [] filesInDir

getTableFilePath :: String -> String
getTableFilePath tableName = dbDirectory ++ [pathSeparator] ++ tableName ++ dbFormat

loadDb :: IO Lib2.Database
loadDb = do
  tables <- getDbTables
  loadDb' tables []
  where
    loadDb' :: [String] -> Lib2.Database -> IO Lib2.Database
    loadDb' [] acc = return acc
    loadDb' (x:xs) acc = do
      fileContent <- readFile $ getTableFilePath x
      case Lib3.parseTable fileContent of
        Left err -> do
          print $ "Failed to load table '" ++ x ++ "'. Error: " ++ err ++ ". Continuing..."
          loadDb' xs acc
        Right table -> loadDb' xs (table : acc)

syncDb :: ConcurrentDb -> IO ()
syncDb db = do
  threadDelay $ 1 * 1000 * 1000 -- 1 second
  tables <- readTVarIO db
  syncDb' tables
  where
    syncDb' :: [ConcurrentTable] -> IO ()
    syncDb' [] = syncDb db
    syncDb' (x:xs) = do
      table <- readTVarIO x
      case Lib3.serializeTable table of
        Left err -> print $ "Failed to serialize table for saving '" ++ fst table ++ ". Error: " ++ err ++ ". Continuing..."
        Right serializedTable -> do
          writeFile (getTableFilePath $ fst table) serializedTable
          syncDb' xs

main :: IO ()
main = do
  db <- newTVarIO =<< mapM newTVarIO =<< liftIO loadDb
  cfg <- defaultSpockCfg () PCNoDatabase (AppState { db = db })
  _
    <- runConcurrently $ (,,)
    <$> Concurrently (syncDb db)
    <*> Concurrently (runSpock webServerPort (spock cfg app))
  return ()

app :: SpockM () () AppState ()
app = do
  post root $ do
    appState <- getState
    mreq <- jsonBody :: ApiAction (Maybe SqlRequest)
    case mreq of
      Nothing -> do
        setStatus badRequest400
        json SqlErrorResponse { errorMessage = "Request body format is incorrect." }
      Just req -> do
        result <- liftIO $ runExecuteIO (db appState) $ Lib3.executeSql $ query req
        case result of
          Left err -> do
            setStatus badRequest400
            json SqlErrorResponse { errorMessage = err }
          Right df -> do
            json SqlSuccessResponse { dataFrame = df }

runExecuteIO :: ConcurrentDb -> Lib3.Execution r -> IO r
runExecuteIO dbRef (Pure r) = return r
runExecuteIO dbRef (Free step) = do
  next <- runStep step
  runExecuteIO dbRef next
  where
    runStep :: Lib3.ExecutionAlgebra a -> IO a
    runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
    runStep (Lib3.LoadTable tableName next) = do
      t <- findTable tableName
      case t of
        Nothing -> return $ next $ Left $ "Table '" ++ tableName ++ "' does not exist."
        Just ref -> readTVarIO ref >>= (return . next . Right)
    runStep (Lib3.SaveTable table next) = do
      inMemoryTable <- findTable $ fst table
      case inMemoryTable of
        Nothing -> do
          tableRefs <- readTVarIO dbRef
          newTableRef <- newTVarIO table
          atomically $ writeTVar dbRef $ newTableRef : tableRefs
          return $ next ()
        Just tableRef -> atomically $ writeTVar tableRef table >>= return . next
    runStep (Lib3.GetTableNames next) = getTableNames >>= return . next

    findTable :: String -> IO (Maybe ConcurrentTable)
    findTable tableName = do
      db <- readTVarIO dbRef
      findTable' db
      where
        findTable' :: [ConcurrentTable] -> IO (Maybe ConcurrentTable)
        findTable' [] = return Nothing
        findTable' (x:xs) = do
          table <- readTVarIO x
          if fst table == tableName
            then return $ Just x
            else findTable' xs
    
    getTableNames :: IO [TableName]
    getTableNames = do
      db <- readTVarIO dbRef
      getTableNames' db []
      where
        getTableNames' :: [ConcurrentTable] -> [TableName] -> IO [TableName]
        getTableNames' [] acc = return acc
        getTableNames' (x:xs) acc = do
          table <- readTVarIO x
          getTableNames' xs (fst table : acc)