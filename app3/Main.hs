module Main (main) where

import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Monad.Free (Free (..))

import Data.Functor((<&>))
import Data.Time ( UTCTime, getCurrentTime )
import Data.List qualified as L
import Lib1 qualified
import Lib3 qualified
import System.Console.Repline
  ( CompleterStyle (Word),
    ExitDecision (Exit),
    HaskelineT,
    WordCompleter,
    evalRepl,
  )
import System.Console.Terminal.Size (Window, size, width)
import System.Directory (listDirectory)
import System.FilePath (dropExtension, pathSeparator, takeExtension)

type Repl a = HaskelineT IO a

final :: Repl ExitDecision
final = do
  liftIO $ putStrLn "Goodbye!"
  return Exit

ini :: Repl ()
ini = liftIO $ putStrLn "Welcome to select-manipulate database! Press [TAB] for auto completion."

completer :: (Monad m) => WordCompleter m
completer n = do
  let names = [
              "select", "*", "from", "show", "table",
              "tables", "insert", "into", "values",
              "set", "update", "delete"
              ]
  return $ Prelude.filter (L.isPrefixOf n) names

-- Evaluation : handle each line user inputs
cmd :: String -> Repl ()
cmd c = do
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
      df <- runExecuteIO $ Lib3.executeSql c
      return $ Lib1.renderDataFrameAsTable s <$> df

main :: IO ()
main =
  evalRepl (const $ pure ">>> ") cmd [] Nothing Nothing (Word completer) ini final

runExecuteIO :: Lib3.Execution r -> IO r
runExecuteIO (Pure r) = return r
runExecuteIO (Free step) = do
    next <- runStep step
    runExecuteIO next
    where
        runStep :: Lib3.ExecutionAlgebra a -> IO a
        runStep (Lib3.GetTime next) = getCurrentTime >>= return . next
        runStep (Lib3.LoadTable tableName next) = do
            fileContent <- readFile (getTableFilePath tableName)
            let parsedResult = Lib3.parseTable fileContent
            return $ next parsedResult
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
        getTableFilePath tableName = "db/" ++ tableName ++ ".yaml"
