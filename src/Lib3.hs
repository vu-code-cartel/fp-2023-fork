{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..)
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame (..), ColumnType (IntegerType, StringType, BoolType), Column (..), Value (..), Row)
import Data.Time ( UTCTime )
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Yaml as Y
import GHC.Generics (Generic)
import Data.Char (toLower)
import Lib1 (validateDataFrame)

type TableName = String
type FileContent = String
type ErrorMessage = String

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | SaveFile TableName FileContent (() -> next)
  | GetTime (UTCTime -> next)
  -- feel free to add more constructors here
  deriving Functor

type Execution = Free ExecutionAlgebra

data SerializedColumn = SerializedColumn {
    name :: String,
    dataType :: String
} deriving (Show, Eq, Generic)

data SerializedTable = SerializedTable {
    tableName :: String,
    columns :: [SerializedColumn],
    rows :: [[Y.Value]]
} deriving (Show, Eq, Generic)

instance Y.FromJSON SerializedColumn
instance Y.FromJSON SerializedTable

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

saveFile :: TableName -> FileContent -> Execution ()
saveFile tableName fileContent = liftF $ SaveFile tableName fileContent id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = do
    return $ Left "implement me"

serializeTable :: (TableName, DataFrame) -> Either ErrorMessage FileContent
serializeTable (tableName, dataFrame) = do
    _ <- validateDataFrame dataFrame
    return $ serializeTable' (tableName, dataFrame)

    where
        serializeTable' :: (TableName, DataFrame) -> FileContent
        serializeTable' (tableName', DataFrame cols rows) = 
            serializeTableName tableName' ++ serializeCols cols ++ serializeRows rows

        serializeTableName :: TableName -> FileContent
        serializeTableName tableName' = "tableName: " ++ tableName' ++ "\n"

        serializeCols :: [Column] -> FileContent
        serializeCols [] = "columns: []\n"
        serializeCols cols = foldl (\acc col -> acc ++ serializeCol col) "columns:\n" cols

        serializeCol :: Column -> FileContent
        serializeCol col =
            "- name: " ++ name serializedCol ++ "\n" ++ 
            "  dataType: " ++ dataType serializedCol ++ "\n"
            where
                serializedCol = serializeCol' col

        serializeCol' :: Column -> SerializedColumn
        serializeCol' (Column colName dataType) =
            SerializedColumn {
                name = colName,
                dataType = serializeDataType dataType }

        serializeDataType :: ColumnType -> String
        serializeDataType IntegerType = "integer"
        serializeDataType StringType = "string"
        serializeDataType BoolType = "bool"

        serializeRows :: [Row] -> FileContent
        serializeRows [] = "rows: []\n"
        serializeRows rows = foldl (\acc row-> acc ++ serializeRow row) "rows:\n" rows

        serializeRow :: [Value] -> FileContent
        serializeRow [] = ""
        serializeRow (x:xs) = "- [" ++ serializeRow' xs (valueToString x) ++ "]\n"

        serializeRow' :: [Value] -> FileContent -> FileContent
        serializeRow' [] acc = acc
        serializeRow' (x:xs) acc = serializeRow' xs (acc ++ ", " ++ valueToString x)

        valueToString :: Value -> String
        valueToString (IntegerValue x) = show x
        valueToString (StringValue x) = x
        valueToString (BoolValue x) = show x
        valueToString NullValue = "null"

parseTable :: FileContent -> Either ErrorMessage (TableName, DataFrame)
parseTable content = do
    serializedTable <- fromYaml content
    table <- parsedTable' serializedTable
    _ <- validateDataFrame $ snd table
    return table

    where
        fromYaml :: FileContent -> Either ErrorMessage SerializedTable
        fromYaml yaml =
            case Y.decodeEither' $ TE.encodeUtf8 $ T.pack yaml :: Either Y.ParseException SerializedTable of
                Left err -> Left $ "Failed to parse table (" ++ show err ++ ")"
                Right table -> Right table

        parsedTable' :: SerializedTable -> Either ErrorMessage (TableName, DataFrame)
        parsedTable' table = do
            cols <- parseCols $ columns table
            rows <- parseRows $ rows table
            return (tableName table, DataFrame cols rows)

        parseRows :: [[Y.Value]] -> Either ErrorMessage [Row]
        parseRows rows = parseRows' rows []

        parseRows' :: [[Y.Value]] -> [Row] -> Either ErrorMessage [Row]
        parseRows' [] acc = Right acc
        parseRows' (x:xs) acc =
            case parseRow x of
                Left err -> Left err
                Right row -> parseRows' xs (acc ++ [row])

        parseRow :: [Y.Value] -> Either ErrorMessage [Value]
        parseRow values = parseRow' values []

        parseRow' :: [Y.Value] -> [Value] -> Either ErrorMessage [Value]
        parseRow' [] acc = Right acc
        parseRow' (x:xs) acc =
            case fromAesonValue x of
                Left err -> Left err
                Right value -> parseRow' xs (acc ++ [value])

        fromAesonValue :: Y.Value -> Either ErrorMessage Value
        fromAesonValue value =
            case value of
                Y.Number val -> if val == fromIntegral (truncate val)
                    then
                        Right $ IntegerValue $ fromIntegral (truncate val)
                    else
                        Left "Non-integer numbers are not supported."
                Y.String val -> Right $ StringValue $ T.unpack val
                Y.Bool val -> Right $ BoolValue val
                Y.Null -> Right NullValue
                _ -> Left $ "Data type of value (" ++ show value ++ ") is not supported."

        parseCols :: [SerializedColumn] -> Either ErrorMessage [Column]
        parseCols cols = parseCols' cols []

        parseCols' :: [SerializedColumn] -> [Column] -> Either ErrorMessage [Column]
        parseCols' [] acc = Right acc
        parseCols' (x:xs) acc =
            case parseCol x of
                Left err -> Left err
                Right col -> parseCols' xs (acc ++ [col])

        parseCol :: SerializedColumn -> Either ErrorMessage Column
        parseCol col =
            case parseDataType (dataType col) of
                Left err -> Left err
                Right t -> Right $ Column (name col) t

        parseDataType :: String -> Either ErrorMessage ColumnType
        parseDataType dataType = case map toLower dataType of
            "integer" -> Right IntegerType
            "string" -> Right StringType
            "bool" -> Right BoolType
            _ -> Left $ "Unrecognized data type: " ++ dataType
