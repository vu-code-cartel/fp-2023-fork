{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}


module Lib3
  ( 
    executeSql,
    Execution,
    ExecutionAlgebra(..),
    ParsedStatementLib3,
    parseStatementLib3,
    serializeTable,
    parseTable
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame (..), ColumnType (IntegerType, StringType, BoolType), Column (..), Value (..), Row)
import Data.Time ( UTCTime )
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Yaml as Y
import GHC.Generics (Generic)
import Data.Char (toLower, isDigit, isSpace)
import Lib1 (validateDataFrame)
import Lib2 (parseEndOfStatement,Parser(..),runParser, parseChar, parseKeyword, parseWhitespace, parseWord)
import Control.Applicative ( many, some, Alternative(empty, (<|>)), optional )


type TableName = String
type FileContent = String
type ErrorMessage = String

data Condition = Condition String RelationalOperator Value
  deriving (Show, Eq)

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | SaveFile TableName FileContent (() -> next)
  | GetTime (UTCTime -> next)
  | InsertData TableName [[Y.Value]] (() -> next)
  -- feel free to add more constructors here
  deriving Functor

data ParsedStatementLib3
  = InsertStatement {
      tableNameInsert :: TableName,
      columnsInsert :: Maybe [String],
      valuesInsert :: [Value]
    }
  | UpdateStatement {
      tableNameUpdate :: TableName,
      updates :: [(String, Value)],
      whereConditions :: Maybe [Condition]
    }
  deriving (Show, Eq)

type Execution = Free ExecutionAlgebra

data RelationalOperator = Equal | LessThan | GreaterThan | LessThanOrEqual | GreaterThanOrEqual | NotEqual
  deriving (Show, Eq)

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

-- call this to get get tableName,[columnNames] [values] from "insert into tablename values('value1', value2,...)" statement, values will be converted by their Y.Values types
-- call this to get all data from update query
parseStatementLib3 :: String -> Either ErrorMessage (TableName, Maybe [String], [Value], Maybe [Condition])
parseStatementLib3 inp = case runParser parser (dropWhile isSpace inp) of
    Left err1 -> Left err1
    Right (rest, statement) -> case statement of
        InsertStatement { tableNameInsert = tableName, columnsInsert = columns, valuesInsert = values} -> 
          Right (tableName, columns, values, Nothing)
        UpdateStatement { tableNameUpdate = tableName, updates = updatesList, whereConditions = conditions} ->
          let (columns, values) = unzip updatesList in
          Right (tableName, Just columns, values, conditions)
  where
    parser :: Parser ParsedStatementLib3
    parser = parseInsertStatement <* parseEndOfStatement <|> parseUpdateStatement <* parseEndOfStatement

parseInsertStatement :: Parser ParsedStatementLib3
parseInsertStatement = do
  _ <- parseKeyword "insert"
  _ <- parseWhitespace
  _ <- parseKeyword "into"
  _ <- parseWhitespace
  tableName <- parseWord
  parseOptionalWhitespace
  columns <- optional parseColumnNames
  _ <- parseKeyword "values"
  parseOptionalWhitespace
  values <- parseValues
  case columns of
    Nothing -> return $ InsertStatement tableName columns values
    Just cols ->
      if length cols == length values
        then return $ InsertStatement tableName columns values
        else Parser $ \_ -> Left "Column count does not match the number of values provided"

parseColumnNames :: Parser [String]
parseColumnNames = do
  _ <- parseChar '('
  names <- parseWord `sepBy` (parseChar ',' *> parseOptionalWhitespace)
  _ <- parseChar ')'
  _ <- parseWhitespace
  return names
 
parseValues :: Parser [Value]
parseValues = do
  _ <- parseChar '('
  values <- parseValue `sepBy` (parseChar ',' *> parseOptionalWhitespace)
  _ <- parseChar ')'
  return values

parseValue :: Parser Value
parseValue = do
  parseOptionalWhitespace
  val <- parseNumericValue <|> parseBoolValue <|> parseNullValue <|> parseStringValue
  parseOptionalWhitespace
  return val

parseOptionalWhitespace :: Parser ()
parseOptionalWhitespace = many (parseWhitespace *> pure ()) *> pure ()

parseNumericValue :: Parser Value
parseNumericValue = (IntegerValue <$> parseInt)

parseInt :: Parser Integer
parseInt = do
  digits <- some (parseSatisfy isDigit)
  return (read digits)

parseNullValue :: Parser Value
parseNullValue = parseKeyword "null" >> return NullValue

parseBoolValue :: Parser Value
parseBoolValue = (parseKeyword "true" >> return (BoolValue True)) <|>
                 (parseKeyword "false" >> return (BoolValue False))

parseStringValue :: Parser Value
parseStringValue = do
  strValue <- parseStringWithQuotes 
  return (StringValue strValue)

parseStringWithQuotes :: Parser String
parseStringWithQuotes = do
  _ <- parseChar '\''
  str <- some (parseSatisfy (/= '\''))
  _ <- parseChar '\''
  return str

sepBy :: Parser a -> Parser b -> Parser [a]
sepBy p sep = do
  x <- p
  xs <- many (sep *> p)
  return (x:xs)

parseSatisfy :: (Char -> Bool) -> Parser Char
parseSatisfy predicate = Parser $ \inp ->
    case inp of
        [] -> Left "Empty input"
        (x:xs) -> if predicate x then Right (xs, x) else Left ("Unexpected character: " ++ [x])

parseUpdateStatement :: Parser ParsedStatementLib3
parseUpdateStatement = do
  _ <- parseKeyword "update"
  _ <- parseWhitespace
  tableName <- parseWord
  _ <- parseWhitespace
  _ <- parseKeyword "set"
  _ <- parseWhitespace
  updatesList <- parseUpdates
  hasWhere <- optional $ parseWhereClauseFlag
  whereConditions <- if hasWhere == Just True
                       then Just <$> parseWhereClause
                       else pure Nothing
  case whereConditions of
    Nothing -> return $ UpdateStatement tableName updatesList Nothing 
    Just conditions ->
      if null conditions
        then Parser $ \_ -> Left "At least one condition is required in the where clause"
        else return $ UpdateStatement tableName updatesList (Just conditions) 

parseWhereClauseFlag :: Parser Bool
parseWhereClauseFlag = do
  parseOptionalWhitespace
  flag <- choice [parseKeyword "where" >> pure True, pure False]
  case flag of
    Just b -> return b
    Nothing -> return False

parseCountWhitespace :: Parser Int
parseCountWhitespace = length <$> many parseWhitespace

parseUpdates :: Parser [(String, Value)]
parseUpdates = do
  updatesList <- parseUpdate `sepBy` (parseChar ',' *> parseOptionalWhitespace)
  return updatesList

parseUpdate :: Parser (String, Value)
parseUpdate = do
  columnName <- parseWord
  parseOptionalWhitespace
  _ <- parseChar '='
  parseOptionalWhitespace
  value <- parseValueAndQuoteFlag
  return (columnName, value)

parseValueAndQuoteFlag :: Parser (Value)
parseValueAndQuoteFlag = do
  parseOptionalWhitespace
  val <- parseNumericValue <|> parseBoolValue <|> parseNullValue <|> parseStringValue
  parseOptionalWhitespace
  return val

parseWhereClause :: Parser [Condition]
parseWhereClause = do
  _ <- parseWhitespace
  conditions <- parseConditions
  return conditions

parseConditions :: Parser [Condition]
parseConditions = parseCondition `sepBy` (parseKeyword "and" *> parseOptionalWhitespace)

parseCondition :: Parser Condition
parseCondition = do
  columnName <- parseWord
  parseOptionalWhitespace
  op <- parseRelationalOperator
  parseOptionalWhitespace
  value <- parseValue
  return $ Condition columnName op value

parseRelationalOperator :: Parser RelationalOperator
parseRelationalOperator =
      (parseKeyword "=" >> pure Equal)
  <|> (parseKeyword "!=" >> pure NotEqual)
  <|> (parseKeyword "<=" >> pure LessThanOrEqual)
  <|> (parseKeyword ">=" >> pure GreaterThanOrEqual)
  <|> (parseKeyword "<" >> pure LessThan)
  <|> (parseKeyword ">" >> pure GreaterThan)

choice :: [Parser a] -> Parser (Maybe a)
choice [] = pure Nothing
choice (p:ps) = (Just <$> p) <|> choice ps

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