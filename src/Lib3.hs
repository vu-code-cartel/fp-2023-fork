{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Lib3
  ( executeSql,
    ParsedStatement(..),
    Execution,
    ExecutionAlgebra(..),
    SelectData (..),
    Aggregate (..),
    AggregateFunction (..),
    Expression (..),
    WhereClause (..),
    WhereCriterion (..),
    SystemFunction (..),
    parseStatement,
    parseTable,
    serializeTable,
    Condition(..)
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame (..), ColumnType (IntegerType, StringType, BoolType, DateTimeType), Column (..), Value (..), Row)
import Data.Time ( UTCTime )
import Control.Applicative ( many, some, Alternative((<|>)), optional )
import Data.List (find, findIndex, elemIndex)
import Data.Maybe (mapMaybe, catMaybes, listToMaybe, isJust, fromMaybe)
import Data.Time.Format (formatTime, defaultTimeLocale, parseTimeM)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Yaml as Y
import GHC.Generics (Generic)
import Data.Char (toLower, isSpace, isDigit)
import Lib1 (validateDataFrame)
import Lib2(
    Parser(..),
    RelationalOperator(..),
    Aggregate(..),
    AggregateFunction(..),
    LogicalOperator(..),
    parseChar,
    parseWhitespace,
    parseKeyword,
    parseWord,
    parseValue,
    sepBy,
    parseEndOfStatement,
    parseRelationalOperator,
    )

type TableName = String
type FileContent = String

data Condition = Condition String RelationalOperator Value
  deriving (Show, Eq)

data ExecutionAlgebra next
  = LoadTable TableName (Either ErrorMessage (TableName, DataFrame) -> next)
  | SaveTable (TableName, DataFrame) (() -> next)
  | GetTime (UTCTime -> next)
  | GetTableNames ([TableName] -> next)
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

type ErrorMessage = String
type ColumnName = String

data Expression
    = ValueExpression Value
    | ColumnExpression ColumnName (Maybe TableName)
    deriving (Show, Eq)

data WhereCriterion = WhereCriterion Expression RelationalOperator Expression
    deriving (Show, Eq)

data SystemFunction
    = Now
    deriving (Show, Eq)

data SelectData
    = SelectColumn ColumnName (Maybe TableName)
    | SelectAggregate Aggregate (Maybe TableName)
    | SelectSystemFunction SystemFunction
    deriving (Show, Eq)

type SelectQuery = [SelectData]
type WhereClause = [(WhereCriterion, Maybe LogicalOperator)]

data ParsedStatement = SelectStatement {
    tables :: [TableName],
    query :: SelectQuery,
    whereClause :: Maybe WhereClause
} | SelectAllStatement {
    tables :: [TableName],
    whereClause :: Maybe WhereClause
} | SystemFunctionStatement {
    function :: SystemFunction
} | ShowTableStatement {
    table :: TableName
} | ShowTablesStatement { 
} | InsertStatement {
      tableNameInsert :: TableName,
      columnsInsert :: Maybe [String],
      valuesInsert :: [Value]
} | UpdateStatement {
      tableNameUpdate :: TableName,
      updates :: [(String, Value)],
      whereConditions :: Maybe [Condition]
} deriving (Show, Eq)

dateTimeFormat :: String
dateTimeFormat = "%Y-%m-%d %H:%M:%S"

loadTable :: TableName -> Execution (Either ErrorMessage (TableName, DataFrame))
loadTable tableName = liftF $ LoadTable tableName id

saveTable :: (TableName, DataFrame) -> Execution ()
saveTable table = liftF $ SaveTable table id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

getTableNames :: Execution [TableName]
getTableNames = liftF $ GetTableNames id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case parseStatement sql of
    Left errorMsg -> return $ Left errorMsg
    Right (SelectStatement tableNames query maybeWhereClause) -> do
        loadResult <- loadTables tableNames
        case loadResult of
            Left errorMsg -> return $ Left errorMsg
            Right tableData -> do
                case updateUnspecifiedTableNames tableData tableNames query of
                    Left errMsg -> return $ Left errMsg
                    Right updatedQuery -> do
                        case validateSelectDataTablesAndColumns tableData updatedQuery of
                            Left validationError -> return $ Left validationError
                            Right _ ->
                                case updateUnspecifiedTableNamesInWhereClause tableData maybeWhereClause of
                                    Left errMsg -> return $ Left errMsg
                                    Right updatedMaybeWhereClause -> 
                                        case validateWhereClauseTablesAndColumns tableData updatedMaybeWhereClause of
                                            Left errMsg -> return $ Left errMsg
                                            Right _ -> do
                                                let productDataFrame = cartesianProduct tableData
                                                let filteredDataFrame = filterDataFrameByWhereClause productDataFrame updatedMaybeWhereClause
                                                
                                                if any isAggregate updatedQuery then do
                                                    aggregateDataFrame <- calculateAggregatesFromDataFrame filteredDataFrame updatedQuery
                                                    return $ aggregateDataFrame
                                                else do
                                                    selectedDataFrame <- selectColumnsFromDataFrame filteredDataFrame updatedQuery
                                                    return $ Right selectedDataFrame
    Right (SelectAllStatement tableNames maybeWhereClause) -> do
        loadResult <- loadTables tableNames
        case loadResult of
            Left errorMsg -> return $ Left errorMsg
            Right tableData -> do 
                case updateUnspecifiedTableNamesInWhereClause tableData maybeWhereClause of
                    Left errMsg -> return $ Left errMsg
                    Right updatedMaybeWhereClause -> do
                        let productDataFrame = cartesianProduct tableData
                        let filteredDataFrame = filterDataFrameByWhereClause productDataFrame updatedMaybeWhereClause
                        return $ Right filteredDataFrame
    Right (ShowTableStatement table) -> do
        loadResult <- loadTable table
        case loadResult of 
            Left errorMsg -> return $ Left errorMsg
            Right (_, dataframe) -> return $ Right dataframe
    Right ShowTablesStatement -> do
        showTablesFrame <- createShowTablesFrame
        return $ Right showTablesFrame
    Right (SystemFunctionStatement function) -> do
        case function of
            Now -> do
                nowDataFrame <- createNowDataFrame
                return $ Right nowDataFrame
    Right (InsertStatement tableName maybeColumns values) -> do
        loadResult <- loadTable tableName
        case loadResult of
            Left errorMsg -> return $ Left errorMsg
            Right tableData -> do
                if isJust maybeColumns
                    then insertWithColumns tableData maybeColumns values
                    else insertWithoutColumns tableData values

insertWithColumns :: (TableName, DataFrame) -> Maybe [ColumnName] -> [Value] -> Execution (Either ErrorMessage DataFrame)
insertWithColumns (tableName, loadedDataFrame) justColumns values = do
    case insertByColumnsIntoDataFrame loadedDataFrame justColumns values of
        Left errMsg -> return $ Left errMsg
        Right updatedDataFrame -> do
            saveTable (tableName, updatedDataFrame)
            return $ Right updatedDataFrame

insertByColumnsIntoDataFrame :: DataFrame -> Maybe [ColumnName] -> [Value] -> Either ErrorMessage DataFrame
insertByColumnsIntoDataFrame (DataFrame columns rows) maybeColumns newValues =
    let changedColumns = fromMaybe [] maybeColumns
        newRow = map (\col -> case elemIndex col changedColumns of
                                  Just index -> newValues !! index
                                  Nothing -> NullValue) [col | Column col _ <- columns]
    in Right $ DataFrame columns (rows ++ [newRow])

insertWithoutColumns :: (TableName, DataFrame) -> [Value] -> Execution (Either ErrorMessage DataFrame)
insertWithoutColumns (tableName, loadedDataFrame) values = do
    validatedValues <- validateRowLength loadedDataFrame values
    case validatedValues of
        Left errMsg -> return $ Left errMsg
        Right newValues -> do
            let updatedDataFrame = insertRowIntoDataFrame loadedDataFrame newValues
            saveTable (tableName, updatedDataFrame)
            return $ Right updatedDataFrame

validateRowLength :: DataFrame -> [Value] -> Execution (Either ErrorMessage [Value])
validateRowLength (DataFrame columns _) values =
    if length values == length columns
        then return $ Right values
        else return $ Left "Incorrect number of values for the row."

insertRowIntoDataFrame :: DataFrame -> [Value] -> DataFrame
insertRowIntoDataFrame (DataFrame columns rows) newValues =
    let updatedRows = rows ++ [newValues] in
        DataFrame columns updatedRows

loadTables :: [TableName] -> Execution (Either ErrorMessage [(TableName, DataFrame)])
loadTables [] = return $ Right []
loadTables tableNames = loadTables' tableNames []
    where
        loadTables' :: [TableName] -> [(TableName, DataFrame)] -> Execution (Either ErrorMessage [(TableName, DataFrame)])
        loadTables' [] acc = return $ Right acc
        loadTables' (x:xs) acc = do
            result <- loadTable x
            case result of
                Left err -> return $ Left err
                Right table -> loadTables' xs (acc ++ [table])

validateSelectDataTablesAndColumns :: [(TableName, DataFrame)] -> [SelectData] -> Either ErrorMessage ()
validateSelectDataTablesAndColumns tableData selectDataList = mapM_ validate selectDataList
  where
    validate :: SelectData -> Either ErrorMessage ()
    validate (SelectColumn columnName maybeTableName) = 
      maybe (Right ()) (`validateTableAndColumn` columnName) maybeTableName
    validate (SelectAggregate (Aggregate _ columnName) maybeTableName) = 
      maybe (Right ()) (`validateTableAndColumn` columnName) maybeTableName
    validate _ = Right () 

    validateTableAndColumn :: TableName -> ColumnName -> Either ErrorMessage ()
    validateTableAndColumn tableName columnName =
      case lookup tableName tableData of
        Just df -> if columnExistsInDataFrame columnName df
                     then Right ()
                     else Left $ "Column " ++ columnName ++ " does not exist in table " ++ tableName
        Nothing -> Left $ "Table " ++ tableName ++ " does not exist in statement"

validateWhereClauseTablesAndColumns :: [(TableName, DataFrame)] -> Maybe WhereClause -> Either ErrorMessage ()
validateWhereClauseTablesAndColumns tableData maybeWhereClause = 
    case maybeWhereClause of
        Just whereClause -> validateWhereClause whereClause
        Nothing -> Right () 

  where
    validateWhereClause :: WhereClause -> Either ErrorMessage ()
    validateWhereClause whereClause = mapM_ validateWhereCriterion whereClause

    validateWhereCriterion :: (WhereCriterion, Maybe LogicalOperator) -> Either ErrorMessage ()
    validateWhereCriterion (WhereCriterion leftExpr op rightExpr, _) = do
        validateExpression leftExpr
        validateExpression rightExpr

    validateExpression :: Expression -> Either ErrorMessage ()
    validateExpression (ColumnExpression columnName maybeTableName) =
        case maybeTableName of
            Just tableName -> 
                case findColumnTable columnName tableData of
                    Just _ -> Right ()
                    Nothing -> Left $ "Column " ++ columnName ++ " does not exist in table " ++ tableName
            Nothing -> Left $ "Table not specified for column " ++ columnName
    validateExpression _ = Right ()  -- Other expressions pass validation by default.

    findColumnTable :: ColumnName -> [(TableName, DataFrame)] -> Maybe TableName
    findColumnTable columnName = fmap fst . find (\(_, df) -> columnExistsInDataFrame columnName df)

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
        serializeTableName tableName' = "tableName: " ++ escapeValue tableName' ++ "\n"

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
        serializeDataType DateTimeType = "datetime"

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
        valueToString (StringValue x) = escapeValue x
        valueToString (BoolValue x) = show x
        valueToString (DateTimeValue x) = escapeValue x
        valueToString NullValue = "null"

        escapeValue :: String -> String
        escapeValue x = "\"" ++ x ++ "\""

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
                Y.String val -> do
                    let str = T.unpack val
                    if isDateTime str then
                        Right $ DateTimeValue str
                    else
                        Right $ StringValue str
                Y.Bool val -> Right $ BoolValue val
                Y.Null -> Right NullValue
                _ -> Left $ "Data type of value (" ++ show value ++ ") is not supported."
        
        isDateTime :: String -> Bool
        isDateTime str =
            case parseTimeM True defaultTimeLocale dateTimeFormat str :: Maybe UTCTime of
                Just _ -> True
                Nothing -> False

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
            "datetime" -> Right DateTimeType
            "bool" -> Right BoolType
            _ -> Left $ "Unrecognized data type: " ++ dataType

----------------------------------------------------------------
--------- updated parsing stuff
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement inp = do
    (rest, statement) <- runParser parser (dropWhile isSpace inp)
    _ <- runParser parseEndOfStatement rest
    return statement
    where
        parser :: Parser ParsedStatement
        parser = parseSelectStatement
                <|> parseSystemFunctionStatement
                <|> parseShowTableStatement
                <|> parseShowTablesStatement
                <|> parseSelectAllStatement
                <|> parseInsertStatement
                <|> parseUpdateStatement

-- statement by type parsing

parseShowTableStatement :: Parser ParsedStatement
parseShowTableStatement = do
    _ <- parseKeyword "show"
    _ <- parseWhitespace
    _ <- parseKeyword "table"
    _ <- parseWhitespace
    ShowTableStatement <$> parseWord

parseShowTablesStatement :: Parser ParsedStatement
parseShowTablesStatement = do
    _ <- parseKeyword "show"
    _ <- parseWhitespace
    _ <- parseKeyword "tables"
    pure ShowTablesStatement

parseSystemFunctionStatement :: Parser ParsedStatement
parseSystemFunctionStatement = do
    _ <- parseKeyword "select"
    _ <- parseWhitespace
    SystemFunctionStatement <$> parseSystemFunction

parseSelectAllStatement :: Parser ParsedStatement
parseSelectAllStatement = do
    _ <- parseKeyword "select"
    _ <- parseWhitespace
    _ <- parseKeyword "*"
    _ <- parseWhitespace
    _ <- parseKeyword "from"
    _ <- parseWhitespace
    tableNames <- parseWord `sepBy` (parseChar ',' *> optional parseWhitespace)
    whereClause <- optional parseWhereClause
    pure $ SelectAllStatement tableNames whereClause

parseSelectStatement :: Parser ParsedStatement
parseSelectStatement = do
    _ <- parseKeyword "select"
    _ <- parseWhitespace
    selectData <- parseSelectData `sepBy` (parseChar ',' *> optional parseWhitespace)
    _ <- parseWhitespace
    _ <- parseKeyword "from"
    _ <- parseWhitespace
    tableNames <- parseWord `sepBy` (parseChar ',' *> optional parseWhitespace)
    whereClause <- optional parseWhereClause
    pure $ SelectStatement tableNames selectData whereClause

parseInsertStatement :: Parser ParsedStatement
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

parseUpdateStatement :: Parser ParsedStatement
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
                       then Just <$> parseWhereClause'
                       else pure Nothing
  case whereConditions of
    Nothing -> return $ UpdateStatement tableName updatesList Nothing 
    Just conditions ->
      if null conditions
        then Parser $ \_ -> Left "At least one condition is required in the where clause"
        else return $ UpdateStatement tableName updatesList (Just conditions) 

-- insert and update util parsing functions

parseOptionalWhitespace :: Parser ()
parseOptionalWhitespace = many (parseWhitespace *> pure ()) *> pure ()

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
  values <- parseValue' `sepBy` (parseChar ',' *> parseOptionalWhitespace)
  _ <- parseChar ')'
  return values

parseValue' :: Parser Value
parseValue' = do
  parseOptionalWhitespace
  val <- parseNumericValue <|> parseBoolValue <|> parseNullValue <|> parseStringValue
  parseOptionalWhitespace
  return val

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

parseSatisfy :: (Char -> Bool) -> Parser Char
parseSatisfy predicate = Parser $ \inp ->
    case inp of
        [] -> Left "Empty input"
        (x:xs) -> if predicate x then Right (xs, x) else Left ("Unexpected character: " ++ [x])

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

parseWhereClauseFlag :: Parser Bool
parseWhereClauseFlag = do
  parseOptionalWhitespace
  flag <- choice [parseKeyword "where" >> pure True, pure False]
  case flag of
    Just b -> return b
    Nothing -> return False

--where clause parsing (for update)

parseWhereClause' :: Parser [Condition]
parseWhereClause' = do
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
  value <- parseValue'
  return $ Condition columnName op value

--where clause parsing (for select)

parseWhereClause :: Parser WhereClause
parseWhereClause = do
    _ <- parseWhitespace
    _ <- parseKeyword "where"
    _ <- parseWhitespace
    some parseCriterionAndOptionalOperator

    where
        parseCriterionAndOptionalOperator :: Parser (WhereCriterion, Maybe LogicalOperator)
        parseCriterionAndOptionalOperator = do
            crit <- parseWhereCriterion
            op <- optional (parseWhitespace >> parseLogicalOperator)
            _ <- optional parseWhitespace
            pure (crit, op)

parseLogicalOperator :: Parser LogicalOperator
parseLogicalOperator = parseKeyword "AND" >> pure And

parseExpression :: Parser Expression
parseExpression = parseValueExpression <|> parseColumnExpression
  where
    parseValueExpression = ValueExpression <$> parseValue
    parseColumnExpression = do
        firstPart <- parseWord
        maybeSecondPart <- optional (parseChar '.' *> parseWord)
        case maybeSecondPart of
            Just secondPart -> pure $ ColumnExpression secondPart (Just firstPart)
            Nothing -> pure $ ColumnExpression firstPart Nothing

parseWhereCriterion :: Parser WhereCriterion
parseWhereCriterion = do
    leftExpr <- parseExpression <|> Parser (\_ -> Left "Missing left-hand expression in criterion.")
    _ <- optional parseWhitespace
    op <- parseRelationalOperator <|> Parser (\_ -> Left "Missing relational operator.")
    _ <- optional parseWhitespace
    rightExpr <- parseExpression <|> Parser (\_ -> Left "Missing right-hand expression in criterion.")
    pure $ WhereCriterion leftExpr op rightExpr


parseSelectData :: Parser SelectData
parseSelectData = tryParseSystemFunction <|> tryParseAggregate <|> tryParseColumn
  where
    tryParseSystemFunction = do
        systemFunction <- parseSystemFunction
        pure $ SelectSystemFunction systemFunction

    tryParseAggregate = parseAggregate

    tryParseColumn = do
        (columnName, maybeTableName) <- parseColumnNameWithOptionalTable
        pure $ SelectColumn columnName maybeTableName
    
parseColumnNameWithOptionalTable :: Parser (ColumnName, Maybe TableName)
parseColumnNameWithOptionalTable = do
    firstPart <- parseWord
    maybeSecondPart <- optional (parseChar '.' *> parseWord)
    case maybeSecondPart of
        Just secondPart -> pure (secondPart, Just firstPart)
        Nothing -> pure (firstPart, Nothing)

-- aggregate parsing

parseAggregateFunction :: Parser AggregateFunction
parseAggregateFunction = parseMin <|> parseSum
  where
    parseMin = do
        _ <- parseKeyword "min"
        pure Min
    parseSum = do
        _ <- parseKeyword "sum"
        pure Sum

parseAggregate :: Parser SelectData
parseAggregate = do
    func <- parseAggregateFunction
    _ <- optional parseWhitespace
    _ <- parseChar '('
    _ <- optional parseWhitespace
    (columnName, maybeTableName) <- parseColumnNameWithOptionalTable
    _ <- optional parseWhitespace
    _ <- parseChar ')'
    let aggregate = Aggregate func columnName
    pure $ SelectAggregate aggregate maybeTableName
    
-- system function parsing 

parseSystemFunctionName :: Parser SystemFunction
parseSystemFunctionName = parseNow
    where 
    parseNow = do
        _ <- parseKeyword "now"
        pure Now

parseSystemFunction :: Parser SystemFunction
parseSystemFunction = do
    _ <- parseKeyword "now"
    _ <- optional parseWhitespace
    _ <- parseChar '('
    _ <- optional parseWhitespace
    _ <- parseChar ')'
    pure Now

-- validation

validateSelectData :: [SelectData] -> Either ErrorMessage ()
validateSelectData selectData
    | all isSelectColumnOrSystemFunction selectData = Right ()
    | all isSelectAggregateOrSystemFunction selectData = Right ()
    | otherwise = Left "Mixing columns and aggregate functions in SELECT is not allowed."

isSelectColumnOrSystemFunction :: SelectData -> Bool
isSelectColumnOrSystemFunction (SelectColumn _ _) = True
isSelectColumnOrSystemFunction (SelectSystemFunction _) = True
isSelectColumnOrSystemFunction _ = False

isSelectAggregateOrSystemFunction :: SelectData -> Bool
isSelectAggregateOrSystemFunction (SelectAggregate _ _) = True
isSelectAggregateOrSystemFunction (SelectSystemFunction _) = True
isSelectAggregateOrSystemFunction _ = False

updateUnspecifiedTableNames :: [(TableName, DataFrame)] -> [TableName] -> SelectQuery -> Either ErrorMessage SelectQuery
updateUnspecifiedTableNames tableData tableNames selectQuery = traverse updateSelectData selectQuery
  where
    updateSelectData :: SelectData -> Either ErrorMessage SelectData
    updateSelectData (SelectColumn columnName Nothing) =
        maybe (Left $ "Column " ++ columnName ++ " not found in any table") (Right . SelectColumn columnName . Just) (findColumnTable columnName tableData)
    updateSelectData (SelectAggregate (Aggregate aggFunc columnName) Nothing) =
        maybe (Left $ "Aggregate column " ++ columnName ++ " not found in any table") (\tableName -> Right $ SelectAggregate (Aggregate aggFunc columnName) (Just tableName)) (findColumnTable columnName tableData)
    updateSelectData sd = Right sd

    findColumnTable :: ColumnName -> [(TableName, DataFrame)] -> Maybe TableName
    findColumnTable columnName = fmap fst . find (\(_, df) -> columnExistsInDataFrame columnName df)

updateUnspecifiedTableNamesInWhereClause :: [(TableName, DataFrame)] -> Maybe WhereClause -> Either ErrorMessage (Maybe WhereClause)
updateUnspecifiedTableNamesInWhereClause tableData maybeWhereClause = 
    case maybeWhereClause of
        Just whereClause -> Just <$> traverse updateWhereCriterion whereClause
        Nothing -> Right Nothing
  where
    updateWhereCriterion :: (WhereCriterion, Maybe LogicalOperator) -> Either ErrorMessage (WhereCriterion, Maybe LogicalOperator)
    updateWhereCriterion (WhereCriterion leftExpr op rightExpr, logicalOp) = do
        updatedLeftExpr <- updateExpression leftExpr
        updatedRightExpr <- updateExpression rightExpr
        return (WhereCriterion updatedLeftExpr op updatedRightExpr, logicalOp)

    updateExpression :: Expression -> Either ErrorMessage Expression
    updateExpression (ColumnExpression columnName Nothing) =
        case findColumnTable columnName tableData of
            Just tableName -> Right $ ColumnExpression columnName (Just tableName)
            Nothing -> Left $ "Column " ++ columnName ++ " not found in any table"
    updateExpression expr = Right expr

    findColumnTable :: ColumnName -> [(TableName, DataFrame)] -> Maybe TableName
    findColumnTable columnName = fmap fst . find (\(_, df) -> columnExistsInDataFrame columnName df)

columnExistsInDataFrame :: ColumnName -> DataFrame -> Bool
columnExistsInDataFrame columnName (DataFrame columns _) =
  any (\(Column colName _) -> colName == columnName) columns

---- produce new dataframe based on tables and where clause

cartesianProduct :: [(TableName, DataFrame)] -> DataFrame
cartesianProduct [] = DataFrame [] []
cartesianProduct tableDataFrames = foldl combineDataFrames (DataFrame [] []) tableDataFrames
  where
    combineDataFrames :: DataFrame -> (TableName, DataFrame) -> DataFrame
    combineDataFrames (DataFrame accCols accRows) (tableName, DataFrame cols rows) =
        let newCols = [Column (tableName ++ "." ++ colName) colType | Column colName colType <- cols]
            newRows = if null accRows then rows else [accRow ++ row | accRow <- accRows, row <- rows]
        in DataFrame (accCols ++ newCols) newRows

findColumnIndex :: ColumnName -> [Column] -> Either ErrorMessage Int
findColumnIndex columnName columns = findColumnIndex' columnName columns 0 -- Start with index 0

findColumnIndex' :: ColumnName -> [Column] -> Int -> Either ErrorMessage Int
findColumnIndex' columnName [] _ = Left $ "Column with name '" ++ columnName ++ "' does not exist in the table."
findColumnIndex' columnName ((Column name _):xs) index
    | columnName == name = Right index
    | otherwise          = findColumnIndex' columnName xs (index + 1)

---------------- where clause filtering
filterDataFrameByWhereClause :: DataFrame -> Maybe WhereClause -> DataFrame
filterDataFrameByWhereClause df Nothing = df
filterDataFrameByWhereClause (DataFrame columns rows) (Just whereClause) =
    let filteredRows = filter (\row -> evaluateWhereClause whereClause columns row) rows
    in DataFrame columns filteredRows

evaluateWhereClause :: WhereClause -> [Column] -> Row -> Bool
evaluateWhereClause [] _ _ = True  -- If WhereClause is empty, include all rows
evaluateWhereClause ((criterion, logicalOp):rest) columns row =
    let criterionResult = evaluateCriterion criterion columns row
    in case logicalOp of
        Just And -> criterionResult && evaluateWhereClause rest columns row
        Nothing -> criterionResult

evaluateCriterion :: WhereCriterion -> [Column] -> Row -> Bool
evaluateCriterion (WhereCriterion leftExpr relOp rightExpr) columns row =
    let
        leftValue = evaluateExpression leftExpr columns row
        rightValue = evaluateExpression rightExpr columns row
    in
        case relOp of
            RelEQ -> leftValue == rightValue
            RelNE -> leftValue /= rightValue
            RelLT -> leftValue < rightValue
            RelGT -> leftValue > rightValue
            RelLE -> leftValue <= rightValue
            RelGE -> leftValue >= rightValue

evaluateExpression :: Expression -> [Column] -> Row -> Value
evaluateExpression (ValueExpression value) _ _ = value
evaluateExpression (ColumnExpression columnName maybeTableName) columns row =
    let fullColumnName = getFullColumnName columnName maybeTableName
    in case lookupColumnValue fullColumnName columns row of
        Just value -> value
        Nothing -> NullValue

------------- process columns query

getFullColumnName :: ColumnName -> Maybe TableName -> ColumnName
getFullColumnName columnName maybeTableName = 
    case maybeTableName of
        Just tableName -> tableName ++ "." ++ columnName
        Nothing -> columnName

lookupColumnValue :: ColumnName -> [Column] -> Row -> Maybe Value
lookupColumnValue columnName columns row =
    case findColumnIndex columnName columns of
        Left _ -> Nothing
        Right columnIndex -> Just (row !! columnIndex)

selectColumnsFromDataFrame :: DataFrame -> [SelectData] -> Execution DataFrame
selectColumnsFromDataFrame (DataFrame allColumns rows) selectData = do
    currentTime <- getTime
    let systemFunctionColumns = mapMaybe (systemFunctionToColumn currentTime) selectData
    let selectedColumnNames = [getFullColumnName colName maybeTableName | SelectColumn colName maybeTableName <- selectData]
    let selectedColumns = filter (\(Column colName _) -> colName `elem` selectedColumnNames) allColumns ++ systemFunctionColumns
    let selectedRows = map (\row -> selectRowColumns selectedColumnNames allColumns row ++ selectSystemFunctionValues systemFunctionColumns currentTime) rows
    return $ DataFrame selectedColumns selectedRows

systemFunctionToColumn :: UTCTime -> SelectData -> Maybe Column
systemFunctionToColumn currentTime (SelectSystemFunction Now) = Just $ Column "NOW()" DateTimeType
systemFunctionToColumn _ _ = Nothing

selectSystemFunctionValues :: [Column] -> UTCTime -> [Value]
selectSystemFunctionValues columns currentTime =
    map (systemFunctionToValue currentTime) columns

systemFunctionToValue :: UTCTime -> Column -> Value
systemFunctionToValue currentTime (Column "NOW()" DateTimeType) =
    DateTimeValue $ formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" currentTime
systemFunctionToValue _ _ = error "Unsupported system function or column type"


selectRowColumns :: [ColumnName] -> [Column] -> Row -> Row
selectRowColumns columnNames allColumns row =
    let columnIndexMap = zip (map (\(Column name _) -> name) allColumns) [0..]
        selectValue name = lookup name columnIndexMap >>= (`safeGet` row)
    in mapMaybe selectValue columnNames

safeGet :: Int -> [a] -> Maybe a
safeGet idx xs = if idx < length xs then Just (xs !! idx) else Nothing

------ process aggregates query

calculateAggregatesFromDataFrame :: DataFrame -> [SelectData] -> Execution (Either ErrorMessage DataFrame)
calculateAggregatesFromDataFrame (DataFrame inputColumns rows) selectData = do
    currentTime <- getTime
    let systemFunctionColumns = mapMaybe (systemFunctionToColumn currentTime) selectData
    let systemFunctionValues = selectSystemFunctionValues systemFunctionColumns currentTime

    let aggregateData = [(func, getFullColumnName colName maybeTableName) 
                            | SelectAggregate (Aggregate func colName) maybeTableName <- selectData]
    let aggregateResults = traverse (\(func, fullColName) -> calculateAggregate rows fullColName inputColumns func) aggregateData

    case aggregateResults of
        Left errMsg -> return $ Left errMsg
        Right aggregatedValues -> 
            let aggDataFrame@(DataFrame aggColumns aggRows) = constructDataFrame inputColumns aggregateData aggregatedValues
                finalColumns = aggColumns ++ systemFunctionColumns
                combinedRow = head aggRows ++ systemFunctionValues
            in return $ Right $ DataFrame finalColumns [combinedRow]

  where
    constructDataFrame :: [Column] -> [(AggregateFunction, ColumnName)] -> [Value] -> DataFrame
    constructDataFrame allColumns aggData aggValues =
        let aggColumns = map (toAggregateColumn allColumns) aggData
            aggRows = [aggValues] 
        in DataFrame aggColumns aggRows


    toAggregateColumn :: [Column] -> (AggregateFunction, ColumnName) -> Column
    toAggregateColumn allColumns (func, fullColName) =
        let colType = maybe IntegerType (\(Column _ t) -> t) $ find (\(Column name _) -> name == fullColName) allColumns
        in Column (show func ++ "(" ++ fullColName ++ ")") colType

    calculateAggregate :: [Row] -> ColumnName -> [Column] -> AggregateFunction -> Either ErrorMessage Value
    calculateAggregate rows fullColName columns func = 
        case getColumnType fullColName columns of
            Just IntegerType -> Right $ calculateIntAggregate func
            Just StringType  -> calculateStringAggregate func
            Just BoolType    -> Right $ calculateBoolAggregate func
            _                -> Left "Column type not supported for aggregation"
      where
        maybeValues = map (\row -> getValueForColumn fullColName columns row) rows
        values = catMaybes maybeValues
        intValues = mapMaybe extractInteger values
        stringValues = mapMaybe extractString values
        boolValues = mapMaybe extractBool values

        calculateIntAggregate Sum = IntegerValue $ sum intValues
        calculateIntAggregate Min = IntegerValue $ minimumOrDefault intValues

        calculateStringAggregate :: AggregateFunction -> Either ErrorMessage Value
        calculateStringAggregate Min = Right $ minimumStringOrDefault stringValues
        calculateStringAggregate _   = Left "Unsupported string aggregation"

        minimumStringOrDefault :: [String] -> Value
        minimumStringOrDefault [] = StringValue ""
        minimumStringOrDefault xs = StringValue $ minimum xs

        calculateBoolAggregate Sum = IntegerValue $ sum (map boolToInt boolValues)
        calculateBoolAggregate Min = IntegerValue $ minimumOrDefault (map boolToInt boolValues)

        minimumOrDefault [] = 0  -- Default value for empty lists
        minimumOrDefault xs = minimum xs

        boolToInt True  = 1
        boolToInt False = 0

    getColumnType :: ColumnName -> [Column] -> Maybe ColumnType
    getColumnType colName columns =
        listToMaybe [t | Column name t <- columns, name == colName]

    getValueForColumn :: ColumnName -> [Column] -> Row -> Maybe Value
    getValueForColumn columnName columns row =
        let columnIndex = findIndex (\(Column colName _) -> colName == columnName) columns
        in columnIndex >>= (`safeGet` row)

    safeGet :: Int -> [a] -> Maybe a
    safeGet idx xs = if idx >= 0 && idx < length xs then Just (xs !! idx) else Nothing

    extractInteger :: Value -> Maybe Integer
    extractInteger (IntegerValue i) = Just i
    extractInteger _ = Nothing

    extractString :: Value -> Maybe String
    extractString (StringValue s) = Just s
    extractString _ = Nothing

    extractBool :: Value -> Maybe Bool
    extractBool (BoolValue b) = Just b
    extractBool _ = Nothing

isAggregate :: SelectData -> Bool
isAggregate (SelectAggregate _ _) = True
isAggregate _ = False

createNowDataFrame :: Execution DataFrame
createNowDataFrame = do
    currentTime <- getTime
    let columns = [Column "NOW()" DateTimeType]
    let rows = [[DateTimeValue $ formatTime defaultTimeLocale dateTimeFormat currentTime]]
    return $ DataFrame columns rows

createShowTablesFrame :: Execution DataFrame
createShowTablesFrame = do
    tableNames <- getTableNames
    let columns = [Column "Tables" StringType]
    let rows = createRows tableNames []
    return $ DataFrame columns rows
    where
        createRows :: [TableName] -> [[Value]] -> [[Value]]
        createRows [] acc = acc
        createRows (x:xs) acc = createRows xs (createRow x : acc)

        createRow :: TableName -> [Value]
        createRow tableName = [StringValue tableName]