{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}

module Lib3
  ( executeSql,
    Execution,
    ExecutionAlgebra(..),
    executeStatement,
    parseStatement
  )
where

import Control.Monad.Free (Free (..), liftF)
import DataFrame (DataFrame (..), ColumnType (IntegerType, StringType, BoolType, DateTimeType), Column (..), Value (..), Row)
import Data.Time ( UTCTime )
import Control.Applicative ( many, some, Alternative(empty, (<|>)), optional )
import Data.Foldable (traverse_)
import Data.List (find, findIndex)
import Data.Maybe (mapMaybe, catMaybes, listToMaybe)
import Data.Time.Clock (UTCTime)
import Data.Time.Format (formatTime, defaultTimeLocale)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Yaml as Y
import GHC.Generics (Generic)
import Data.Char (toLower)
import Lib1 (validateDataFrame)
import Data.Char (toLower, isSpace, isAlphaNum, isDigit)
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
import InMemoryTables (database)

type TableName = String
type FileContent = String

data Condition = Condition String RelationalOperator' Value
  deriving (Show, Eq)

data ExecutionAlgebra next
  = LoadFile TableName (FileContent -> next)
  | SaveFile TableName FileContent (() -> next)
  | GetTime (UTCTime -> next)
  | ExecutePure String (Either ErrorMessage DataFrame -> next)
  | InsertData TableName [[Y.Value]] (() -> next)
  -- feel free to add more constructors here
  deriving Functor

type Execution = Free ExecutionAlgebra

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

data RelationalOperator' = Equal | LessThan | GreaterThan | LessThanOrEqual | GreaterThanOrEqual | NotEqual
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

type ErrorMessage = String
type Database = [(TableName, DataFrame)]
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
} | SystemFunctionStatement {
    function :: SystemFunction
} | ShowTableStatement {
    table :: TableName
} | ShowTablesStatement { }
    deriving (Show, Eq)

loadFile :: TableName -> Execution FileContent
loadFile name = liftF $ LoadFile name id

saveFile :: TableName -> FileContent -> Execution ()
saveFile tableName fileContent = liftF $ SaveFile tableName fileContent id

getTime :: Execution UTCTime
getTime = liftF $ GetTime id

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = liftF $ ExecutePure sql id

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
  values <- parseValues'
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
 
parseValues' :: Parser [Value]
parseValues' = do
  _ <- parseChar '('
  values <- parseValue `sepBy` (parseChar ',' *> parseOptionalWhitespace)
  _ <- parseChar ')'
  return values

parseValue' :: Parser Value
parseValue' = do
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
                       then Just <$> parseWhereClause'
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
  op <- parseRelationalOperator'
  parseOptionalWhitespace
  value <- parseValue'
  return $ Condition columnName op value

parseRelationalOperator' :: Parser RelationalOperator'
parseRelationalOperator' =
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

----------------------------------------------------------------
--------- updated parsing stuff
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement inp = case runParser parser (dropWhile isSpace inp) of
    Left err1 -> Left err1
    Right (rest, statement) -> case statement of
        SelectStatement _ _ _ -> case runParser parseEndOfStatement rest of
            Left err2 -> Left err2
            Right _ -> Right statement
        SystemFunctionStatement _ -> case runParser parseEndOfStatement rest of
            Left err2 -> Left err2
            Right _ -> Right statement
        ShowTableStatement _ -> case runParser parseEndOfStatement rest of
            Left err2 -> Left err2
            Right _ -> Right statement
        ShowTablesStatement -> case runParser parseEndOfStatement rest of
            Left err2 -> Left err2
            Right _ -> Right statement
    where
        parser :: Parser ParsedStatement
        parser = parseSelectStatement
                <|> parseSystemFunctionStatement
                <|> parseShowTableStatement
                <|> parseShowTablesStatement
                <|> parseSelectAllStatement

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
    updatedSelectData <- liftEither $ updateUnspecifiedTableNames tableNames selectData
    updatedWhereClause <- liftEither $ updateUnspecifiedTableNamesInWhereClause tableNames whereClause

    -- Perform validations on the updated selectData
    case validateAll updatedSelectData tableNames updatedWhereClause of
        Left err -> Parser $ \_ -> Left err
        Right _ -> pure $ SelectStatement tableNames updatedSelectData updatedWhereClause

validateAll :: [SelectData] -> [TableName] -> Maybe WhereClause -> Either ErrorMessage ()
validateAll updatedSelectData tableNames maybeWhereClause = do
    validateSelectData updatedSelectData
    validateTablesExistInDatabase tableNames
    validateColumnsInDatabase updatedSelectData
    validateTableNamesInSelectData tableNames updatedSelectData
    case maybeWhereClause of
        Just whereClause -> traverse_ (validateWhereCriteria tableNames . fst) whereClause
        Nothing -> Right ()


parseSelectAllStatement :: Parser ParsedStatement
parseSelectAllStatement = do
    _ <- parseKeyword "select"
    _ <- parseWhitespace
    _ <- parseKeyword "*"
    _ <- parseWhitespace
    _ <- parseKeyword "from"
    _ <- parseWhitespace
    ShowTableStatement <$> parseWord

-- util parsing functions

liftEither :: Either ErrorMessage a -> Parser a
liftEither = either (Parser . const . Left) pure

--where clause parsing

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

-- Checks if a column exists in the specified table
checkColumnExistsInTable :: ColumnName -> TableName -> Either ErrorMessage ()
checkColumnExistsInTable columnName tableName = do
    df <- getTableByName tableName
    if columnExistsInDataFrame columnName df
    then Right ()
    else Left $ "Column '" ++ columnName ++ "' does not exist in table '" ++ tableName ++ "'"

-- Checks if a column exists in any of the specified tables
columnExistsInAnyTable :: ColumnName -> [TableName] -> Bool
columnExistsInAnyTable columnName tableNames =
    any (\tableName -> case getTableByName tableName of
                          Right df -> columnExistsInDataFrame columnName df
                          Left _ -> False) tableNames

-- Validate if columns with specified table names exist in their respective tables
validateColumnsInDatabase :: [SelectData] -> Either ErrorMessage ()
validateColumnsInDatabase selectData = traverse_ validateColumnExists selectData
  where
    validateColumnExists (SelectColumn columnName (Just tableName)) =
        checkColumnExistsInTable columnName tableName
    validateColumnExists (SelectAggregate (Aggregate _ columnName) (Just tableName)) =
        checkColumnExistsInTable columnName tableName
    validateColumnExists _ = Right ()  -- No validation needed for system functions or unspecified table names

-- Validate if all specified table names exist in the database
validateTablesExistInDatabase :: [TableName] -> Either ErrorMessage ()
validateTablesExistInDatabase = traverse_ getTableByName

-- Validate if columns reference a table included in the FROM clause or exist in any of the specified tables
validateTableNamesInSelectData :: [TableName] -> SelectQuery -> Either ErrorMessage ()
validateTableNamesInSelectData tableNames query =
    if all (\sd -> isTableNameValid sd || isColumnInAnyTable sd) query
    then Right ()
    else Left "One or more columns reference a table not included in the FROM clause or do not exist in any specified table."
  where
    isTableNameValid (SelectColumn _ (Just tableName)) = tableName `elem` tableNames
    isTableNameValid (SelectAggregate _ (Just tableName)) = tableName `elem` tableNames
    isTableNameValid _ = True  -- True for SystemFunction or unspecified table name
    
    isColumnInAnyTable (SelectColumn columnName Nothing) = columnExistsInAnyTable columnName tableNames
    isColumnInAnyTable (SelectAggregate (Aggregate _ columnName) Nothing) = columnExistsInAnyTable columnName tableNames
    isColumnInAnyTable _ = False

updateUnspecifiedTableNames :: [TableName] -> SelectQuery -> Either ErrorMessage SelectQuery
updateUnspecifiedTableNames tableNames selectQuery = traverse updateSelectData selectQuery
  where
    updateSelectData :: SelectData -> Either ErrorMessage SelectData
    updateSelectData (SelectColumn columnName Nothing) =
        case findColumnTable columnName tableNames of
            Just tableName -> Right $ SelectColumn columnName (Just tableName)
            Nothing -> Right $ SelectColumn columnName Nothing  -- Column not found in any table
    updateSelectData (SelectAggregate (Aggregate aggFunc columnName) Nothing) =
        case findColumnTable columnName tableNames of
            Just tableName -> Right $ SelectAggregate (Aggregate aggFunc columnName) (Just tableName)
            Nothing -> Right $ SelectAggregate (Aggregate aggFunc columnName) Nothing
    updateSelectData sd = Right sd  -- No update needed for other cases

    findColumnTable :: ColumnName -> [TableName] -> Maybe TableName
    findColumnTable columnName = find (columnExistsIn columnName)

columnExistsInDataFrame :: ColumnName -> DataFrame -> Bool
columnExistsInDataFrame columnName (DataFrame columns _) =
  any (\(Column colName _) -> colName == columnName) columns

findColumnTable :: ColumnName -> [TableName] -> Maybe TableName
findColumnTable columnName = find (columnExistsIn columnName)

columnExistsIn :: ColumnName -> TableName -> Bool
columnExistsIn columnName tableName =
  case getTableByName tableName of
    Right df -> columnExistsInDataFrame columnName df
    Left _ -> False

validateWhereClause :: [TableName] -> Maybe WhereClause -> Either ErrorMessage ()
validateWhereClause tableNames maybeWhereClause = 
    case maybeWhereClause of
        Just whereClause -> traverse_ (validateWhereCriteria tableNames . fst) whereClause
        Nothing -> Right ()

validateWhereCriteria :: [TableName] -> WhereCriterion -> Either ErrorMessage ()
validateWhereCriteria tableNames (WhereCriterion leftExpr _ rightExpr) = do
    validateExpression leftExpr
    validateExpression rightExpr
  where
    validateExpression :: Expression -> Either ErrorMessage ()
    validateExpression (ColumnExpression columnName maybeTableName) =
        case maybeTableName of
            Just tableName -> checkColumnExistsInTable columnName tableName
            Nothing -> 
                if columnExistsInAnyTable columnName tableNames
                then Right ()
                else Left $ "Column '" ++ columnName ++ "' does not exist in any of the specified tables."
    validateExpression _ = Right ()  -- No validation needed for ValueExpression

updateUnspecifiedTableNamesInWhereClause :: [TableName] -> Maybe WhereClause -> Either ErrorMessage (Maybe WhereClause)
updateUnspecifiedTableNamesInWhereClause tableNames maybeWhereClause = 
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
        case findColumnTable columnName tableNames of
            Just tableName -> Right $ ColumnExpression columnName (Just tableName)
            Nothing -> Right $ ColumnExpression columnName Nothing -- Column not found in any table
    updateExpression expr = Right expr -- No update needed for other cases

-- --util functions

getTableByName :: TableName -> Either ErrorMessage DataFrame
getTableByName tableName =
  case lookup tableName database of
    Just table -> Right table
    Nothing -> Left $ "Table with name '" ++ tableName ++ "' does not exist in the database."

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


createDataFrameFromSelectStatement :: ParsedStatement -> Either ErrorMessage DataFrame
createDataFrameFromSelectStatement (SelectStatement tables _ _) = do
    tableDataFrames <- mapM (\tableName -> fmap (\df -> (tableName, df)) (getTableByName tableName)) tables
    return $ cartesianProduct tableDataFrames
createDataFrameFromSelectStatement _ = Left "Not a SelectStatement"

findColumnIndex :: ColumnName -> [Column] -> Either ErrorMessage Int
findColumnIndex columnName columns = findColumnIndex' columnName columns 0 -- Start with index 0

findColumnIndex' :: ColumnName -> [Column] -> Int -> Either ErrorMessage Int
findColumnIndex' columnName [] _ = Left $ "Column with name '" ++ columnName ++ "' does not exist in the table."
findColumnIndex' columnName ((Column name _):xs) index
    | columnName == name = Right index
    | otherwise          = findColumnIndex' columnName xs (index + 1)


filterDataFrameByWhereClause :: DataFrame -> WhereClause -> DataFrame
filterDataFrameByWhereClause (DataFrame columns rows) whereClause =
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

selectColumnsFromDataFrame :: DataFrame -> [SelectData] -> UTCTime -> DataFrame
selectColumnsFromDataFrame (DataFrame allColumns rows) selectData currentTime =
    let systemFunctionColumns = mapMaybe (systemFunctionToColumn currentTime) selectData
        selectedColumnNames = [getFullColumnName colName maybeTableName | SelectColumn colName maybeTableName <- selectData]
        selectedColumns = filter (\(Column colName _) -> colName `elem` selectedColumnNames) allColumns ++ systemFunctionColumns
        selectedRows = map (\row -> selectRowColumns selectedColumnNames allColumns row ++ selectSystemFunctionValues systemFunctionColumns currentTime) rows
    in DataFrame selectedColumns selectedRows

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

calculateAggregatesFromDataFrame :: DataFrame -> [SelectData] -> UTCTime -> Either ErrorMessage DataFrame
calculateAggregatesFromDataFrame (DataFrame inputColumns rows) selectData currentTime =
    let
        -- Identify and process system function columns
        systemFunctionColumns = mapMaybe (systemFunctionToColumn currentTime) selectData
        systemFunctionValues = selectSystemFunctionValues systemFunctionColumns currentTime

        -- Proceed with aggregate data calculation
        aggregateData = [(func, getFullColumnName colName maybeTableName) 
                         | SelectAggregate (Aggregate func colName) maybeTableName <- selectData]
        aggregateResults = traverse (\(func, fullColName) -> calculateAggregate rows fullColName inputColumns func) aggregateData

    in case aggregateResults of
        Left errMsg -> Left errMsg
        Right aggregatedValues -> 
            -- Construct DataFrame for aggregated values
            let aggDataFrame@(DataFrame aggColumns aggRows) = constructDataFrame inputColumns aggregateData aggregatedValues

                -- Combine aggregated DataFrame columns with system function columns
                finalColumns = aggColumns ++ systemFunctionColumns
                combinedRow = head aggRows ++ systemFunctionValues  -- Assuming aggRows has at least one row
            in Right $ DataFrame finalColumns [combinedRow]

  where
    constructDataFrame :: [Column] -> [(AggregateFunction, ColumnName)] -> [Value] -> DataFrame
    constructDataFrame allColumns aggData aggValues =
        let aggColumns = map (toAggregateColumn allColumns) aggData
            aggRows = [aggValues]  -- Each aggregated value in its own column
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
        minimumStringOrDefault [] = StringValue "" -- Default value for empty lists
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
-----------------------------------------------

-- -- Executes a parsed statement. Produces a DataFrame. Uses
-- -- InMemoryTables.databases as a source of data.

executeStatement :: UTCTime -> ParsedStatement -> Either ErrorMessage DataFrame
executeStatement currentTime (SelectStatement tables query maybeWhereClause) =
    case createDataFrameFromSelectStatement (SelectStatement tables query maybeWhereClause) of
        Left errMsg -> Left errMsg
        Right productDataFrame ->
            let filteredDataFrame = case maybeWhereClause of
                    Just whereClause -> filterDataFrameByWhereClause productDataFrame whereClause
                    Nothing -> productDataFrame
            in processSelectQuery currentTime filteredDataFrame query
executeStatement currentTime (SystemFunctionStatement function) = 
    case function of
        Now -> Right $ createNowDataFrame currentTime
executeStatement _ (ShowTableStatement tableName) =
    getTableByName tableName
executeStatement _ ShowTablesStatement =
    Right $ convertToDataFrame (tableNames database)

processSelectQuery :: UTCTime -> DataFrame -> [SelectData] -> Either ErrorMessage DataFrame
processSelectQuery currentTime dataframe query =
    if any isAggregate query
    then calculateAggregatesFromDataFrame dataframe query currentTime
    else Right $ selectColumnsFromDataFrame dataframe query currentTime

isAggregate :: SelectData -> Bool
isAggregate (SelectAggregate _ _) = True
isAggregate _ = False
tableNames :: Database -> [TableName]
tableNames db = map fst db

createNowDataFrame :: UTCTime -> DataFrame
createNowDataFrame currentTime = 
    let columns = [Column "NOW()" DateTimeType]
        rows = [[DateTimeValue $ formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S" currentTime]]
    in DataFrame columns rows

convertToDataFrame :: [TableName] -> DataFrame
convertToDataFrame alltableNames = DataFrame [Column "Table Name" StringType] (map (\name -> [StringValue name]) alltableNames)