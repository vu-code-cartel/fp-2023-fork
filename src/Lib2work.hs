{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE InstanceSigs #-}
{-# LANGUAGE DataKinds #-}
{-# OPTIONS_GHC -Wno-overlapping-patterns #-}
{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}
{-# HLINT ignore "Eta reduce" #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}
{-# HLINT ignore "Redundant return" #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Lib2work
  ( parseStatement,
    executeStatement,
    ParsedStatement (..),
    SelectQuery (..),
    RelationalOperator (..),
    SelectData (..),
    Aggregate (..),
    AggregateFunction (..),
    Expression (..),
    WhereClause (..),
    WhereCriterion (..),
    LogicalOperator (..),
    Value(..)
  )
where

import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..), Row)
import InMemoryTables (TableName, database)
import Control.Applicative ( many, some, Alternative(empty, (<|>)), optional )
import Data.Char (toLower, isSpace, isAlphaNum)
import Data.Foldable (traverse_)
import Data.List (find, findIndex)
import Data.Maybe (mapMaybe, catMaybes, listToMaybe)
import Data.Time.Clock (UTCTime)
import Data.Time.Format (formatTime, defaultTimeLocale)

type ErrorMessage = String
type Database = [(TableName, DataFrame)]
type ColumnName = String

data RelationalOperator
    = RelEQ
    | RelNE
    | RelLT
    | RelGT
    | RelLE
    | RelGE
    deriving (Show, Eq)


data LogicalOperator
    = And
    deriving (Show, Eq)

data Expression
    = ValueExpression Value
    | ColumnExpression ColumnName (Maybe TableName)
    deriving (Show, Eq)

data WhereCriterion = WhereCriterion Expression RelationalOperator Expression
    deriving (Show, Eq)

data AggregateFunction
    = Min
    | Sum
    deriving (Show, Eq)

data SystemFunction
    = Now
    deriving (Show, Eq)

data Aggregate = Aggregate AggregateFunction ColumnName 
    deriving (Show, Eq)

data SelectData
    = SelectColumn ColumnName (Maybe TableName)
    | SelectAggregate Aggregate (Maybe TableName)
    | SelectSystemFunction SystemFunction
    deriving (Show, Eq)

type SelectQuery = [SelectData]
type WhereClause = [(WhereCriterion, Maybe LogicalOperator)]

-- Keep the type, modify constructors

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

newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (String, a)
}

instance Functor Parser where
    fmap :: (a -> b) -> Parser a -> Parser b
    fmap f p = Parser $ \inp ->
        case runParser p inp of
            Left err -> Left err
            Right (l, a) -> Right (l, f a)

instance Applicative Parser where
    pure :: a -> Parser a
    pure a = Parser $ \inp -> Right (inp, a)
    (<*>) :: Parser (a -> b) -> Parser a -> Parser b
    pf <*> pa = Parser $ \inp1 ->
        case runParser pf inp1 of
            Left err1 -> Left err1
            Right (inp2, f) -> case runParser pa inp2 of
                Left err2 -> Left err2
                Right (inp3, a) -> Right (inp3, f a)

instance Alternative Parser where
    empty :: Parser a
    empty = Parser $ \_ -> Left "Error"
    (<|>) :: Parser a -> Parser a -> Parser a
    p1 <|> p2 = Parser $ \inp ->
        case runParser p1 inp of
            Right a1 -> Right a1
            Left _ -> case runParser p2 inp of
                Right a2 -> Right a2
                Left err -> Left err

instance Monad Parser where
    (>>=) :: Parser a -> (a -> Parser b) -> Parser b
    pa >>= pbGen = Parser $ \inp1 ->
        case runParser pa inp1 of
            Left err1 -> Left err1
            Right (inp2, a) -> case runParser (pbGen a) inp2 of
                Left err2 -> Left err2
                Right (inp3, b) -> Right (inp3, b)

instance Ord Value where
    compare NullValue NullValue = EQ
    compare NullValue _ = LT
    compare _ NullValue = GT
    compare (IntegerValue a) (IntegerValue b) = compare a b
    compare (StringValue a) (StringValue b) = compare a b
    compare (BoolValue a) (BoolValue b) = compare a b

-- Parses user input into an entity representing a parsed
-- statement

parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement inp = case runParser parser (dropWhile isSpace inp) of
    Left err1 -> Left err1
    Right (rest, statement) -> case statement of
        SystemFunctionStatement _ -> case runParser parseEndOfStatement rest of
            Left err2 -> Left err2
            Right _ -> Right statement
        SelectStatement _ _ _ -> case runParser parseEndOfStatement rest of
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
        parser = parseSystemFunctionStatement
                <|> parseSelectStatement
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

parseKeyword :: String -> Parser String
parseKeyword keyword = Parser $ \inp ->
    case take (length keyword) inp of
        [] -> Left "Empty input"
        xs
            | map toLower xs == map toLower keyword -> Right (drop (length xs) inp, xs)
            | otherwise -> Left $ "Expected " ++ keyword

parseWhitespace :: Parser String
parseWhitespace = Parser $ \inp ->
    case span isSpace inp of
        ("", _) -> Left $ "Expected whitespace before: " ++ inp
        (whitespace, rest) -> Right (rest, whitespace)

parseEndOfStatement :: Parser String
parseEndOfStatement = do
    _ <- optional parseWhitespace
    _ <- optional (parseChar ';')
    _ <- optional parseWhitespace
    ensureNothingLeft
    where
        ensureNothingLeft :: Parser String
        ensureNothingLeft = Parser $ \inp ->
            case inp of
                [] -> Right ([], [])
                s -> Left ("Characters found after end of SQL statement." ++ s)

parseChar :: Char -> Parser Char
parseChar ch = Parser $ \inp ->
    case inp of
        [] -> Left "Empty input"
        (x:xs) -> if ch == x then Right (xs, ch) else Left ("Expected " ++ [ch])

parseWord :: Parser String
parseWord = Parser $ \inp ->
    case takeWhile (\x -> isAlphaNum x || x == '_') inp of
        [] -> Left "Empty input"
        xs -> Right (drop (length xs) inp, xs)

parseValue :: Parser Value
parseValue = do
    _ <- parseChar '\''
    strValue <- many (parseSatisfy (/= '\''))
    _ <- parseChar '\''
    return $ StringValue strValue
    where
        parseSatisfy :: (Char -> Bool) -> Parser Char
        parseSatisfy predicate = Parser $ \inp ->
            case inp of
                [] -> Left "Empty input"
                (x:xs) -> if predicate x then Right (xs, x) else Left ("Unexpected character: " ++ [x])

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

parseRelationalOperator :: Parser RelationalOperator
parseRelationalOperator =
      (parseKeyword "=" >> pure RelEQ)
  <|> (parseKeyword "!=" >> pure RelNE)
  <|> (parseKeyword "<=" >> pure RelLE)
  <|> (parseKeyword ">=" >> pure RelGE)
  <|> (parseKeyword "<" >> pure RelLT)
  <|> (parseKeyword ">" >> pure RelGT)

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


sepBy :: Parser a -> Parser b -> Parser [a]
sepBy p sep = do
    x <- p
    xs <- many (sep *> p)
    return (x:xs)

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

calculateAggregatesFromDataFrame :: DataFrame -> [SelectData] -> Either ErrorMessage DataFrame
calculateAggregatesFromDataFrame (DataFrame columns rows) selectData =
    let aggregateData = [(func, getFullColumnName colName maybeTableName) 
                         | SelectAggregate (Aggregate func colName) maybeTableName <- selectData]
        aggregateResults = traverse (\(func, fullColName) -> calculateAggregate rows fullColName columns func) aggregateData
    in case aggregateResults of
        Left errMsg -> Left errMsg
        Right aggregatedValues -> Right $ constructDataFrame aggregateData aggregatedValues
  where
    constructDataFrame :: [(AggregateFunction, ColumnName)] -> [Value] -> DataFrame
    constructDataFrame aggData aggValues =
        let aggColumns = map (toAggregateColumn columns) aggData
            aggRows = [aggValues]  -- Each aggregated value in its own column
        in DataFrame aggColumns aggRows

    toAggregateColumn :: [Column] -> (AggregateFunction, ColumnName) -> Column
    toAggregateColumn allColumns (func, fullColName) =
        let colType = maybe IntegerType (\(Column _ t) -> t) $ find (\(Column name _) -> name == fullColName) allColumns
        in Column (show func ++ "_" ++ fullColName) colType

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
executeStatement _ (ShowTableStatement tableName) =
    getTableByName tableName
executeStatement _ ShowTablesStatement =
    Right $ convertToDataFrame (tableNames database)

processSelectQuery :: UTCTime -> DataFrame -> [SelectData] -> Either ErrorMessage DataFrame
processSelectQuery currentTime dataframe query =
    if any isAggregate query
    then calculateAggregatesFromDataFrame dataframe query
    else Right $ selectColumnsFromDataFrame dataframe query currentTime

isAggregate :: SelectData -> Bool
isAggregate (SelectAggregate _ _) = True
isAggregate _ = False
tableNames :: Database -> [TableName]
tableNames db = map fst db

convertToDataFrame :: [TableName] -> DataFrame
convertToDataFrame alltableNames = DataFrame [Column "Table Name" StringType] (map (\name -> [StringValue name]) alltableNames)