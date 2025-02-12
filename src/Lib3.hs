{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

module Lib3
  ( Lib3.ParsedStatement(..),
    Execution,
    ExecutionAlgebra(..),
    SelectData (..),
    Aggregate (..),
    AggregateFunction (..),
    Expression (..),
    WhereClause (..),
    WhereCriterion (..),
    SystemFunction (..),
    executeSql,
    executeSqlWithParser,
    Lib3.parseStatement,
    parseTable,
    serializeTable
  )
where

import Control.Monad.Free (Free (..), liftF)
import Control.Monad.Trans.Except
import Control.Monad.Trans.Class (lift)
import DataFrame (DataFrame (..), ColumnType (IntegerType, StringType, BoolType, DateTimeType), Column (..), Value (..), Row)
import Data.Time ( UTCTime )
import Control.Applicative ( many, some, Alternative((<|>)), optional )
import Data.List (find, findIndex, elemIndex, sortBy)
import Data.Maybe (mapMaybe, catMaybes, listToMaybe, isJust, fromMaybe)
import Data.Ord (comparing, Down(..))
import Data.Time.Format (formatTime, defaultTimeLocale, parseTimeM)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Yaml as Y
import GHC.Generics (Generic)
import Data.Char (toLower, isSpace, isDigit)
import Lib1 (validateDataFrame)
import Lib2(
    Parser(..),
    parseChar,
    parseWhitespace,
    parseKeyword,
    parseWord,
    parseValue,
    sepBy,
    parseEndOfStatement,
    parseRelationalOperator,
    )
import Parser (
    ParsedStatement(..),
    RelationalOperator(..),
    LogicalOperator(..),
    Expression(..),
    AggregateFunction(..),
    Aggregate(..),
    WhereCriterion(..),
    SystemFunction(..),
    SelectData(..),
    SelectQuery(..),
    Condition(..),
    OrderClause(..),
    SortDirection(..),
    parseStatement
    )

type TableName = String
type FileContent = String

data ExecutionAlgebra next
  = LoadTable TableName (Either ErrorMessage (TableName, DataFrame) -> next)
  | SaveTable (TableName, DataFrame) (() -> next)
  | GetTime (UTCTime -> next)
  | GetTableNames ([TableName] -> next)
  | RemoveTable TableName (Maybe ErrorMessage -> next)
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
} | DeleteStatement {
    tableNameDelete :: TableName,
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

removeTable :: TableName -> Execution (Maybe ErrorMessage)
removeTable tableName = liftF $ RemoveTable tableName id

executeSqlWithParser :: String -> Execution (Either ErrorMessage DataFrame)
executeSqlWithParser sql = case Parser.parseStatement sql of
    Left errorMsg -> return $ Left errorMsg
    Right (Parser.SelectStatement tableNames query maybeWhereClause maybeOrderClause) -> 
        executeSelectStatementWithOrderClause tableNames query maybeWhereClause maybeOrderClause
    Right (Parser.SelectAllStatement tableNames maybeWhereClause maybeOrderClause) -> do
        executeSelectAllWithOptionalOrder tableNames maybeWhereClause maybeOrderClause
    Right (Parser.ShowTableStatement table) -> do
        loadResult <- loadTable table
        case loadResult of 
            Left errorMsg -> return $ Left errorMsg
            Right (_, dataframe) -> return $ Right dataframe
    Right Parser.ShowTablesStatement -> do
        showTablesFrame <- createShowTablesFrame
        return $ Right showTablesFrame
    Right (Parser.SystemFunctionStatement function) -> do
        case function of
            Now -> do
                nowDataFrame <- createNowDataFrame
                return $ Right nowDataFrame
    Right (Parser.InsertStatement tableName maybeColumns values) -> do
        loadResult <- loadTable tableName
        case loadResult of
            Left errorMsg -> return $ Left errorMsg
            Right tableData -> do
                case maybeColumns of
                    Just justColumns -> insertWithColumns tableData justColumns values
                    Nothing -> insertWithoutColumns tableData values
    Right (Parser.UpdateStatement tableName updates whereConditions) -> do
        loadResult <- loadTable tableName
        case loadResult of
            Left errorMsg -> return $ Left errorMsg
            Right tableData -> do
                if isJust whereConditions
                    then updateWithWhere tableData updates whereConditions
                    else updateWithoutWhere tableData updates
    Right (Parser.DeleteStatement tableName whereConditions) -> do
        loadResult <- loadTable tableName
        case loadResult of
            Left errorMsg -> return $ Left errorMsg
            Right tableData -> do
                if isJust whereConditions
                    then deleteWithWhere tableData whereConditions
                    else deleteWithoutWhere tableData
    Right (Parser.DropTableStatement tableName) -> do
        _ <- removeTable tableName
        return $ Right (DataFrame [] [])
    Right (Parser.CreateTableStatement tableName columns) -> do
        loadResult <- loadTable tableName
        case loadResult of
            Left _ -> do
                saveTable (tableName, DataFrame columns [])
                return $ Right (DataFrame columns [])
            Right _ -> return $ Left "Table already exists."

executeSql :: String -> Execution (Either ErrorMessage DataFrame)
executeSql sql = case Lib3.parseStatement sql of
    Left errorMsg -> return $ Left errorMsg
    Right (Lib3.SelectStatement tableNames query maybeWhereClause) -> 
        executeSelectStatement tableNames query maybeWhereClause
    Right (Lib3.SelectAllStatement tableNames maybeWhereClause) -> do
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
    Right (Lib3.ShowTableStatement table) -> do
        loadResult <- loadTable table
        case loadResult of 
            Left errorMsg -> return $ Left errorMsg
            Right (_, dataframe) -> return $ Right dataframe
    Right Lib3.ShowTablesStatement -> do
        showTablesFrame <- createShowTablesFrame
        return $ Right showTablesFrame
    Right (Lib3.SystemFunctionStatement function) -> do
        case function of
            Now -> do
                nowDataFrame <- createNowDataFrame
                return $ Right nowDataFrame
    Right (Lib3.InsertStatement tableName maybeColumns values) -> do
        loadResult <- loadTable tableName
        case loadResult of
            Left errorMsg -> return $ Left errorMsg
            Right tableData -> do
                case maybeColumns of
                    Just justColumns -> insertWithColumns tableData justColumns values
                    Nothing -> insertWithoutColumns tableData values
    Right (Lib3.UpdateStatement tableName updates whereConditions) -> do
        loadResult <- loadTable tableName
        case loadResult of
            Left errorMsg -> return $ Left errorMsg
            Right tableData -> do
                if isJust whereConditions
                    then updateWithWhere tableData updates whereConditions
                    else updateWithoutWhere tableData updates
    Right (Lib3.DeleteStatement tableName whereConditions) -> do
        loadResult <- loadTable tableName
        case loadResult of
            Left errorMsg -> return $ Left errorMsg
            Right tableData -> do
                if isJust whereConditions
                    then deleteWithWhere tableData whereConditions
                    else deleteWithoutWhere tableData

executeSelectStatementWithOrderClause :: [TableName] -> SelectQuery -> Maybe WhereClause -> Maybe OrderClause -> Execution (Either ErrorMessage DataFrame)
executeSelectStatementWithOrderClause tableNames query maybeWhereClause maybeOrderClause = do
    loadResult <- loadTables tableNames
    case loadResult of
        Left errorMsg -> return $ Left errorMsg
        Right tableData -> runExceptT $ do
            updatedQuery <- ExceptT . return $ updateUnspecifiedTableNames tableData tableNames query
            _ <- ExceptT . return $ validateSelectDataTablesAndColumns tableData updatedQuery
            updatedMaybeWhereClause <- ExceptT . return $ updateUnspecifiedTableNamesInWhereClause tableData maybeWhereClause
            _ <- ExceptT . return $ validateWhereClauseTablesAndColumns tableData updatedMaybeWhereClause
            updatedMaybeOrderClause <- ExceptT . return $ updateUnspecifiedTableNamesInOrderClause tableData maybeOrderClause

            let productDataFrame = cartesianProduct tableData
            let filteredDataFrame = filterDataFrameByWhereClause productDataFrame updatedMaybeWhereClause
            resultDataFrame <- if any isAggregate updatedQuery 
                               then ExceptT $ calculateAggregatesFromDataFrame filteredDataFrame updatedQuery
                               else lift $ selectColumnsFromDataFrame filteredDataFrame updatedQuery

            case updatedMaybeOrderClause of
                Just orderClause -> ExceptT . return $ sortDataFrame resultDataFrame orderClause
                Nothing -> return resultDataFrame

executeSelectAllWithOptionalOrder :: [TableName] -> Maybe WhereClause -> Maybe OrderClause -> Execution (Either ErrorMessage DataFrame)
executeSelectAllWithOptionalOrder tableNames maybeWhereClause maybeOrderClause = runExceptT $ do
    tableData <- ExceptT $ loadTables tableNames
    updatedMaybeWhereClause <- ExceptT . return $ updateUnspecifiedTableNamesInWhereClause tableData maybeWhereClause
    updatedMaybeOrderClause <- ExceptT . return $ updateUnspecifiedTableNamesInOrderClause tableData maybeOrderClause

    let productDataFrame = cartesianProduct tableData
    let filteredDataFrame = filterDataFrameByWhereClause productDataFrame updatedMaybeWhereClause

    case updatedMaybeOrderClause of
        Just orderClause -> ExceptT . return $ sortDataFrame filteredDataFrame orderClause
        Nothing -> return filteredDataFrame

executeSelectStatement :: [TableName] -> SelectQuery -> Maybe WhereClause ->Execution (Either ErrorMessage DataFrame)
executeSelectStatement tableNames query maybeWhereClause = do
    loadResult <- loadTables tableNames
    case loadResult of
        Left errorMsg -> return $ Left errorMsg
        Right tableData -> runExceptT $ do
            updatedQuery <- ExceptT . return $ updateUnspecifiedTableNames tableData tableNames query
            _ <- ExceptT . return $ validateSelectDataTablesAndColumns tableData updatedQuery
            updatedMaybeWhereClause <- ExceptT . return $ updateUnspecifiedTableNamesInWhereClause tableData maybeWhereClause
            _ <- ExceptT . return $ validateWhereClauseTablesAndColumns tableData updatedMaybeWhereClause


            let productDataFrame = cartesianProduct tableData
            let filteredDataFrame = filterDataFrameByWhereClause productDataFrame updatedMaybeWhereClause

            if any isAggregate updatedQuery then
                ExceptT $ calculateAggregatesFromDataFrame filteredDataFrame updatedQuery
            else
                lift $ selectColumnsFromDataFrame filteredDataFrame updatedQuery

-- order by functions

sortDataFrame :: DataFrame -> OrderClause -> Either ErrorMessage DataFrame
sortDataFrame (DataFrame columns rows) orderClause = do
    sortCriteria <- mapM (createSortCriterion columns) orderClause
    let sortedRows = sortBy (makeMultiColumnComparator sortCriteria) rows
    return $ DataFrame columns sortedRows

createSortCriterion :: [Column] -> (SortDirection, Maybe TableName, ColumnName) -> Either ErrorMessage (Int, SortDirection)
createSortCriterion columns (direction, maybeTableName, columnName) =
    case findIndex (matchesColumn maybeTableName columnName) columns of
        Just idx -> Right (idx, direction)
        Nothing -> Left $ "Column not found: " ++ fromMaybe "" maybeTableName ++ "." ++ columnName

matchesColumn :: Maybe TableName -> ColumnName -> Column -> Bool
matchesColumn maybeTableName columnName (Column colName _) =
    colName == fullColumnName
    where fullColumnName = fromMaybe "" maybeTableName ++ "." ++ columnName

makeMultiColumnComparator :: [(Int, SortDirection)] -> Row -> Row -> Ordering
makeMultiColumnComparator [] _ _ = EQ
makeMultiColumnComparator ((idx, dir):criteria) row1 row2 =
    let primaryOrder = compareValue dir (row1 !! idx) (row2 !! idx)
    in if primaryOrder == EQ then makeMultiColumnComparator criteria row1 row2 else primaryOrder

compareValue :: SortDirection -> Value -> Value -> Ordering
compareValue Asc v1 v2 = compare v1 v2
compareValue Desc v1 v2 = compare v2 v1

-- delete functions

deleteWithoutWhere :: (TableName, DataFrame) -> Execution (Either ErrorMessage DataFrame)
deleteWithoutWhere (tableName, originalDataFrame) = do
    let deletedDataFrame = deleteAllRows originalDataFrame
    saveTable (tableName, deletedDataFrame)
    return $ Right deletedDataFrame

deleteAllRows :: DataFrame -> DataFrame
deleteAllRows (DataFrame columns _) = DataFrame columns []

deleteWithWhere :: (TableName, DataFrame) -> Maybe [Condition] -> Execution (Either ErrorMessage DataFrame)
deleteWithWhere (tableName, originalDataFrame) (Just whereConditions) = do
    let filteredDataFrame = filterDataFrameByConditions originalDataFrame whereConditions
    case deleteFilteredRows originalDataFrame filteredDataFrame of
        Left errMsg -> return $ Left errMsg
        Right finalDataFrame -> do
            saveTable (tableName, finalDataFrame)
            return $ Right finalDataFrame

deleteFilteredRows :: DataFrame -> DataFrame -> Either ErrorMessage DataFrame
deleteFilteredRows _ (DataFrame _ []) = Left "There are no rows that fit the where conditions"
deleteFilteredRows (DataFrame columns originalRows) (DataFrame _ filteredRows) =
    let newRows = filter (\row -> row `notElem` filteredRows) originalRows
    in if null newRows
        then Left "Error deleting rows. No rows matching where clause"
        else Right (DataFrame columns newRows)

-- update functions

updateWithWhere :: (TableName, DataFrame) -> [(ColumnName, Value)] -> Maybe [Condition] -> Execution (Either ErrorMessage DataFrame)
updateWithWhere (tableName, originalDataFrame) updates (Just whereConditions) = do
        let filteredDataFrame = filterDataFrameByConditions originalDataFrame whereConditions
        case updateFilteredRows filteredDataFrame updates of
            Left errMsg -> return $ Left errMsg
            Right updatedRows -> do
                let combinedDataFrame = updateOriginalDataFrame originalDataFrame updatedRows
                saveTable (tableName, combinedDataFrame)
                return $ Right combinedDataFrame

filterDataFrameByConditions :: DataFrame -> [Condition] -> DataFrame
filterDataFrameByConditions (DataFrame columns rows) conditions =
    let filteredRows = filter (satisfiesConditions conditions columns) rows
    in DataFrame columns filteredRows

satisfiesConditions :: [Condition] -> [Column] -> Row -> Bool
satisfiesConditions conditions columns row = all (evalCondition row columns) conditions

evalCondition :: Row -> [Column] -> Condition -> Bool
evalCondition row columns (Condition columnName op rightValue) =
    case findColumnIndex columnName columns of
        Left _ -> False
        Right colIndex -> let leftValue = row !! colIndex in
            case op of
                RelEQ -> leftValue == rightValue
                RelNE -> leftValue /= rightValue
                RelLT -> leftValue < rightValue
                RelGT -> leftValue > rightValue
                RelLE -> leftValue <= rightValue
                RelGE -> leftValue >= rightValue 

updateFilteredRows :: DataFrame -> [(ColumnName, Value)] -> Either ErrorMessage [(Row, Row)]
updateFilteredRows (DataFrame columns rows) updates =
    let updateRow' row = (row, updateRow row updates columns)
        updatedRows = map updateRow' rows
    in Right updatedRows

updateOriginalDataFrame :: DataFrame -> [(Row, Row)] -> DataFrame
updateOriginalDataFrame (DataFrame columns oldRows) updatedRowTuples =
    let updatedRows = map snd updatedRowTuples
        unchangedRows = filter (\oldRow -> notElem oldRow (map fst updatedRowTuples)) oldRows
        newRows = unchangedRows ++ updatedRows
    in DataFrame columns newRows

updateWithoutWhere :: (TableName, DataFrame) -> [(ColumnName, Value)] -> Execution (Either ErrorMessage DataFrame)
updateWithoutWhere (tableName, dataFrame) updates = do
    case updateAllRows dataFrame updates of
        Left errMsg -> return $ Left errMsg
        Right updatedDataFrame -> do
            saveTable (tableName, updatedDataFrame)
            return $ Right updatedDataFrame

updateAllRows :: DataFrame -> [(ColumnName, Value)] -> Either ErrorMessage DataFrame
updateAllRows (DataFrame columns rows) updates =
    let updatedRows = map (\row -> updateRow row updates columns) rows
    in Right (DataFrame columns updatedRows)

updateRow :: Row -> [(ColumnName, Value)] -> [Column]-> Row
updateRow row updates columns =
    foldl (\acc (colName, value) -> updateValue colName value acc columns) row updates

updateValue :: ColumnName -> Value -> Row -> [Column] -> Row
updateValue colName newValue row columns =
    case findColumnIndex colName columns of
        Left _ -> row -- Column not found, do nothing
        Right colIndex -> replaceAtIndex colIndex newValue row

replaceAtIndex :: Int -> a -> [a] -> [a]
replaceAtIndex _ _ [] = []
replaceAtIndex i newVal (x:xs)
    | i == 0    = newVal : xs
    | otherwise = x : replaceAtIndex (i - 1) newVal xs

-- insert functions

insertWithColumns :: (TableName, DataFrame) -> [ColumnName] -> [Value] -> Execution (Either ErrorMessage DataFrame)
insertWithColumns (tableName, loadedDataFrame) justColumns values = do
    case insertByColumnsIntoDataFrame loadedDataFrame justColumns values of
        Left errMsg -> return $ Left errMsg
        Right updatedDataFrame -> do
            saveTable (tableName, updatedDataFrame)
            return $ Right updatedDataFrame

insertByColumnsIntoDataFrame :: DataFrame -> [ColumnName] -> [Value] -> Either ErrorMessage DataFrame
insertByColumnsIntoDataFrame (DataFrame columns rows) changedColumns newValues =
    let newRow = map (\col -> case elemIndex col changedColumns of
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

-- end of insert functions

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
parseStatement :: String -> Either ErrorMessage Lib3.ParsedStatement
parseStatement inp = do
    (rest, statement) <- runParser parser (dropWhile isSpace inp)
    _ <- runParser parseEndOfStatement rest
    return statement
    where
        parser :: Parser Lib3.ParsedStatement
        parser = parseSelectStatement
                <|> parseSystemFunctionStatement
                <|> parseShowTableStatement
                <|> parseShowTablesStatement
                <|> parseSelectAllStatement
                <|> parseInsertStatement
                <|> parseUpdateStatement
                <|> parseDeleteStatement

-- statement by type parsing

parseShowTableStatement :: Parser Lib3.ParsedStatement
parseShowTableStatement = do
    _ <- parseKeyword "show"
    _ <- parseWhitespace
    _ <- parseKeyword "table"
    _ <- parseWhitespace
    Lib3.ShowTableStatement <$> parseWord

parseShowTablesStatement :: Parser Lib3.ParsedStatement
parseShowTablesStatement = do
    _ <- parseKeyword "show"
    _ <- parseWhitespace
    _ <- parseKeyword "tables"
    pure Lib3.ShowTablesStatement

parseSystemFunctionStatement :: Parser Lib3.ParsedStatement
parseSystemFunctionStatement = do
    _ <- parseKeyword "select"
    _ <- parseWhitespace
    Lib3.SystemFunctionStatement <$> parseSystemFunction

parseSelectAllStatement :: Parser Lib3.ParsedStatement
parseSelectAllStatement = do
    _ <- parseKeyword "select"
    _ <- parseWhitespace
    _ <- parseKeyword "*"
    _ <- parseWhitespace
    _ <- parseKeyword "from"
    _ <- parseWhitespace
    tableNames <- parseWord `sepBy` (parseChar ',' *> optional parseWhitespace)
    whereClause <- optional parseWhereClause
    pure $ Lib3.SelectAllStatement tableNames whereClause

parseSelectStatement :: Parser Lib3.ParsedStatement
parseSelectStatement = do
    _ <- parseKeyword "select"
    _ <- parseWhitespace
    selectData <- parseSelectData `sepBy` (parseChar ',' *> optional parseWhitespace)
    _ <- parseWhitespace
    _ <- parseKeyword "from"
    _ <- parseWhitespace
    tableNames <- parseWord `sepBy` (parseChar ',' *> optional parseWhitespace)
    whereClause <- optional parseWhereClause
    pure $ Lib3.SelectStatement tableNames selectData whereClause

parseInsertStatement :: Parser Lib3.ParsedStatement
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
    Nothing -> return $ Lib3.InsertStatement tableName columns values
    Just cols ->
      if length cols == length values
        then return $ Lib3.InsertStatement tableName columns values
        else Parser $ \_ -> Left "Column count does not match the number of values provided"

parseUpdateStatement :: Parser Lib3.ParsedStatement
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
    Nothing -> return $ Lib3.UpdateStatement tableName updatesList Nothing 
    Just conditions ->
      if null conditions
        then Parser $ \_ -> Left "At least one condition is required in the where clause"
        else return $ Lib3.UpdateStatement tableName updatesList (Just conditions) 

parseDeleteStatement :: Parser Lib3.ParsedStatement
parseDeleteStatement = do
    _ <- parseKeyword "delete"
    _ <- parseWhitespace
    _ <- parseKeyword "from"
    _ <- parseWhitespace
    tableName <- parseWord
    hasWhere <- optional $ parseWhereClauseFlag
    whereConditions <- if hasWhere == Just True
                        then Just <$> parseWhereClause'
                        else pure Nothing
    case whereConditions of
        Nothing -> return $ Lib3.DeleteStatement tableName Nothing 
        Just conditions ->
            if null conditions
                then Parser $ \_ -> Left "At least one condition is required in the where clause"
                else return $ Lib3.DeleteStatement tableName (Just conditions)

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

columnExistsInDataFrame :: ColumnName -> DataFrame -> Bool
columnExistsInDataFrame columnName (DataFrame columns _) =
  any (\(Column colName _) -> colName == columnName) columns

updateUnspecifiedTableNamesInOrderClause :: [(TableName, DataFrame)] -> Maybe OrderClause -> Either ErrorMessage (Maybe OrderClause)
updateUnspecifiedTableNamesInOrderClause tableData maybeOrderClause = case maybeOrderClause of
    Just orderClause -> fmap Just (traverse updateOrderElement orderClause)
    Nothing -> Right Nothing
  where
    updateOrderElement :: (SortDirection, Maybe TableName, ColumnName) -> Either ErrorMessage (SortDirection, Maybe TableName, ColumnName)
    updateOrderElement (sortDir, Nothing, columnName) =
        maybe (Left $ "Column " ++ columnName ++ " not found in any table") (\tableName -> Right (sortDir, Just tableName, columnName)) (findColumnTable columnName tableData)
    updateOrderElement oe = Right oe

findColumnTable :: ColumnName -> [(TableName, DataFrame)] -> Maybe TableName
findColumnTable columnName = fmap fst . find (\(_, df) -> columnExistsInDataFrame columnName df)

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

    -- Create selectedColumns based on selectData, allowing duplicates
    let selectedColumns = concatMap (selectColumnsByName allColumns) selectData ++ systemFunctionColumns

    let selectedRows = map (\row -> selectRowColumns (map columnName selectedColumns) allColumns row ++ selectSystemFunctionValues systemFunctionColumns currentTime) rows
    return $ DataFrame selectedColumns selectedRows

  where
    selectColumnsByName :: [Column] -> SelectData -> [Column]
    selectColumnsByName allCols (SelectColumn colName maybeTableName) =
        case find (\(Column name _) -> name == getFullColumnName colName maybeTableName) allCols of
            Just col -> [col]
            Nothing -> []
    selectColumnsByName _ _ = []

    columnName :: Column -> ColumnName
    columnName (Column name _) = name

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