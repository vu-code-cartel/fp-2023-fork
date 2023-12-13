{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE InstanceSigs #-}

module Parser (
    ParsedStatement (..),
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
    parseStatement
    )
where
import Control.Monad (void) 
import Data.Char (isSpace, toLower, isAlphaNum, isDigit)
import Data.String (IsString, fromString)
import Control.Applicative(Alternative(empty, (<|>)),optional, some, many)
import Control.Monad.Trans.State.Strict (State, StateT, get, put, runState, runStateT, state)
import Control.Monad.Trans.Class(lift, MonadTrans)
import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..), Row)
import InMemoryTables (TableName, database)

type ErrorMessage = String

-- parsed statement types
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

data AggregateFunction
    = Min
    | Sum
    deriving (Show, Eq)

data Aggregate = Aggregate AggregateFunction ColumnName
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

data Condition = Condition String RelationalOperator Value
  deriving (Show, Eq)

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
}  | InsertStatement {
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
} | DropTableStatement{
    table :: TableName
} | CreateTableStatement{
    table :: TableName,
    newColumns :: [Column] 
}deriving (Show, Eq)
--
-- monad + state parser

newtype EitherT e m a = EitherT {
    runEitherT :: m (Either e a)
}

instance MonadTrans (EitherT e) where
  lift :: Monad m => m a -> EitherT e m a
  lift ma = EitherT $ fmap Right ma

--
--monads

instance Monad m => Functor (EitherT e m) where
  fmap :: (a -> b) -> EitherT e m a -> EitherT e m b
  fmap f ta = EitherT $ do
    eit <- runEitherT ta
    case eit of
      Left e -> return $ Left e
      Right a -> return $ Right (f a)

instance Monad m => Applicative (EitherT e m) where
  pure :: a -> EitherT e m a
  pure a = EitherT $ return $ Right a
  (<*>) :: EitherT e m (a -> b) -> EitherT e m a -> EitherT e m b
  af <*> aa = EitherT $ do
    f <- runEitherT af
    case f of
      Left e1 -> return $ Left e1
      Right r1 -> do
        a <- runEitherT aa
        case a of
          Left e2 -> return $ Left e2
          Right r2 -> return $ Right (r1 r2)


instance (IsString e) => Alternative (EitherT e (State s)) where
    empty :: EitherT e (State s) a
    empty = EitherT $ state $ \s -> (Left (fromString "Error"), s)

    (<|>) :: EitherT e (State s) a -> EitherT e (State s) a -> EitherT e (State s) a
    a1 <|> a2 = EitherT $ state $ \s ->
        let result = runState (runEitherT a1) s
        in case result of
            (Left _, _) -> runState (runEitherT a2) s
            _ -> result



instance Monad m => Monad (EitherT e m) where
  (>>=) :: EitherT e m a -> (a -> EitherT e m b) -> EitherT e m b
  m >>= k = EitherT $ do
    eit <- runEitherT m
    case eit of
      Left e -> return $ Left e
      Right r -> runEitherT (k r)

--
throwE :: Monad m => e -> EitherT e m a
throwE err = EitherT $ return $ Left err

type ParseError = String

type Parser a = EitherT ParseError (State String) a

runParser :: Parser a -> String -> Either ParseError (String, a)
runParser parser input = 
    let eitherResult = runState (runEitherT parser) input
    in case eitherResult of
        (Left errMsg, _) -> Left errMsg
        (Right value, remainingInput) -> Right (remainingInput, value)

parseStatement :: String -> Either ParseError ParsedStatement
parseStatement inp = do
    let trimmedInput = dropWhile isSpace inp
    (rest, statement) <- runParser parser trimmedInput
    (_, _) <- runParser parseEndOfStatement rest
    return statement
    where
        parser :: Parser ParsedStatement
        parser = parseShowTablesStatement
                <|> parseShowTableStatement
                <|> parseSelectAllStatement  
                <|> parseSystemFunctionStatement
                <|> parseSelectStatement
                <|> parseInsertStatement
                <|> parseUpdateStatement
                <|> parseDeleteStatement
                <|> parseDropTableStatement  
                <|> parseCreateTableStatement

--statement parsers by type
parseDropTableStatement :: Parser ParsedStatement
parseDropTableStatement = do
    _ <- parseKeyword "drop"
    _ <- parseWhitespace
    _ <- parseKeyword "table"
    _ <- parseWhitespace
    tableName <- parseWord 
    pure $ DropTableStatement tableName

parseCreateTableStatement :: Parser ParsedStatement
parseCreateTableStatement = do
    _ <- parseKeyword "create"
    _ <- parseWhitespace
    _ <- parseKeyword "table"
    _ <- parseWhitespace
    tableName <- parseWord
    _ <- optional parseWhitespace
    _ <- parseChar '('
    columnsWithType <- parseColumnList
    _ <- optional parseWhitespace
    _ <- parseChar ')'
    _ <- optional parseWhitespace
    pure $ CreateTableStatement tableName columnsWithType

parseColumnList :: Parser [Column]
parseColumnList = parseColumn `sepBy` (optional parseWhitespace *> parseChar ',' <* optional parseWhitespace)

parseColumn :: Parser Column
parseColumn = do
    columnName <- parseWord
    _ <- parseWhitespace
    columnType <- parseWord >>= either (throwE . show) pure . parseColumnType
    pure (Column columnName columnType)

parseColumnType :: String -> Either ParseError ColumnType
parseColumnType "int" = Right IntegerType
parseColumnType "varchar" = Right StringType
parseColumnType "bool" = Right BoolType
parseColumnType "date" = Right DateTimeType
parseColumnType other = Left $ "Unknown column type: " ++ other
   

--statement parsers by type
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
    pure $ SelectStatement tableNames selectData whereClause

parseInsertStatement :: Parser ParsedStatement
parseInsertStatement = do
  _ <- parseKeyword "insert"
  _ <- parseWhitespace
  _ <- parseKeyword "into"
  _ <- parseWhitespace
  tableName <- parseWord
  _ <- optional parseWhitespace
  columns <- optional parseColumnNames
  _ <- parseKeyword "values"
  _ <- optional parseWhitespace
  values <- parseValues
  case columns of
    Nothing -> return $ InsertStatement tableName columns values
    Just cols ->
      if length cols == length values
        then return $ InsertStatement tableName columns values
        else throwE "Column count does not match the number of values provided"

parseUpdateStatement :: Parser ParsedStatement
parseUpdateStatement = do
  _ <- parseKeyword "update"
  _ <- parseWhitespace
  tableName <- parseWord
  _ <- parseWhitespace
  _ <- parseKeyword "set"
  _ <- parseWhitespace
  updatesList <- parseUpdates
  whereConditions <- optional parseWhereClause'
  pure $ UpdateStatement tableName updatesList whereConditions

parseDeleteStatement :: Parser ParsedStatement
parseDeleteStatement = do
    _ <- parseKeyword "delete"
    _ <- parseWhitespace
    _ <- parseKeyword "from"
    _ <- parseWhitespace
    tableName <- parseWord
    _ <- optional parseWhitespace
    whereConditions <- optional parseWhereClause'
    pure $ DeleteStatement tableName whereConditions

-- util parsing functions
sepBy :: Parser a -> Parser b -> Parser [a]
sepBy p sep = do
    x <- p
    xs <- many (sep *> p)
    return (x:xs)

parseChar :: Char -> Parser Char
parseChar a = do
    inp <- lift get
    case inp of
        [] -> throwE "Empty input"
        (x:xs) -> if a == x then do
                                lift $ put xs
                                return a
                            else throwE ([a] ++ " expected but " ++ [x] ++ " found")

parseKeyword :: String -> Parser String
parseKeyword keyword = do
    inp <- lift get  
    let len = length keyword
    case splitAt len inp of
        (_, []) -> throwE "Empty input"  
        (xs, rest) 
            | map toLower xs == map toLower keyword -> do
                lift $ put rest 
                return xs
            | otherwise -> throwE $ "Expected " ++ keyword ++ ", but found " ++ xs
        
parseWord :: Parser String
parseWord = do
    inp <- lift get  
    let word = takeWhile (\x -> isAlphaNum x || x == '_') inp
    case word of
        [] -> throwE "Empty input"
        xs -> do
            lift $ put $ drop (length xs) inp 
            return xs

parseWhitespace :: Parser String
parseWhitespace = do
    inp <- lift get 
    case span isSpace inp of
        ("", _) -> throwE $ "Expected whitespace before: " ++ inp
        (whitespace, rest) -> do
            lift $ put rest 
            return whitespace

parseEndOfStatement :: Parser String
parseEndOfStatement = do
    _ <- optional parseWhitespace
    _ <- optional (parseChar ';')
    _ <- optional parseWhitespace
    ensureNothingLeft
  where
    ensureNothingLeft :: Parser String
    ensureNothingLeft = do
        inp <- lift get  
        case inp of
            [] -> return [] 
            s -> throwE $ "Characters found after end of SQL statement: " ++ s

parseValue :: Parser Value
parseValue = do
    _ <- parseChar '\''
    strValue <- many (parseSatisfy (/= '\''))
    _ <- parseChar '\''
    return $ StringValue strValue
-- data transformation parsers

parseWhereClause' :: Parser [Condition]
parseWhereClause' = do
  _ <- parseKeyword "where"
  _ <- parseWhitespace
  conditions <- parseConditions
  return conditions

parseConditions :: Parser [Condition]
parseConditions = parseCondition `sepBy` (parseKeyword "and" *> optional parseWhitespace)

parseCondition :: Parser Condition
parseCondition = do
  columnName <- parseWord
  _ <- optional parseWhitespace
  op <- parseRelationalOperator
  _ <- optional parseWhitespace
  value <- parseValue'
  return $ Condition columnName op value

parseSatisfy :: (Char -> Bool) -> Parser Char
parseSatisfy predicate = EitherT $ state $ \inp ->
    case inp of
        [] -> (Left "Empty input", inp)
        (x:xs) -> if predicate x 
                then (Right x, xs) 
                else (Left ("Unexpected character: " ++ [x]), inp)
        

parseColumnNames :: Parser [String]
parseColumnNames = do
  _ <- parseChar '('
  names <- parseWord `sepBy` (parseChar ',' *> optional parseWhitespace)
  _ <- parseChar ')'
  _ <- parseWhitespace
  return names

parseValues :: Parser [Value]
parseValues = do
  _ <- parseChar '('
  values <- parseValue' `sepBy` (parseChar ',' *> optional parseWhitespace)
  _ <- parseChar ')'
  return values

parseValue' :: Parser Value
parseValue' = do
  _ <- optional parseWhitespace
  val <- parseNumericValue <|> parseBoolValue <|> parseNullValue <|> parseStringValue
  _ <- optional parseWhitespace
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

parseUpdates :: Parser [(String, Value)]
parseUpdates = do
  updatesList <- parseUpdate `sepBy` (parseChar ',' *> optional parseWhitespace)
  return updatesList

parseUpdate :: Parser (String, Value)
parseUpdate = do
  columnName <- parseWord
  _ <- optional parseWhitespace
  _ <- parseChar '='
  _ <- optional parseWhitespace
  value <- parseValueAndQuoteFlag
  return (columnName, value)

parseValueAndQuoteFlag :: Parser (Value)
parseValueAndQuoteFlag = do
  _ <- optional parseWhitespace
  val <- parseNumericValue <|> parseBoolValue <|> parseNullValue <|> parseStringValue
  _ <- optional parseWhitespace
  return val


-- where clause parsing
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
    leftExpr <- parseExpression <|> throwE "Missing left-hand expression in criterion."
    _ <- optional parseWhitespace
    op <- parseRelationalOperator <|> throwE "Missing relational operator."
    _ <- optional parseWhitespace
    rightExpr <- parseExpression <|> throwE "Missing right-hand expression in criterion."
    return $ WhereCriterion leftExpr op rightExpr


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

parseRelationalOperator :: Parser RelationalOperator
parseRelationalOperator =
      (parseKeyword "=" >> pure RelEQ)
  <|> (parseKeyword "!=" >> pure RelNE)
  <|> (parseKeyword "<=" >> pure RelLE)
  <|> (parseKeyword ">=" >> pure RelGE)
  <|> (parseKeyword "<" >> pure RelLT)
  <|> (parseKeyword ">" >> pure RelGT)

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