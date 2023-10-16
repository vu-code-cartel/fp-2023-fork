{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# LANGUAGE InstanceSigs #-}

module Lib2
  ( parseStatement,
    executeStatement,
    ParsedStatement
  )
where

import DataFrame (DataFrame(..), Column(..), ColumnType(..), Value(..)) 
import InMemoryTables (TableName, database)
import Control.Applicative

type ErrorMessage = String
type Database = [(TableName, DataFrame)]

data ParsedStatement = ShowTablesStatement

newtype Parser a = Parser {
    runParser :: String -> Either ErrorMessage (String, a)
}

instance Functor Parser where
    fmap f p = Parser $ \inp ->
        case runParser p inp of
            Left err -> Left err
            Right (l, a) -> Right (l, f a)

instance Applicative Parser where
    pure a = Parser $ \inp -> Right (inp, a)
    pf <*> pa = Parser $ \inp1 ->
        case runParser pf inp1 of
            Left err1 -> Left err1
            Right (inp2, f) -> case runParser pa inp2 of
                Left err2 -> Left err2
                Right (inp3, a) -> Right (inp3, f a)

instance Alternative Parser where
    empty = Parser $ \_ -> Left "Error"
    p1 <|> p2 = Parser $ \inp ->
        case runParser p1 inp of
            Right a1 -> Right a1
            Left _ -> case runParser p2 inp of
                Right a2 -> Right a2
                Left err -> Left err

instance Monad Parser where
    pa >>= pbGen = Parser $ \inp1 ->
        case runParser pa inp1 of
            Left err1 -> Left err1
            Right (inp2, a) -> case runParser (pbGen a) inp2 of
                Left err2 -> Left err2
                Right (inp3, b) -> Right (inp3, b)

myCustomIsPrefixOf :: Eq a => [a] -> [a] -> Bool
myCustomIsPrefixOf [] _ = True
myCustomIsPrefixOf _ [] = False
myCustomIsPrefixOf (x:xs) (y:ys) = x == y && myCustomIsPrefixOf xs ys

string :: String -> Parser String
string s = Parser $ \inp ->
  if myCustomIsPrefixOf s inp
    then Right (drop (length s) inp, s)
    else Left "Parse error"

parseShow :: Parser ()
parseShow = do
  _ <- string "show"
  pure ()

parseWhiteSpace :: Parser ()
parseWhiteSpace = do
  _ <- many (string " ")
  pure ()

parseTables :: Parser ()
parseTables = do
  _ <- string "tables"
  pure ()

parseShowTables :: Parser ParsedStatement
parseShowTables = do
  parseShow
  parseWhiteSpace
  parseTables
  pure ShowTablesStatement


toLowerString :: String -> String
toLowerString = map toLowerChar
  where
    toLowerChar c
      | c >= 'A' && c <= 'Z' = toEnum (fromEnum c + 32)
      | otherwise = c

-- use here <|> to check parsers: for example in case of: case runParser (parseSelect <|> parseShowTables) (toLowerString inp) of ...
parseStatement :: String -> Either ErrorMessage ParsedStatement
parseStatement inp =
  case runParser parseShowTables (toLowerString inp) of
    Left err -> Left err
    Right ("", statement) -> Right statement
    Right (_, _) -> Left "Parse error"

executeStatement :: ParsedStatement -> Either ErrorMessage DataFrame
executeStatement ShowTablesStatement = Right $ convertToDataFrame (tableNames database)

tableNames :: Database -> [TableName]
tableNames db = map fst db

convertToDataFrame :: [TableName] -> DataFrame
convertToDataFrame tableNames = DataFrame [Column "Table Name" StringType] (map (\name -> [StringValue name]) tableNames)