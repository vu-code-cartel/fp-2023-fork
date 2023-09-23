{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Lib1
  ( parseSelectAllStatement,
    findTableByName,
    validateDataFrame,
    renderDataFrameAsTable,
  )
where

import DataFrame (DataFrame (..), Row, Column (..), ColumnType (..), Value (..))
import InMemoryTables (TableName)



type ErrorMessage = String

type Database = [(TableName, DataFrame)]

-- Your code modifications go below this comment

-- 1) implement the function which returns a data frame by its name
-- in provided Database list
findTableByName :: Database -> String -> Maybe DataFrame
findTableByName _ _ = error "findTableByName not implemented"

-- 2) implement the function which parses a "select * from ..."
-- sql statement and extracts a table name from the statement
parseSelectAllStatement :: String -> Either ErrorMessage TableName
parseSelectAllStatement "" = Left "Empty input"
parseSelectAllStatement selectStatement1 =
    case parseStatement selectStatement1 of
        Left e -> Left e
        Right selectStatement -> Right selectStatement
  where
    parseStatement :: String -> Either ErrorMessage String
    parseStatement input =
        case parseSelect input of
            Left e -> Left e
            Right rest1 ->
                case parseWhitespace rest1 of
                    Left e -> Left e
                    Right rest2 ->
                        case parseColumnList rest2 of
                            Left e -> Left e
                            Right rest3 ->
                                case parseWhitespace rest3 of
                                    Left e -> Left e
                                    Right rest4 ->
                                        case parseFrom rest4 of
                                            Left e -> Left e
                                            Right rest5 ->
                                                case parseWhitespace rest5 of
                                                    Left e -> Left e
                                                    Right rest6 -> Right (removeTrailingSemicolon rest6)

    parseWhitespace :: String -> Either ErrorMessage String
    parseWhitespace s = case span customIsSpace s of
        ("", rest) -> Left "Expected whitespace"
        (whitespace, rest) -> Right rest

    parseColumnList :: String -> Either ErrorMessage String
    parseColumnList ('*':rest) = Right rest
    parseColumnList _ = Left "Expected '*'"

    parseFrom :: String -> Either ErrorMessage String
    parseFrom s = case stripPrefix "from" (makeLowerCaseString s) of
        Just rest -> Right rest
        Nothing -> Left "Expected 'from'"

    parseSelect :: String -> Either ErrorMessage String
    parseSelect s = case stripPrefix "select" (makeLowerCaseString s) of
        Just rest -> Right rest
        Nothing -> Left "Expected 'select'"

    makeLowerCaseString :: String -> String
    makeLowerCaseString = map toLower

    toLower :: Char -> Char
    toLower c
        | c >= 'A' && c <= 'Z' = toEnum (fromEnum c + 32)
        | otherwise = c

    removeTrailingSemicolon :: String -> String
    removeTrailingSemicolon s
        | not (null s) && last s == ';' = init s
        | otherwise = s

    customIsSpace :: Char -> Bool
    customIsSpace c
        | c == ' '  = True
        | otherwise = False

    stripPrefix :: Eq a => [a] -> [a] -> Maybe [a]
    stripPrefix [] xs = Just xs
    stripPrefix (_:_) [] = Nothing
    stripPrefix (p:prefix) (x:xs)
        | p == x = stripPrefix prefix xs
        | otherwise = Nothing
        
-- 3) implement the function which validates tables: checks if
-- columns match value types, if rows sizes match columns,..
validateDataFrame :: DataFrame -> Either ErrorMessage ()
validateDataFrame _ = error "validateDataFrame ot implemented"

-- 4) implement the function which renders a given data frame
-- as ascii-art table (use your imagination, there is no "correct"
-- answer for this task!), it should respect terminal
-- width (in chars, provided as the first argument)
renderDataFrameAsTable :: Integer -> DataFrame -> String
renderDataFrameAsTable _ _ = error "renderDataFrameAsTable not implemented"
