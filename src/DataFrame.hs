{-# LANGUAGE DeriveGeneric #-}

module DataFrame (Column (..), ColumnType (..), Value (..), Row, DataFrame (..)) where

import GHC.Generics
import Data.Aeson qualified as A

data ColumnType
  = IntegerType
  | StringType
  | BoolType
  | DateTimeType 
  deriving (Show, Eq, Generic)

data Column = Column String ColumnType
  deriving (Show, Eq, Generic)

data Value
  = IntegerValue Integer
  | StringValue String
  | BoolValue Bool
  | NullValue
  | DateTimeValue String  
  deriving (Show, Eq, Generic)

type Row = [Value]

data DataFrame = DataFrame [Column] [Row]
  deriving (Show, Eq, Generic)

instance A.ToJSON Column;
instance A.ToJSON ColumnType;
instance A.ToJSON Value;
instance A.ToJSON DataFrame;
instance A.FromJSON Column;
instance A.FromJSON ColumnType;
instance A.FromJSON Value;
instance A.FromJSON DataFrame;