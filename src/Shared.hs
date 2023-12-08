{-# LANGUAGE DeriveGeneric #-}

module Shared 
  (
    webServerPort, 
    SqlRequest (..),
    SqlErrorResponse (..),
    SqlSuccessResponse (..)
  ) where

import Data.Aeson
import GHC.Generics
import DataFrame (DataFrame)

webServerPort :: Int
webServerPort = 5000

data SqlRequest = SqlRequest {
  query :: String
} deriving (Generic)

data SqlErrorResponse = SqlErrorResponse {
  errorMessage :: String
} deriving (Generic)

data SqlSuccessResponse = SqlSuccessResponse {
  dataFrame :: DataFrame
} deriving (Generic)

instance ToJSON SqlRequest
instance ToJSON SqlErrorResponse
instance ToJSON SqlSuccessResponse
instance FromJSON SqlRequest
instance FromJSON SqlErrorResponse
instance FromJSON SqlSuccessResponse