import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import Lib3
import Test.Hspec
import DataFrame
import Control.Monad.Free (Free (..))
import Data.Time ( UTCTime, getCurrentTime )
import Data.IORef

type DBMock = [(String, IORef String)]

-- setupDB :: IO DBMock
-- setupDB = do
--   employeesRef <- newIORef "tableName: employees\n\
--                            \columns:\n\
--                            \- name: id\n\
--                            \  dataType: integer\n\
--                            \- name: name\n\
--                            \  dataType: string\n\
--                            \- name: surname\n\
--                            \  dataType: string\n\
--                            \rows:\n\
--                            \- [1, Vi, Po]\n\
--                            \- [2, Ed, Dl]\n"
--   return [("employees", employeesRef)]

-- runExecuteIO :: DBMock -> Lib3.Execution r -> IO (DBMock, r)
-- runExecuteIO dbMock (Pure r) = return (dbMock, r)
-- runExecuteIO dbMock (Free step) = do
--   next <- runStep step
--   runExecuteIO dbMock next
--   where
--     runStep :: Lib3.ExecutionAlgebra a -> IO a
--     runStep (Lib3.GetTime next) =
--       getCurrentTime >>= return . next
--     runStep (Lib3.LoadFile tableName next) = do
--       readIORef (getValueByKey dbMock tableName) >>= return . next
--     runStep (Lib3.SaveFile tableName fileContent next) = do
--       writeIORef (getValueByKey dbMock tableName) fileContent >>= return . next

getValueByKey :: Eq a => [(a, b)] -> a -> b
getValueByKey [] _ = error "Key not found"
getValueByKey ((k, v):xs) key
  | key == k = v
  | otherwise = getValueByKey xs key

main :: IO ()
main = hspec $ do
  describe "Lib1.findTableByName" $ do
    it "handles empty lists" $ do
      Lib1.findTableByName [] "" `shouldBe` Nothing
    it "handles empty names" $ do
      Lib1.findTableByName D.database "" `shouldBe` Nothing
    it "can find by name" $ do
      Lib1.findTableByName D.database "employees" `shouldBe` Just (snd D.tableEmployees)
    it "can find by case-insensitive name" $ do
      Lib1.findTableByName D.database "employEEs" `shouldBe` Just (snd D.tableEmployees)
  describe "Lib1.parseSelectAllStatement" $ do
    it "handles empty input" $ do
      Lib1.parseSelectAllStatement "" `shouldSatisfy` isLeft
    it "handles invalid queries" $ do
      Lib1.parseSelectAllStatement "select from dual" `shouldSatisfy` isLeft
    it "returns table name from correct queries" $ do
      Lib1.parseSelectAllStatement "selecT * from dual;" `shouldBe` Right "dual"
  describe "Lib1.validateDataFrame" $ do
    it "finds types mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldSatisfy` isLeft
    it "finds column size mismatch" $ do
      Lib1.validateDataFrame (snd D.tableInvalid2) `shouldSatisfy` isLeft
    it "reports different error messages" $ do
      Lib1.validateDataFrame (snd D.tableInvalid1) `shouldNotBe` Lib1.validateDataFrame (snd D.tableInvalid2)
    it "passes valid tables" $ do
      Lib1.validateDataFrame (snd D.tableWithNulls) `shouldBe` Right ()
  describe "Lib1.renderDataFrameAsTable" $ do
    it "renders a table" $ do
      Lib1.renderDataFrameAsTable 100 (snd D.tableEmployees) `shouldSatisfy` not . null
  describe "Lib2.parseStatement" $ do
    it "handles empty input" $ do
      Lib2.parseStatement "" `shouldSatisfy` isLeft
    it "handles unexpected symbols after end of statement" $ do
      Lib2.parseStatement "SHOW TABLE Users;a" `shouldSatisfy` isLeft
    it "handles whitespace error" $ do
      Lib2.parseStatement "ShowTable Users;" `shouldSatisfy` isLeft
    it "handles show table without table name" $ do
      Lib2.parseStatement "SHOW TABLE" `shouldSatisfy` isLeft
    it "parses show table statement with uppercase" $ do
      Lib2.parseStatement "SHOW TABLE Users;" `shouldBe` Right (ShowTableStatement "Users")
    it "parses show table statement with lowercase alphanum and underscore table name" $ do
      Lib2.parseStatement "show table _organization_123" `shouldBe` Right (ShowTableStatement "_organization_123")
    it "parses show table statement with mixed casing and whitespace" $ do
      Lib2.parseStatement "   ShOW   taBlE    Hello_WORLD   ;  " `shouldBe` Right (ShowTableStatement "Hello_WORLD")
    it "parses show tables statement with uppercase" $ do
      Lib2.parseStatement "SHOW TABLES;" `shouldBe` Right ShowTablesStatement
    it "handles whitespace error in show tables statement" $ do
      Lib2.parseStatement "ShowTables;" `shouldSatisfy` isLeft
    it "handles unexpected symbols after end of the show tables statement" $ do
      Lib2.parseStatement "show tables;a" `shouldSatisfy` isLeft
    it "parses show tables statement with mixed casing" $ do
      Lib2.parseStatement "ShOW taBLeS;" `shouldBe` Right ShowTablesStatement
    it "parses show tables statement with whitespaces" $ do
      Lib2.parseStatement "show      tables    ;  " `shouldBe` Right ShowTablesStatement
    it "parses show tables statement lowercase" $ do
      Lib2.parseStatement "show tables;" `shouldBe` Right ShowTablesStatement
    it "parses show tables statement Uppercase" $ do
      Lib2.parseStatement "SHOW TABLES;" `shouldBe` Right ShowTablesStatement
    it "parses show tables statement with mixed casing and whitespace" $ do
      Lib2.parseStatement "   ShOW   taBLeS    ;  " `shouldBe` Right ShowTablesStatement
    it "handles invalid statement" $ do
      Lib2.parseStatement "shw tables;" `shouldSatisfy` isLeft
    it "handles invalid SELECT statement" $ do
      Lib2.parseStatement "SELECT id name FROM employees;" `shouldSatisfy` isLeft
    it "handles basic SELECT statement" $ do
      Lib2.parseStatement "SELECT a, b FROM table;" `shouldBe` Right (SelectStatement {table = "table", query = [SelectColumn "a",SelectColumn "b"], whereClause = Nothing})
    it "handles basic SELECT statement with columns without whitespace separator" $ do
      Lib2.parseStatement "SELECT a,b FROM table;" `shouldBe` Right (SelectStatement {table = "table", query = [SelectColumn "a",SelectColumn "b"], whereClause = Nothing})
    it "handles basic SELECT statement with columns without  separator" $ do
      Lib2.parseStatement "SELECT a b FROM table;" `shouldSatisfy` isLeft
    it "handles basic SELECT statement with multiple aggregates" $ do
      Lib2.parseStatement "SELECT MIN(a), SUM(b) FROM table;" `shouldBe` Right (SelectStatement {table = "table", query = [SelectAggregate (Aggregate Min "a"), SelectAggregate (Aggregate Sum "b")], whereClause = Nothing})
    it "handles SELECT statement that mixes columns and aggregates" $ do
      Lib2.parseStatement "SELECT MIN(a), b FROM table;" `shouldSatisfy` isLeft
    it "handles SELECT statement with multiple WHERE criterion that compare columns" $ do
      Lib2.parseStatement "SELECT a, b, c, d FROM table WHERE a=b AND b!=c AND c>d AND d<e AND a>=b AND b<=c;" `shouldBe` Right (SelectStatement {table = "table", query = [SelectColumn "a",SelectColumn "b",SelectColumn "c",SelectColumn "d"], whereClause = Just [(WhereCriterion (ColumnExpression "a") RelEQ (ColumnExpression "b"),Just And),(WhereCriterion (ColumnExpression "b") RelNE (ColumnExpression "c"),Just And),(WhereCriterion (ColumnExpression "c") RelGT (ColumnExpression "d"),Just And),(WhereCriterion (ColumnExpression "d") RelLT (ColumnExpression "e"),Just And),(WhereCriterion (ColumnExpression "a") RelGE (ColumnExpression "b"),Just And),(WhereCriterion (ColumnExpression "b") RelLE (ColumnExpression "c"),Nothing)]})
    it "handles SELECT statement with multiple WHERE criterion that compare strings" $ do
        Lib2.parseStatement "SELECT a FROM table WHERE 'aa'='aa' AND 'a'!='b' 'b'<'c';" `shouldBe` Right (SelectStatement {table = "table", query = [SelectColumn "a"], whereClause = Just [(WhereCriterion (ValueExpression (StringValue
        "aa")) RelEQ (ValueExpression (StringValue "aa")),Just And),(WhereCriterion (ValueExpression (StringValue "a")) RelNE (ValueExpression
        (StringValue "b")),Nothing),(WhereCriterion (ValueExpression (StringValue "b")) RelLT (ValueExpression (StringValue "c")),Nothing)]})
    it "handles SELECT statement with multiple WHERE criterion that compare strings and columns" $ do
      Lib2.parseStatement "SELECT a FROM table WHERE a='aa' AND aaa!='b' AND 'b'<aaa;" `shouldBe` Right (SelectStatement {table = "table", query = [SelectColumn "a"], whereClause = Just [(WhereCriterion (ColumnExpression "a") RelEQ (ValueExpression (StringValue "aa")),Just And),(WhereCriterion (ColumnExpression "aaa") RelNE (ValueExpression (StringValue "b")),Just And),(WhereCriterion (ValueExpression (StringValue "b")) RelLT (ColumnExpression "aaa"),Nothing)]})
  describe "Lib2.executeStatement" $ do
    it "executes show tables statement" $ do
      Lib2.executeStatement ShowTablesStatement `shouldSatisfy` isRight
    it "executes show table <name> statement" $ do
      Lib2.executeStatement ShowTableStatement {table = "employees"} `shouldSatisfy` isRight
    it "executes simple select statement" $ do
      Lib2.executeStatement SelectStatement {table = "employees", query = [SelectColumn "id"], whereClause = Nothing} `shouldSatisfy` isRight
    it "executes select statement with an aggregate" $ do
      Lib2.executeStatement SelectStatement {table = "employees", query = [SelectAggregate (Aggregate Min "id")], whereClause = Nothing} `shouldSatisfy` isRight
    it "executes select statement with a where clause" $ do
      Lib2.executeStatement SelectStatement {table = "employees", query = [SelectColumn "id"], whereClause = Just [(WhereCriterion (ColumnExpression "name") RelEQ (ValueExpression (StringValue "Ed")),Nothing)]} `shouldSatisfy` isRight
  describe "Lib3.parseTable" $ do
    it "parses valid table" $ do
      Lib3.parseTable "tableName: Organization_Employees  \n\
                      \columns:                           \n\
                      \- name: id                         \n\
                      \  dataType: integer                \n\
                      \- name: name                       \n\
                      \  dataType: string                 \n\
                      \- name: surname                    \n\
                      \  dataType: StrInG                 \n\
                      \- name: isFired                    \n\
                      \  dataType: bool                   \n\
                      \rows:                              \n\
                      \- [1, Vi, Po, false]               \n\
                      \- [2, Ed, Dl, NULL]                \n\
                      \- [Null, <UNKNOWN>, null, TRUE]"
                      `shouldBe` Right ("Organization_Employees", DataFrame
                        [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType, Column "isFired" BoolType] [
                        [IntegerValue 1, StringValue "Vi", StringValue "Po", BoolValue False],
                        [IntegerValue 2, StringValue "Ed", StringValue "Dl", NullValue],
                        [NullValue, StringValue "<UNKNOWN>", NullValue, BoolValue True]])
    it "parses table with no rows" $ do
      Lib3.parseTable "tableName: NoRows                  \n\
                      \columns:                           \n\
                      \- name: Id                         \n\
                      \  dataType: integer                \n\
                      \- name: Name                       \n\
                      \  dataType: string                 \n\
                      \rows: []"
                      `shouldBe` Right ("NoRows", DataFrame [Column "Id" IntegerType, Column "Name" StringType] [])
    it "parses table with no columns" $ do
      Lib3.parseTable "tableName: NoCols_123              \n\
                      \columns: []                        \n\
                      \rows: []"
                      `shouldBe` Right ("NoCols_123", DataFrame [] [])
    it "handles unknown data type" $ do
      Lib3.parseTable "tableName: NoRows                  \n\
                      \columns:                           \n\
                      \- name: Id                         \n\
                      \  dataType: number                 \n\
                      \rows: []"
                      `shouldSatisfy` isLeft
    it "handles non-integer numbers" $ do
      Lib3.parseTable "tableName: Decimals                \n\
                      \columns:                           \n\
                      \- name: Id                         \n\
                      \  dataType: integer                \n\
                      \rows:                              \n\
                      \- [1.1]"
                      `shouldSatisfy` isLeft
    it "handles data type mismatch" $ do
      Lib3.parseTable "tableName: Mismatch                \n\
                      \columns:                           \n\
                      \- name: Id                         \n\
                      \  dataType: integer                \n\
                      \rows:                              \n\
                      \- [test]"
                      `shouldSatisfy` isLeft
    it "handles missing value in row" $ do
      Lib3.parseTable "tableName: Missing_Value           \n\
                      \columns:                           \n\
                      \- name: Id                         \n\
                      \  dataType: integer                \n\
                      \- name: Name                       \n\
                      \  dataType: string                 \n\
                      \rows:                              \n\
                      \- [1, John]                        \n\
                      \- [2]                              \n\
                      \- [3, Eric]"
                      `shouldSatisfy` isLeft
    it "handles rows with too many values" $ do
      Lib3.parseTable "tableName: Too_Many                \n\
                      \columns:                           \n\
                      \- name: Id                         \n\
                      \  dataType: integer                \n\
                      \- name: Name                       \n\
                      \  dataType: string                 \n\
                      \rows:                              \n\
                      \- [1, John]                        \n\
                      \- [2, Just]                        \n\
                      \- [3, Eric, Doe]"
                      `shouldSatisfy` isLeft
  describe "Lib3.serializeTable" $ do
    it "serializes valid table" $ do
      Lib3.serializeTable ("Organization_Employees",
        DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType, Column "isFired" BoolType] [
                  [IntegerValue 1, StringValue "Vi", StringValue "Po", BoolValue False],
                  [IntegerValue 2, StringValue "Ed", StringValue "Dl", NullValue],
                  [NullValue, StringValue "<UNKNOWN>", NullValue, BoolValue True]])
        `shouldBe` Right "tableName: Organization_Employees\n\
                         \columns:\n\
                         \- name: id\n\
                         \  dataType: integer\n\
                         \- name: name\n\
                         \  dataType: string\n\
                         \- name: surname\n\
                         \  dataType: string\n\
                         \- name: isFired\n\
                         \  dataType: bool\n\
                         \rows:\n\
                         \- [1, Vi, Po, False]\n\
                         \- [2, Ed, Dl, null]\n\
                         \- [null, <UNKNOWN>, null, True]\n"
    it "serializes table with no rows" $ do
      Lib3.serializeTable ("NoRows", DataFrame [Column "Id" IntegerType, Column "Name" StringType] [])
      `shouldBe` Right "tableName: NoRows\n\
                       \columns:\n\
                       \- name: Id\n\
                       \  dataType: integer\n\
                       \- name: Name\n\
                       \  dataType: string\n\
                       \rows: []\n"
    it "serializes table with no columns" $ do
      Lib3.serializeTable ("NoCols_123", DataFrame [] [])
      `shouldBe` Right "tableName: NoCols_123\n\
                       \columns: []\n\
                       \rows: []\n"
    it "handles data type mismatch" $ do
      Lib3.serializeTable ("Mismatch", DataFrame [Column "Id" IntegerType] [[StringValue "test"]]) `shouldSatisfy` isLeft
    it "handles missing value in row" $ do
      Lib3.serializeTable ("Missing_Value", DataFrame
        [Column "Id" IntegerType, Column "Name" StringType] [
        [IntegerValue 1, StringValue "John"],
        [IntegerValue 2],
        [IntegerValue 3, StringValue "Eric"]])
        `shouldSatisfy` isLeft
    it "handles rows with too many values" $ do
      Lib3.serializeTable ("Too_Many", DataFrame
        [Column "Id" IntegerType, Column "Name" StringType] [
        [IntegerValue 1, StringValue "John"],
        [IntegerValue 2, StringValue "Just"],
        [IntegerValue 3, StringValue "Eric", StringValue "Doe"]])
        `shouldSatisfy` isLeft
    -- it "PLACEHOLDER TEST JUST FOR USAGE SHOWCASE" $ do
    --   db <- setupDB
    --   res <- runExecuteIO db $ Lib3.executeSql "my_sql"
    --   updatedDb <- readIORef $ getValueByKey (fst res) "employees"
