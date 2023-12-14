import Data.Either
import Data.Maybe ()
import InMemoryTables qualified as D
import Lib1
import Lib2
import Lib3
import Parser
import Test.Hspec
import DataFrame
import Control.Monad.Free (Free (..))
import Data.Time ( UTCTime, getCurrentTime )
import Data.IORef
import Lib2 (Value(NullValue))

type DBMock = [(String, IORef String)]

setupDB :: IO DBMock
setupDB = do
  employeesRef <- newIORef "tableName: employees\n\
                           \columns:\n\
                           \- name: id\n\
                           \  dataType: integer\n\
                           \- name: name\n\
                           \  dataType: string\n\
                           \- name: surname\n\
                           \  dataType: string\n\
                           \rows:\n\
                           \- [1, Vi, Po]\n\
                           \- [2, Ed, Dl]\n"
  peopleRef <- newIORef "tableName: people\n\
                           \columns:\n\
                           \- name: id\n\
                           \  dataType: integer\n\
                           \- name: name\n\
                           \  dataType: string\n\
                           \- name: surname\n\
                           \  dataType: string\n\
                           \rows:\n\
                           \- [1, Vi, Po]\n\
                           \- [2, Ja, Ne]\n\
                           \- [3, Jo, Nas]\n\
                           \- [4, Jo, Po]\n\
                           \- [5, Ed, Dl]\n"
  animalsRef <- newIORef "tableName: animals\n\
                           \columns:\n\
                           \- name: id\n\
                           \  dataType: integer\n\
                           \- name: animal\n\
                           \  dataType: string\n\
                           \- name: masterName\n\
                           \  dataType: string\n\
                           \- name: masterSurname\n\
                           \  dataType: string\n\
                           \rows:\n\
                           \- [1, Cat, Ja, Ne]\n\
                           \- [2, Dog, Jo, Nas]\n"
  return [("employees", employeesRef),("people", peopleRef),("animals", animalsRef)]

runExecuteIO :: DBMock -> IO UTCTime -> Lib3.Execution r -> IO r
runExecuteIO dbMock getCurrentTime' (Pure r) = return r
runExecuteIO dbMock getCurrentTime' (Free step) = do
    (newDbMock, next) <- runStep dbMock getCurrentTime' step
    runExecuteIO newDbMock getCurrentTime' next
  where
    runStep :: DBMock -> IO UTCTime -> Lib3.ExecutionAlgebra a -> IO (DBMock, a)
    runStep db getCurrentTime' (Lib3.GetTime next) = 
        getCurrentTime' >>= \time -> return (db, next time)

    runStep db getCurrentTime' (Lib3.LoadTable tableName next) =
        case lookup tableName db of
            Just ref -> readIORef ref >>= \content -> return (db, next $ Lib3.parseTable content)
            Nothing -> return (db, next $ Left $ "Table '" ++ tableName ++ "' does not exist.")

    runStep db getCurrentTime' (Lib3.SaveTable (tableName, tableContent) next) =
        case lookup tableName db of
            Just ref -> case Lib3.serializeTable (tableName, tableContent) of
                Left err -> error err  
                Right serializedTable -> do
                    writeIORef ref serializedTable
                    return (db, next ())
            Nothing -> return (db, next ())  

    runStep db getCurrentTime' (Lib3.GetTableNames next) = 
        return (db, next $ map fst db)

getValueByKey :: Eq a => [(a, b)] -> a -> b
getValueByKey [] _ = error "Key not found"
getValueByKey ((k, v):xs) key
  | key == k = v
  | otherwise = getValueByKey xs key

mockGetCurrentTime :: IO UTCTime
mockGetCurrentTime = return $ read "1410-07-15 11:00:00 UTC"

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
      Lib2.parseStatement "SHOW TABLE Users;" `shouldBe` Right (Lib2.ShowTableStatement "Users")
    it "parses show table statement with lowercase alphanum and underscore table name" $ do
      Lib2.parseStatement "show table _organization_123" `shouldBe` Right (Lib2.ShowTableStatement "_organization_123")
    it "parses show table statement with mixed casing and whitespace" $ do
      Lib2.parseStatement "   ShOW   taBlE    Hello_WORLD   ;  " `shouldBe` Right (Lib2.ShowTableStatement "Hello_WORLD")
    it "parses show tables statement with uppercase" $ do
      Lib2.parseStatement "SHOW TABLES;" `shouldBe` Right Lib2.ShowTablesStatement
    it "handles whitespace error in show tables statement" $ do
      Lib2.parseStatement "ShowTables;" `shouldSatisfy` isLeft
    it "handles unexpected symbols after end of the show tables statement" $ do
      Lib2.parseStatement "show tables;a" `shouldSatisfy` isLeft
    it "parses show tables statement with mixed casing" $ do
      Lib2.parseStatement "ShOW taBLeS;" `shouldBe` Right Lib2.ShowTablesStatement
    it "parses show tables statement with whitespaces" $ do
      Lib2.parseStatement "show      tables    ;  " `shouldBe` Right Lib2.ShowTablesStatement
    it "parses show tables statement lowercase" $ do
      Lib2.parseStatement "show tables;" `shouldBe` Right Lib2.ShowTablesStatement
    it "parses show tables statement Uppercase" $ do
      Lib2.parseStatement "SHOW TABLES;" `shouldBe` Right Lib2.ShowTablesStatement
    it "parses show tables statement with mixed casing and whitespace" $ do
      Lib2.parseStatement "   ShOW   taBLeS    ;  " `shouldBe` Right Lib2.ShowTablesStatement
    it "handles invalid statement" $ do
      Lib2.parseStatement "shw tables;" `shouldSatisfy` isLeft
    it "handles invalid SELECT statement" $ do
      Lib2.parseStatement "SELECT id name FROM employees;" `shouldSatisfy` isLeft
    it "handles basic SELECT statement" $ do
      Lib2.parseStatement "SELECT a, b FROM table;" `shouldBe` Right (Lib2.SelectStatement {Lib2.table = "table", Lib2.query = [Lib2.SelectColumn "a",Lib2.SelectColumn "b"], Lib2.whereClause = Nothing})
    it "handles basic SELECT statement with columns without whitespace separator" $ do
      Lib2.parseStatement "SELECT a,b FROM table;" `shouldBe` Right (Lib2.SelectStatement {Lib2.table = "table", Lib2.query = [Lib2.SelectColumn "a",Lib2.SelectColumn "b"], Lib2.whereClause = Nothing})
    it "handles basic SELECT statement with columns without  separator" $ do
      Lib2.parseStatement "SELECT a b FROM table;" `shouldSatisfy` isLeft
    it "handles basic SELECT statement with multiple aggregates" $ do
      Lib2.parseStatement "SELECT MIN(a), SUM(b) FROM table;" `shouldBe` Right (Lib2.SelectStatement {Lib2.table = "table", Lib2.query = [Lib2.SelectAggregate (Lib2.Aggregate Lib2.Min "a"), Lib2.SelectAggregate (Lib2.Aggregate Lib2.Sum "b")], Lib2.whereClause = Nothing})
    it "handles SELECT statement that mixes columns and aggregates" $ do
      Lib2.parseStatement "SELECT MIN(a), b FROM table;" `shouldSatisfy` isLeft
    it "handles SELECT statement with multiple WHERE criterion that compare columns" $ do
      Lib2.parseStatement "SELECT a, b, c, d FROM table WHERE a=b AND b!=c AND c>d AND d<e AND a>=b AND b<=c;" `shouldBe` Right (Lib2.SelectStatement {Lib2.table = "table", Lib2.query = [Lib2.SelectColumn "a",Lib2.SelectColumn "b",Lib2.SelectColumn "c",Lib2.SelectColumn "d"], Lib2.whereClause = Just [(Lib2.WhereCriterion (Lib2.ColumnExpression "a") RelEQ (Lib2.ColumnExpression "b"),Just Lib2.And),(Lib2.WhereCriterion (Lib2.ColumnExpression "b") RelNE (Lib2.ColumnExpression "c"),Just Lib2.And),(Lib2.WhereCriterion (Lib2.ColumnExpression "c") RelGT (Lib2.ColumnExpression "d"),Just Lib2.And),(Lib2.WhereCriterion (Lib2.ColumnExpression "d") RelLT (Lib2.ColumnExpression "e"),Just Lib2.And),(Lib2.WhereCriterion (Lib2.ColumnExpression "a") RelGE (Lib2.ColumnExpression "b"),Just Lib2.And),(Lib2.WhereCriterion (Lib2.ColumnExpression "b") RelLE (Lib2.ColumnExpression "c"),Nothing)]})
    it "handles SELECT statement with multiple WHERE criterion that compare strings" $ do
        Lib2.parseStatement "SELECT a FROM table WHERE 'aa'='aa' AND 'a'!='b' 'b'<'c';" `shouldBe` Right (Lib2.SelectStatement {Lib2.table = "table", Lib2.query = [Lib2.SelectColumn "a"], Lib2.whereClause = Just [(Lib2.WhereCriterion (Lib2.ValueExpression (StringValue
        "aa")) RelEQ (Lib2.ValueExpression (StringValue "aa")),Just Lib2.And),(Lib2.WhereCriterion (Lib2.ValueExpression (StringValue "a")) RelNE (Lib2.ValueExpression
        (StringValue "b")),Nothing),(Lib2.WhereCriterion (Lib2.ValueExpression (StringValue "b")) RelLT (Lib2.ValueExpression (StringValue "c")),Nothing)]})
    it "handles SELECT statement with multiple WHERE criterion that compare strings and columns" $ do
      Lib2.parseStatement "SELECT a FROM table WHERE a='aa' AND aaa!='b' AND 'b'<aaa;" `shouldBe` Right (Lib2.SelectStatement {Lib2.table = "table", Lib2.query = [Lib2.SelectColumn "a"], Lib2.whereClause = Just [(Lib2.WhereCriterion (Lib2.ColumnExpression "a") Parser.RelEQ (Lib2.ValueExpression (StringValue "aa")),Just Lib2.And),(Lib2.WhereCriterion (Lib2.ColumnExpression "aaa") RelNE (Lib2.ValueExpression (StringValue "b")),Just Lib2.And),(Lib2.WhereCriterion (Lib2.ValueExpression (StringValue "b")) RelLT (Lib2.ColumnExpression "aaa"),Nothing)]})
  describe "Lib2.executeStatement" $ do
    it "executes show tables statement" $ do
      Lib2.executeStatement Lib2.ShowTablesStatement `shouldSatisfy` isRight
    it "executes show table <name> statement" $ do
      Lib2.executeStatement Lib2.ShowTableStatement {Lib2.table = "employees"} `shouldSatisfy` isRight
    it "executes simple select statement" $ do
      Lib2.executeStatement Lib2.SelectStatement {Lib2.table = "employees", Lib2.query = [Lib2.SelectColumn "id"], Lib2.whereClause = Nothing} `shouldSatisfy` isRight
    it "executes select statement with an aggregate" $ do
      Lib2.executeStatement Lib2.SelectStatement {Lib2.table = "employees", Lib2.query = [Lib2.SelectAggregate (Lib2.Aggregate Lib2.Min "id")], Lib2.whereClause = Nothing} `shouldSatisfy` isRight
    it "executes select statement with a where clause" $ do
      Lib2.executeStatement Lib2.SelectStatement {Lib2.table = "employees", Lib2.query = [Lib2.SelectColumn "id"], Lib2.whereClause = Just [(Lib2.WhereCriterion (Lib2.ColumnExpression "name") RelEQ (Lib2.ValueExpression (StringValue "Ed")),Nothing)]} `shouldSatisfy` isRight
  describe "Lib3.parseTable" $ do
    it "parses valid table" $ do
      Lib3.parseTable "tableName: Organization_Employees\n\
                      \columns:\n\
                      \- name: id\n\
                      \  dataType: integer\n\
                      \- name: name\n\
                      \  dataType: string\n\
                      \- name: surname\n\
                      \  dataType: StrInG\n\
                      \- name: isFired\n\
                      \  dataType: bool\n\
                      \- name: startedAt\n\
                      \  dataType: datetime\n\
                      \rows:\n\
                      \- [1, Vi, \"Po\", false, 2023-09-23 16:29:24]\n\
                      \- [2, \"Ed\", Dl, NULL, 1993-09-23 12:00:00]\n\
                      \- [123, \"Lorem ipsum\", \"d,o\\\\l:o;r_s()i\n$t%a^m@e't\\\".\", True, 2025-01-14 10:29:24]\n\
                      \- [Null, \"<UNKNOWN>\", null, TRUE, 2023-02-14 10:29:24]"
                      `shouldBe` Right ("Organization_Employees", DataFrame [
                        Column "id" IntegerType, Column "name" StringType, Column "surname" StringType, Column "isFired" BoolType, Column "startedAt" DateTimeType] [
                        [IntegerValue 1, StringValue "Vi", StringValue "Po", BoolValue False, DateTimeValue "2023-09-23 16:29:24"],
                        [IntegerValue 2, StringValue "Ed", StringValue "Dl", NullValue, DateTimeValue "1993-09-23 12:00:00"],
                        [IntegerValue 123, StringValue "Lorem ipsum", StringValue "d,o\\l:o;r_s()i $t%a^m@e't\".", BoolValue True, DateTimeValue "2025-01-14 10:29:24"],
                        [NullValue, StringValue "<UNKNOWN>", NullValue, BoolValue True, DateTimeValue "2023-02-14 10:29:24"]])
    it "parses table with no rows" $ do
      Lib3.parseTable "tableName: NoRows\n\
                      \columns:\n\
                      \- name: Id\n\
                      \  dataType: integer\n\
                      \- name: Name\n\
                      \  dataType: string\n\
                      \rows: []"
                      `shouldBe` Right ("NoRows", DataFrame [Column "Id" IntegerType, Column "Name" StringType] [])
    it "parses table with no columns" $ do
      Lib3.parseTable "tableName: NoCols_123\n\
                      \columns: []\n\
                      \rows: []"
                      `shouldBe` Right ("NoCols_123", DataFrame [] [])
    it "handles unknown data type" $ do
      Lib3.parseTable "tableName: NoRows\n\
                      \columns:\n\
                      \- name: Id\n\
                      \  dataType: number\n\
                      \rows: []"
                      `shouldSatisfy` isLeft
    it "handles non-integer numbers" $ do
      Lib3.parseTable "tableName: Decimals\n\
                      \columns:\n\
                      \- name: Id\n\
                      \  dataType: integer\n\
                      \rows:\n\
                      \- [1.1]"
                      `shouldSatisfy` isLeft
    it "handles data type mismatch" $ do
      Lib3.parseTable "tableName: Mismatch\n\
                      \columns:\n\
                      \- name: Id\n\
                      \  dataType: integer\n\
                      \rows:\n\
                      \- [test]"
                      `shouldSatisfy` isLeft
    it "handles missing value in row" $ do
      Lib3.parseTable "tableName: Missing_Value\n\
                      \columns:\n\
                      \- name: Id\n\
                      \  dataType: integer\n\
                      \- name: Name\n\
                      \  dataType: string\n\
                      \rows:\n\
                      \- [1, John]\n\
                      \- [2]\n\
                      \- [3, Eric]"
                      `shouldSatisfy` isLeft
    it "handles rows with too many values" $ do
      Lib3.parseTable "tableName: Too_Many\n\
                      \columns:\n\
                      \- name: Id\n\
                      \  dataType: integer\n\
                      \- name: Name\n\
                      \  dataType: string\n\
                      \rows:\n\
                      \- [1, John]\n\
                      \- [2, Just]\n\
                      \- [3, Eric, Doe]"
                      `shouldSatisfy` isLeft
  describe "Lib3.serializeTable" $ do
    it "serializes valid table" $ do
      Lib3.serializeTable ("Organization_Employees",
        DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType, Column "isFired" BoolType, Column "startedAt" DateTimeType] [
                  [IntegerValue 1, StringValue "Vi", StringValue "Po", BoolValue False, DateTimeValue "2023-09-23 16:29:24"],
                  [IntegerValue 2, StringValue "Ed", StringValue "Dl", NullValue, DateTimeValue "1993-09-23 12:00:00"],
                  [IntegerValue 123, StringValue "Lorem ipsum", StringValue "d,o\\l:o;r_s()i\n$t%a^m@e't\".", BoolValue True, DateTimeValue "2025-01-14 10:29:24"],
                  [NullValue, StringValue "<UNKNOWN>", NullValue, BoolValue True, DateTimeValue "2023-02-14 10:29:24"]])
        `shouldBe` Right "tableName: \"Organization_Employees\"\n\
                         \columns:\n\
                         \- name: id\n\
                         \  dataType: integer\n\
                         \- name: name\n\
                         \  dataType: string\n\
                         \- name: surname\n\
                         \  dataType: string\n\
                         \- name: isFired\n\
                         \  dataType: bool\n\
                         \- name: startedAt\n\
                         \  dataType: datetime\n\
                         \rows:\n\
                         \- [1, \"Vi\", \"Po\", False, \"2023-09-23 16:29:24\"]\n\
                         \- [2, \"Ed\", \"Dl\", null, \"1993-09-23 12:00:00\"]\n\
                         \- [123, \"Lorem ipsum\", \"d,o\\l:o;r_s()i\n$t%a^m@e't\".\", True, \"2025-01-14 10:29:24\"]\n\
                         \- [null, \"<UNKNOWN>\", null, True, \"2023-02-14 10:29:24\"]\n"
    it "serializes table with no rows" $ do
      Lib3.serializeTable ("NoRows", DataFrame [Column "Id" IntegerType, Column "Name" StringType] [])
      `shouldBe` Right "tableName: \"NoRows\"\n\
                       \columns:\n\
                       \- name: Id\n\
                       \  dataType: integer\n\
                       \- name: Name\n\
                       \  dataType: string\n\
                       \rows: []\n"
    it "serializes table with no columns" $ do
      Lib3.serializeTable ("NoCols_123", DataFrame [] [])
      `shouldBe` Right "tableName: \"NoCols_123\"\n\
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
    it "parser handles SELECT columns with specified tables" $ do
      Lib3.parseStatement "SELECT table1.id, table2.id FROM table1, table2;"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2"], Lib3.query = [Lib3.SelectColumn "id" (Just "table1"),Lib3.SelectColumn "id" (Just "table2")], Lib3.whereClause = Nothing})
    it "parser handles SELECT aggregates with specified tables" $ do
      Lib3.parseStatement "SELECT MIN(table1.id), SUM(table2.age) FROM table1, table2;"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2"], Lib3.query = [Lib3.SelectAggregate (Lib3.Aggregate Lib3.Min "id") (Just "table1"),Lib3.SelectAggregate (Lib3.Aggregate Lib3.Sum "age") (Just "table2")], Lib3.whereClause = Nothing})
    it "parser handles SELECT NOW() system function" $ do
      Lib3.parseStatement "SELECT NOW();"
      `shouldBe` Right (Lib3.SystemFunctionStatement {Lib3.function = Now})
    it "parser handles system function mixed with SELECT columns" $ do
      Lib3.parseStatement "SELECT table1.id, table2.age, NOW() FROM table1, table2;"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2"], Lib3.query = [Lib3.SelectColumn "id" (Just "table1"),Lib3.SelectColumn "age" (Just "table2"),Lib3.SelectSystemFunction Now], Lib3.whereClause = Nothing})
    it "parser handles system function mixed with SELECT aggregates" $ do
      Lib3.parseStatement "SELECT NOW(), MIN(table1.id), SUM(table2.age), MIN(table3.name) FROM table1, table2, table3;"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2","table3"], Lib3.query = [Lib3.SelectSystemFunction Now,Lib3.SelectAggregate (Lib3.Aggregate Lib3.Min "id") (Just "table1"),Lib3.SelectAggregate (Lib3.Aggregate Lib3.Sum "age") (Just "table2"),Lib3.SelectAggregate (Lib3.Aggregate Lib3.Min "name") (Just "table3")], Lib3.whereClause = Nothing})
    it "parser handles unspecified tables" $ do
      Lib3.parseStatement "SELECT id, age FROM table1, table2;"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2"], Lib3.query = [Lib3.SelectColumn "id" Nothing,Lib3.SelectColumn "age" Nothing], Lib3.whereClause = Nothing})
    it "parser handles specified table names in WHERE clause" $ do
      Lib3.parseStatement "SELECT table1.id, table2.age FROM table1, table2 WHERE table1.name=table2.name;"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2"], Lib3.query = [Lib3.SelectColumn "id" (Just "table1"),Lib3.SelectColumn "age" (Just "table2")], Lib3.whereClause = Just [(Lib3.WhereCriterion (Lib3.ColumnExpression "name" (Just "table1")) RelEQ (Lib3.ColumnExpression "name" (Just "table2")),Nothing)]})
    it "parser handles specified and unspecified tables in WHERE clause" $ do
      Lib3.parseStatement "SELECT table1.id, table2.age FROM table1, table2 WHERE identification=table2.name AND name != 'Jane';"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2"], Lib3.query = [Lib3.SelectColumn "id" (Just "table1"),Lib3.SelectColumn "age" (Just "table2")], Lib3.whereClause = Just [(Lib3.WhereCriterion (Lib3.ColumnExpression "identification" Nothing) RelEQ (Lib3.ColumnExpression "name" (Just "table2")),Just Parser.And),(Lib3.WhereCriterion (Lib3.ColumnExpression "name" Nothing) RelNE (Lib3.ValueExpression (StringValue "Jane")),Nothing)]})
    it "basic select to check io " $ do
      db <- setupDB
      res <- runExecuteIO db getCurrentTime $ Lib3.executeSql "SELECT id FROM employees;"
      res `shouldBe` Right (DataFrame [Column "employees.id" IntegerType] [[IntegerValue 1],[IntegerValue 2]])
    it "SELECT for multiple tables with matching rows from where clause" $ do
        db <- setupDB
        res <- runExecuteIO db getCurrentTime $ Lib3.executeSql "SELECT id, name, surname FROM employees, people WHERE employees.name=people.name AND employees.surname=people.surname;"
        res `shouldBe` Right (DataFrame [Column "employees.id" IntegerType, Column "employees.name" StringType, Column "employees.surname" StringType] [[IntegerValue 1,StringValue "Vi",StringValue "Po"],[IntegerValue 2,StringValue "Ed",StringValue "Dl"]])
    it "SELECT from table that does not exist" $ do
        db <- setupDB
        res <- runExecuteIO db getCurrentTime $ Lib3.executeSql "SELECT id FROM table;"
        res `shouldBe` Left "Table 'table' does not exist."
    it "SELECT handles unspecified table names from multiple tables" $ do
        db <- setupDB
        res <- runExecuteIO db getCurrentTime $ Lib3.executeSql "SELECT name, surname, animal FROM people, animals WHERE name=masterName AND surname=masterSurname;"
        res `shouldBe` Right (DataFrame [Column "people.name" StringType, Column "people.surname" StringType, Column "animals.animal" StringType] [[StringValue "Ja",StringValue "Ne",StringValue "Cat"],[StringValue "Jo",StringValue "Nas",StringValue "Dog"]])
    it "SELECT handles aggregates from multiple tables" $ do
        db <- setupDB
        res <- runExecuteIO db getCurrentTime $ Lib3.executeSql "SELECT SUM(people.id), MIN(masterName) FROM people, animals ;"
        res `shouldBe` Right (DataFrame [Column "Sum(people.id)" IntegerType, Column "Min(animals.masterName)" StringType] [[IntegerValue 30,StringValue "Ja"]])
    it "SELECT handles NOW function" $ do
      db <- setupDB
      res <- runExecuteIO db mockGetCurrentTime $ Lib3.executeSql "SELECT NOW();"
      res `shouldBe` Right (DataFrame [Column "NOW()" DateTimeType] [[DateTimeValue "1410-07-15 11:00:00"]])
    it "SELECT handles NOW function with columns" $ do
      db <- setupDB
      res <- runExecuteIO db mockGetCurrentTime $ Lib3.executeSql "SELECT id, name, NOW() FROM employees;"
      res `shouldBe` Right (DataFrame [Column "employees.id" IntegerType,Column "employees.name" StringType,Column "NOW()" DateTimeType] [[IntegerValue 1,StringValue "Vi",DateTimeValue "1410-07-15 11:00:00"],[IntegerValue 2,StringValue "Ed",DateTimeValue "1410-07-15 11:00:00"]])
    it "SELECT handles NOW function with aggregates" $ do
      db <- setupDB
      res <- runExecuteIO db mockGetCurrentTime $ Lib3.executeSql "SELECT SUM(id), MIN(name), NOW() FROM employees;"
      res `shouldBe` Right (DataFrame [Column "Sum(employees.id)" IntegerType,Column "Min(employees.name)" StringType,Column "NOW()" DateTimeType] [[IntegerValue 3,StringValue "Ed",DateTimeValue "1410-07-15 11:00:00"]])
    it "parses a simple insert statement (uppercase)" $ do
      let input = "INSERT INTO myTable VALUES (1, 'John', true);"
      Lib3.parseStatement input `shouldBe` Right (Lib3.InsertStatement "myTable" Nothing [IntegerValue 1, StringValue "John", BoolValue True])
    it "parses an insert statement with specified columns" $ do
      let input = "INSERT INTO myTable (id, name) VALUES (1, 'John');"
      Lib3.parseStatement input `shouldBe` Right (Lib3.InsertStatement "myTable" (Just ["id", "name"]) [IntegerValue 1, StringValue "John"])
    it "handles mismatched column count and values" $ do
      let input = "INSERT INTO myTable (id, name) VALUES (1, 'John', true);"
      Lib3.parseStatement input `shouldSatisfy` isLeft
    it "parses a simple insert statement (mixed case)" $ do
      let input = "InSeRt InTo myTable Values (1, 'John', True);"
      Lib3.parseStatement input `shouldBe` Right (Lib3.InsertStatement "myTable" Nothing [IntegerValue 1, StringValue "John", BoolValue True])
    it "parses a simple insert statement (lowercase)" $ do
      let input = "insert into myTable VALUES (1, 'John', true);"
      Lib3.parseStatement input `shouldBe` Right (Lib3.InsertStatement "myTable" Nothing [IntegerValue 1, StringValue "John", BoolValue True])
    it "handles string value without single quotes" $ do
      let input = "INSERT INTO myTable (id, name) VALUES (1, John);"
      Lib3.parseStatement input `shouldSatisfy` isLeft
    it "parses insert statement without semicolon" $ do
      let input = "INSERT INTO myTable (id, name) VALUES (1, 'John')"
      Lib3.parseStatement input `shouldBe` Right (Lib3.InsertStatement "myTable" (Just ["id", "name"]) [IntegerValue 1, StringValue "John"])
    it "handles missed keyword in insert statement" $ do
      let input = "INSERT myTable (id, name, flag) VALUES (1, 'John', true);"
      Lib3.parseStatement input `shouldSatisfy` isLeft
    it "handles missed tablename in insert statement" $ do
      let input = "INSERT INTO (id, name, flag) VALUES (1, 'John', true);"
      Lib3.parseStatement input `shouldSatisfy` isLeft
    it "handles missed values in the brackets in insert statement" $ do
      let input = "INSERT myTable (id, name, flag) VALUES ();"
      Lib3.parseStatement input `shouldSatisfy` isLeft
    it "parses statement with excessive white spaces between statements' parts" $ do
      let input = "  INSERT  INTO    myTable(id,name)   VALUES(1,'John')  ;   "
      Lib3.parseStatement input `shouldBe` Right (Lib3.InsertStatement "myTable" (Just ["id", "name"]) [IntegerValue 1, StringValue "John"])
    it "handles extra letters between semicolon and end of input in insert statement" $ do
      let input = "INSERT INTO myTable (id, name) VALUES (1, 'John'); extra_letters"
      Lib3.parseStatement input `shouldSatisfy` isLeft
    it "parses update statement with where(uppercase)" $ do
      let input = "UPDATE myTable SET column1 = 'value1', column2 = 42 WHERE column3 = 'condition';"
      Lib3.parseStatement input `shouldBe` Right (Lib3.UpdateStatement {Lib3.tableNameUpdate = "myTable", Lib3.updates = [("column1",StringValue "value1"),("column2",IntegerValue 42)], Lib3.whereConditions = Just [Condition "column3" RelEQ (StringValue "condition")]})
    it "parses update statement with where (lowercase)" $ do
      let input = "update myTable set column1 = 'value1', column2 = 42 WHERE column3 = 'condition';"
      Lib3.parseStatement input `shouldBe` Right (Lib3.UpdateStatement {Lib3.tableNameUpdate = "myTable", Lib3.updates = [("column1",StringValue "value1"),("column2",IntegerValue 42)], Lib3.whereConditions = Just [Condition "column3" RelEQ (StringValue "condition")]})
    it "parses update statement with where (mixedcase)" $ do
      let input = "UpdATE myTable sET column1 = 'value1', column2 = 42 WhErE column3 != 'condition';"
      Lib3.parseStatement input `shouldBe` Right (Lib3.UpdateStatement {Lib3.tableNameUpdate = "myTable", Lib3.updates = [("column1",StringValue "value1"),("column2",IntegerValue 42)], Lib3.whereConditions = Just [Condition "column3" RelNE (StringValue "condition")]})
    it "parses update statement without where (uppercase)" $ do
      let input = "UPDATE myTable SET column1 = 'value1', column2 = 42;"
      Lib3.parseStatement input `shouldBe` Right (Lib3.UpdateStatement {Lib3.tableNameUpdate = "myTable", Lib3.updates = [("column1",StringValue "value1"),("column2",IntegerValue 42)], Lib3.whereConditions = Nothing})
    it "parses update statement with excessive white spaces between statements' parts" $ do
      let input = "UPDATE     myTable     SET    column1   = 'value1', column2 = 42     WHERE   column3 <= 'condition';"
      Lib3.parseStatement input `shouldBe` Right (Lib3.UpdateStatement {Lib3.tableNameUpdate = "myTable", Lib3.updates = [("column1",StringValue "value1"),("column2",IntegerValue 42)], Lib3.whereConditions = Just [Condition "column3" RelLE (StringValue "condition")]})
    it "handles extra letters between semicolon and end of input in update statement" $ do
      let input = "UPDATE myTable SET column1 = 'value1', column2 = 42 WHERE column3 = 'condition'; extra_letters "
      Lib3.parseStatement input `shouldSatisfy` isLeft
    it "parses update statement without semicolon" $ do
      let input = "UPDATE myTable SET column1 = 'value1', column2 = 42 WHERE column3 = 'condition'"
      Lib3.parseStatement input `shouldBe` Right (Lib3.UpdateStatement {Lib3.tableNameUpdate = "myTable", Lib3.updates = [("column1",StringValue "value1"),("column2",IntegerValue 42)], Lib3.whereConditions = Just [Condition "column3" RelEQ (StringValue "condition")]})
    it "handles invalid value type (for example string without ' ') in update statement" $ do
      let input = "UPDATE myTable SET column1 = 'value1', column2 = invalid_type;"
      Lib3.parseStatement input `shouldSatisfy` isLeft
    it "handles update statement without 'SET' keyword" $ do
      let input = "UPDATE myTable column1 = 'value1' WHERE column2 = 'condition';"
      Lib3.parseStatement input `shouldSatisfy` isLeft
    it "handles update statement without tablename" $ do
      let input = "UPDATE SET column1 = 'value1' WHERE column2 = 'condition';"
      Lib3.parseStatement input `shouldSatisfy` isLeft
    it "handles update statement without value for the column keyword" $ do
      let input = "UPDATE myTable SET column1 =  WHERE column2 = 'condition';"
      Lib3.parseStatement input `shouldSatisfy` isLeft
    it "inserts data without specifying columns" $ do
      db <- setupDB
      res <- runExecuteIO db getCurrentTime $ Lib3.executeSql "INSERT INTO employees values(3,'name','surname');"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "Ed", StringValue "Dl"], [IntegerValue 3, StringValue "name", StringValue "surname"]])
    it "inserts data into specified columns" $ do
      db <- setupDB
      res <- runExecuteIO db getCurrentTime $ Lib3.executeSql "INSERT INTO employees(name) values('name');"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 1, StringValue "Vi", StringValue "Po"], [IntegerValue 2, StringValue "Ed", StringValue "Dl"], [NullValue, StringValue "name", NullValue]])
    it "updates data without a where clause" $ do
      db <- setupDB
      res <- runExecuteIO db getCurrentTime $ Lib3.executeSql "UPDATE employees SET id=0;"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 0, StringValue "Vi", StringValue "Po"], [IntegerValue 0, StringValue "Ed", StringValue "Dl"]])
    it "updates data with a where clause" $ do
      db <- setupDB
      res <- runExecuteIO db getCurrentTime $ Lib3.executeSql "UPDATE employees SET name='new' where id=1;"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 2, StringValue "Ed", StringValue "Dl"], [IntegerValue 1, StringValue "new", StringValue "Po"]])
    it "deletes all rows if there is no where clause" $ do
      db <- setupDB
      res <- runExecuteIO db getCurrentTime $ Lib3.executeSql "DELETE FROM employees;"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [])
    it "deletes data with a where clause" $ do
      db <- setupDB
      res <- runExecuteIO db getCurrentTime $ Lib3.executeSql "DELETE FROM employees where name='Vi'"
      res `shouldBe` Right (DataFrame [Column "id" IntegerType, Column "name" StringType, Column "surname" StringType] [[IntegerValue 2, StringValue "Ed", StringValue "Dl"]])
    it "parses drop table statement (lowercase)" $ do
      let input = "drop table tablename;"
      Parser.parseStatement input `shouldBe` Right (Parser.DropTableStatement { Parser.table = "tablename" })
    it "parses drop table statement with (mixed case)" $ do
      let input = "dRoP tAbLe tablename;"
      Parser.parseStatement input `shouldBe` Right (Parser.DropTableStatement { Parser.table = "tablename" })
    it "parses drop table statement with (uppercase)" $ do
      let input = "DROP TABLE tablename;"
      Parser.parseStatement input `shouldBe` Right (Parser.DropTableStatement { Parser.table = "tablename" })
    it "parses drop table statement with many whitespaces" $ do
      let input = "drop    table          tablename;"
      Parser.parseStatement input `shouldBe` Right (Parser.DropTableStatement { Parser.table = "tablename" })
    it "handles incorrect drop table statement with missed keyword" $ do
      let input = "drop tablename;"
      Parser.parseStatement input `shouldSatisfy` isLeft
    it "parses create table statement" $ do
      let input = "create table exampleTable (id int , name varchar , flag bool, holidays_from date );"
      Parser.parseStatement input `shouldBe` Right (Parser.CreateTableStatement {Parser.table = "exampleTable", Parser.newColumns = [Column "id" IntegerType,Column "name" StringType,Column "flag" BoolType,Column "holidays_from" DateTimeType]})
    it "handles incorrect create table statement with missed keyword" $ do
      let input = "create exampleTable (id int , name varchar , flag bool, holidays_from date );"
      Parser.parseStatement input `shouldSatisfy` isLeft
    it "handles incorrect create table statement with invalid datatype" $ do
      let input = "create table exampleTable (id int , name varchar , flag bol, holidays_from date );"
      Parser.parseStatement input `shouldSatisfy` isLeft