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

runExecuteIO :: DBMock -> Lib3.Execution r -> IO r
runExecuteIO dbMock (Pure r) = return r
runExecuteIO dbMock (Free step) = do
    (newDbMock, next) <- runStep dbMock step
    runExecuteIO newDbMock next
  where
    runStep :: DBMock -> Lib3.ExecutionAlgebra a -> IO (DBMock, a)
    runStep db (Lib3.GetTime next) = 
        getCurrentTime >>= \time -> return (db, next time)

    runStep db (Lib3.LoadTable tableName next) =
        case lookup tableName db of
            Just ref -> readIORef ref >>= \content -> return (db, next $ Lib3.parseTable content)
            Nothing -> return (db, next $ Left $ "Table '" ++ tableName ++ "' does not exist.")

    runStep db (Lib3.SaveTable (tableName, tableContent) next) =
        case lookup tableName db of
            Just ref -> case Lib3.serializeTable (tableName, tableContent) of
                Left err -> error err  
                Right serializedTable -> do
                    writeIORef ref serializedTable
                    return (db, next())
            Nothing -> return (db, next())  

    runStep db (Lib3.GetTableNames next) = 
        return (db, next $ map fst db)

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
      Lib2.parseStatement "SELECT MIN(a), SUM(b) FROM table;" `shouldBe` Right (Lib2.SelectStatement {Lib2.table = "table", Lib2.query = [Lib2.SelectAggregate (Lib2.Aggregate Min "a"), Lib2.SelectAggregate (Lib2.Aggregate Sum "b")], Lib2.whereClause = Nothing})
    it "handles SELECT statement that mixes columns and aggregates" $ do
      Lib2.parseStatement "SELECT MIN(a), b FROM table;" `shouldSatisfy` isLeft
    it "handles SELECT statement with multiple WHERE criterion that compare columns" $ do
      Lib2.parseStatement "SELECT a, b, c, d FROM table WHERE a=b AND b!=c AND c>d AND d<e AND a>=b AND b<=c;" `shouldBe` Right (Lib2.SelectStatement {Lib2.table = "table", Lib2.query = [Lib2.SelectColumn "a",Lib2.SelectColumn "b",Lib2.SelectColumn "c",Lib2.SelectColumn "d"], Lib2.whereClause = Just [(Lib2.WhereCriterion (Lib2.ColumnExpression "a") RelEQ (Lib2.ColumnExpression "b"),Just And),(Lib2.WhereCriterion (Lib2.ColumnExpression "b") RelNE (Lib2.ColumnExpression "c"),Just And),(Lib2.WhereCriterion (Lib2.ColumnExpression "c") RelGT (Lib2.ColumnExpression "d"),Just And),(Lib2.WhereCriterion (Lib2.ColumnExpression "d") RelLT (Lib2.ColumnExpression "e"),Just And),(Lib2.WhereCriterion (Lib2.ColumnExpression "a") RelGE (Lib2.ColumnExpression "b"),Just And),(Lib2.WhereCriterion (Lib2.ColumnExpression "b") RelLE (Lib2.ColumnExpression "c"),Nothing)]})
    it "handles SELECT statement with multiple WHERE criterion that compare strings" $ do
        Lib2.parseStatement "SELECT a FROM table WHERE 'aa'='aa' AND 'a'!='b' 'b'<'c';" `shouldBe` Right (Lib2.SelectStatement {Lib2.table = "table", Lib2.query = [Lib2.SelectColumn "a"], Lib2.whereClause = Just [(Lib2.WhereCriterion (Lib2.ValueExpression (StringValue
        "aa")) RelEQ (Lib2.ValueExpression (StringValue "aa")),Just And),(Lib2.WhereCriterion (Lib2.ValueExpression (StringValue "a")) RelNE (Lib2.ValueExpression
        (StringValue "b")),Nothing),(Lib2.WhereCriterion (Lib2.ValueExpression (StringValue "b")) RelLT (Lib2.ValueExpression (StringValue "c")),Nothing)]})
    it "handles SELECT statement with multiple WHERE criterion that compare strings and columns" $ do
      Lib2.parseStatement "SELECT a FROM table WHERE a='aa' AND aaa!='b' AND 'b'<aaa;" `shouldBe` Right (Lib2.SelectStatement {Lib2.table = "table", Lib2.query = [Lib2.SelectColumn "a"], Lib2.whereClause = Just [(Lib2.WhereCriterion (Lib2.ColumnExpression "a") Lib2.RelEQ (Lib2.ValueExpression (StringValue "aa")),Just And),(Lib2.WhereCriterion (Lib2.ColumnExpression "aaa") RelNE (Lib2.ValueExpression (StringValue "b")),Just And),(Lib2.WhereCriterion (Lib2.ValueExpression (StringValue "b")) RelLT (Lib2.ColumnExpression "aaa"),Nothing)]})
  describe "Lib2.executeStatement" $ do
    it "executes show tables statement" $ do
      Lib2.executeStatement Lib2.ShowTablesStatement `shouldSatisfy` isRight
    it "executes show table <name> statement" $ do
      Lib2.executeStatement Lib2.ShowTableStatement {Lib2.table = "employees"} `shouldSatisfy` isRight
    it "executes simple select statement" $ do
      Lib2.executeStatement Lib2.SelectStatement {Lib2.table = "employees", Lib2.query = [Lib2.SelectColumn "id"], Lib2.whereClause = Nothing} `shouldSatisfy` isRight
    it "executes select statement with an aggregate" $ do
      Lib2.executeStatement Lib2.SelectStatement {Lib2.table = "employees", Lib2.query = [Lib2.SelectAggregate (Lib2.Aggregate Min "id")], Lib2.whereClause = Nothing} `shouldSatisfy` isRight
    it "executes select statement with a where clause" $ do
      Lib2.executeStatement Lib2.SelectStatement {Lib2.table = "employees", Lib2.query = [Lib2.SelectColumn "id"], Lib2.whereClause = Just [(Lib2.WhereCriterion (Lib2.ColumnExpression "name") RelEQ (Lib2.ValueExpression (StringValue "Ed")),Nothing)]} `shouldSatisfy` isRight
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
    it "parser handles SELECT columns with specified tables" $ do
      Lib3.parseStatement "SELECT table1.id, table2.id FROM table1, table2;"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2"], Lib3.query = [Lib3.SelectColumn "id" (Just "table1"),Lib3.SelectColumn "id" (Just "table2")], Lib3.whereClause = Nothing})
    it "parser handles SELECT aggregates with specified tables" $ do
      Lib3.parseStatement "SELECT MIN(table1.id), SUM(table2.age) FROM table1, table2;"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2"], Lib3.query = [Lib3.SelectAggregate (Lib3.Aggregate Min "id") (Just "table1"),Lib3.SelectAggregate (Lib3.Aggregate Sum "age") (Just "table2")], Lib3.whereClause = Nothing})
    it "parser handles SELECT NOW() system function" $ do
      Lib3.parseStatement "SELECT NOW();"
      `shouldBe` Right (Lib3.SystemFunctionStatement {Lib3.function = Now})
    it "parser handles system function mixed with SELECT columns" $ do
      Lib3.parseStatement "SELECT table1.id, table2.age, NOW() FROM table1, table2;"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2"], Lib3.query = [Lib3.SelectColumn "id" (Just "table1"),Lib3.SelectColumn "age" (Just "table2"),Lib3.SelectSystemFunction Now], Lib3.whereClause = Nothing})
    it "parser handles system function mixed with SELECT aggregates" $ do
      Lib3.parseStatement "SELECT NOW(), MIN(table1.id), SUM(table2.age), MIN(table3.name) FROM table1, table2, table3;"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2","table3"], Lib3.query = [Lib3.SelectSystemFunction Now,Lib3.SelectAggregate (Lib3.Aggregate Min "id") (Just "table1"),Lib3.SelectAggregate (Lib3.Aggregate Sum "age") (Just "table2"),Lib3.SelectAggregate (Lib3.Aggregate Min "name") (Just "table3")], Lib3.whereClause = Nothing})
    it "parser handles unspecified tables" $ do
      Lib3.parseStatement "SELECT id, age FROM table1, table2;"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2"], Lib3.query = [Lib3.SelectColumn "id" Nothing,Lib3.SelectColumn "age" Nothing], Lib3.whereClause = Nothing})
    it "parser handles specified table names in WHERE clause" $ do
      Lib3.parseStatement "SELECT table1.id, table2.age FROM table1, table2 WHERE table1.name=table2.name;"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2"], Lib3.query = [Lib3.SelectColumn "id" (Just "table1"),Lib3.SelectColumn "age" (Just "table2")], Lib3.whereClause = Just [(Lib3.WhereCriterion (Lib3.ColumnExpression "name" (Just "table1")) RelEQ (Lib3.ColumnExpression "name" (Just "table2")),Nothing)]})
    it "parser handles specified and unspecified tables in WHERE clause" $ do
      Lib3.parseStatement "SELECT table1.id, table2.age FROM table1, table2 WHERE identification=table2.name AND name != 'Jane';"
      `shouldBe` Right (Lib3.SelectStatement {Lib3.tables = ["table1","table2"], Lib3.query = [Lib3.SelectColumn "id" (Just "table1"),Lib3.SelectColumn "age" (Just "table2")], Lib3.whereClause = Just [(Lib3.WhereCriterion (Lib3.ColumnExpression "identification" Nothing) RelEQ (Lib3.ColumnExpression "name" (Just "table2")),Just And),(Lib3.WhereCriterion (Lib3.ColumnExpression "name" Nothing) RelNE (Lib3.ValueExpression (StringValue "Jane")),Nothing)]})
    it "basic select to check io " $ do
      db <- setupDB
      res <- runExecuteIO db $ Lib3.executeSql "SELECT id FROM employees;"
      res `shouldBe` Right (DataFrame [Column "employees.id" IntegerType] [[IntegerValue 1],[IntegerValue 2]])
    it "SELECT without WHERE clause to show cartesian product is used" $ do
      db <- setupDB
      res <- runExecuteIO db $ Lib3.executeSql "SELECT id, name, surname FROM employees, employees;"
      res `shouldBe` Right (DataFrame [Column "employees.id" IntegerType,Column "employees.name" StringType,Column "employees.surname" StringType,Column "employees.id" IntegerType,Column "employees.name" StringType,Column "employees.surname" StringType] [[IntegerValue 1,StringValue "Vi",StringValue "Po"],[IntegerValue 1,StringValue "Vi",StringValue "Po"],[IntegerValue 2,StringValue "Ed",StringValue "Dl"],[IntegerValue 2,StringValue "Ed",StringValue "Dl"]])
    it "SELECT for multiple tables with matching rows from where clause" $ do
      db <- setupDB
      res <- runExecuteIO db $ Lib3.executeSql "SELECT id, name, surname FROM employees, people WHERE employees.name=people.name AND employees.surname=people.surname;"
      res `shouldBe` Right (DataFrame [Column "employees.id" IntegerType,Column "employees.name" StringType,Column "employees.surname" StringType] [[IntegerValue 1,StringValue "Vi",StringValue "Po"],[IntegerValue 2,StringValue "Ed",StringValue "Dl"]])
    it "SELECT from table that does not exist" $ do
      db <- setupDB
      res <- runExecuteIO db $ Lib3.executeSql "SELECT id FROM table;"
      res `shouldBe` Left "Table 'table' does not exist."
    it "SELECT handles unspecified table names from multiple tables" $ do
      db <- setupDB
      res <- runExecuteIO db $ Lib3.executeSql "SELECT name, surname, animal FROM people, animals WHERE name=masterName AND surname=masterSurname;"
      res `shouldBe` Right (DataFrame [Column "people.name" StringType,Column "people.surname" StringType,Column "animals.animal" StringType] [[StringValue "Ja",StringValue "Ne",StringValue "Cat"],[StringValue "Jo",StringValue "Nas",StringValue "Dog"]])
    it "SELECT handles aggregates from multiple tables" $ do
      db <- setupDB
      res <- runExecuteIO db $ Lib3.executeSql "SELECT SUM(people.id), MIN(masterName) FROM people, animals ;"
      res `shouldBe` Right (DataFrame [Column "Sum(people.id)" IntegerType,Column "Min(animals.masterName)" StringType] [[IntegerValue 30,StringValue "Ja"]])
    