package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLower;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestRestrictUserCodeSemanticAnalyzerHook extends TestAbstractSemanticAnalyzerHook {
  protected static final String TABLE3 = TABLE_PREFIX + "_T3";
  @Before
  public void setUp() throws Exception {
    super.setUp(RestrictUserCodeSemanticAnalyzerHook.class.getName());
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }

  @Test
  public void testUnsupportedOperations() throws Exception {
    ArrayList<String> statements = new ArrayList<String>();
    statements.add("CREATE TEMPORARY FUNCTION customlowerudf AS '" + GenericUDFLower.class.getName() + "'");

    executeStatements(40000, statements);
  }

  @Test
  public void testSupportedOperations() throws Exception {
    ArrayList<String> statements = new ArrayList<String>();

    // Valid metadata queries for MSTR support.
    statements.add("EXPLAIN DROP TABLE IF EXISTS TMPTBL");
    statements.add("DROP TABLE IF EXISTS TMPTBL");
    statements.add("CREATE TABLE " + TABLE3 + " LIKE " + TABLE1);
    statements.add("DROP TABLE IF EXISTS " + TABLE3);
    statements.add("EXPLAIN CREATE TABLE " + TABLE3 + " AS SELECT * FROM " + TABLE1);
    statements.add("CREATE TABLE " + TABLE3 + " AS SELECT * FROM " + TABLE1);
    statements.add("DROP TABLE IF EXISTS " + TABLE3);

    // Valid read-only operations for ODBC support.
    statements.add("USE default");
    statements.add("SHOW DATABASES");
    statements.add("SHOW TABLES");
    statements.add("SHOW CREATE TABLE " + TABLE1);
    statements.add("SHOW PARTITIONS " + TABLE2);
    statements.add("SHOW COLUMNS FROM default." + TABLE2);
    statements.add("DESCRIBE " + TABLE1);
    statements.add("DESC " + TABLE2);
    statements.add("DESCRIBE DATABASE default");
    statements.add("SHOW TBLPROPERTIES " + TABLE2);
    statements.add("SHOW FUNCTIONS");

    // Valid select queries.
    statements.add("SELECT * FROM " + TABLE1);
    statements.add("SELECT * FROM " + TABLE1 + " JOIN " + TABLE2);
    statements.add("EXPLAIN SELECT * FROM " + TABLE1 + " JOIN " + TABLE2);
    statements.add("SELECT * FROM (SELECT * FROM " + TABLE1 + ") T JOIN " + TABLE2);
    statements.add("SELECT * FROM (SELECT * FROM " + TABLE1 + ") T JOIN " + TABLE2 + " ON (T.ID = " + TABLE2 + ".ID)");
    statements.add("EXPLAIN SELECT * FROM (SELECT * FROM " + TABLE1 + ") T JOIN " + TABLE2 + " ON (T.ID = " + TABLE2 + ".ID)");
    statements.add("SELECT * FROM " + TABLE1 + " JOIN " + TABLE2 + " ON (" + TABLE1 + ".ID = " + TABLE2 + ".ID) ORDER BY " + TABLE1 + ".ID");
    statements.add("EXPLAIN SELECT UPPER(" + TABLE1 + ".NAME), " + TABLE2 + ".NAME, " + TABLE1 + ".ID FROM " + TABLE1 + " JOIN " + TABLE2 + " ON (" + TABLE1 + ".ID = " + TABLE2 + ".ID) ORDER BY " + TABLE1 + ".ID");
    statements.add("FROM " + TABLE1 + " SELECT *");
    statements.add("EXPLAIN FROM " + TABLE1 + " SELECT *");

    statements.add("DROP DATABASE IF EXISTS TMPDB CASCADE");
    statements.add("CREATE DATABASE TMPDB");
    statements.add("DROP DATABASE IF EXISTS TMPDB CASCADE");

    statements.add("INSERT OVERWRITE TABLE " + TABLE2 + " PARTITION(ID='1') SELECT NAME FROM " + TABLE1);
    statements.add("FROM " + TABLE1 + " INSERT OVERWRITE TABLE " + TABLE2 + " PARTITION (ID='0') SELECT NAME");

    statements.add("CREATE VIEW V AS SELECT * FROM " + TABLE1);
    statements.add("DROP VIEW IF EXISTS V");

    executeStatements(0, statements);
  }
}
