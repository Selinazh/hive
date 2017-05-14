package org.apache.hadoop.hive.ql.parse;

import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;

import static junit.framework.Assert.assertEquals;

public abstract class TestAbstractSemanticAnalyzerHook {
  protected static final String TABLE_PREFIX = "semantic_hook";
  protected static final String TABLE1 = TABLE_PREFIX + "_T1";
  protected static final String TABLE2 = TABLE_PREFIX + "_T2";

  private HiveConf conf;
  private Driver systemDriver;
  private Driver testDriver;
  private SessionState testSession;

  protected void setUp(String hook) throws Exception {
    setUpSystemDriver();
    setupTestDriver(hook);
  }

  protected void tearDown() throws Exception {
    this.testDriver.close();

    this.systemDriver.run("DROP TABLE IF EXISTS " + TABLE2);
    this.systemDriver.run("DROP TABLE IF EXISTS " + TABLE1);
    this.systemDriver.close();

    this.testSession.close();
  }

  protected void executeStatements(int response, List<String> statements) throws Exception {
    for (String statement : statements) {
      System.out.println(statement);
      assertEquals(response, this.testDriver.run(statement, false).getResponseCode());
    }
  }

  private void setUpSystemDriver() throws Exception {
    // Set up the system driver.
    this.conf = new HiveConf(this.getClass());
    this.conf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    this.conf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    this.conf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    this.conf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, System.getProperty("test.warehouse.dir"));

    // Clean up the warehouse directory before running any tests.
    Path warehouse = new Path(HiveConf.getVar(this.conf, HiveConf.ConfVars.METASTOREWAREHOUSE));
    FileSystem fs = warehouse.getFileSystem(this.conf);
    if (fs.exists(warehouse)) {
      fs.delete(warehouse, true);
    }

    this.systemDriver = new Driver(this.conf);
    SessionState session = SessionState.start(this.conf);

    // Initialize the default tables.
    this.systemDriver.run("CREATE TABLE IF NOT EXISTS " + TABLE1 + " (NAME STRING, ID INT)");
    this.systemDriver.run("CREATE TABLE IF NOT EXISTS " + TABLE2 + " (NAME STRING) PARTITIONED BY (ID INT)");

    session.close();
  }

  private void setupTestDriver(String hook) throws Exception {
    // Set up the test driver with the specified semantic analyser hook.
    HiveConf conf = new HiveConf(this.conf);
    conf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname, hook);
    this.testDriver = new Driver(conf);
    this.testSession = SessionState.start(conf);
  }
}
