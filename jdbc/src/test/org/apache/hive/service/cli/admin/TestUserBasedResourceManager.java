package org.apache.hive.service.cli.admin;

import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.service.cli.thrift.TResourceStatusList;
import org.apache.hive.service.server.HiveServer2;

import junit.framework.Assert;

import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TestUserBasedResourceManager extends TestCase{

  private static HiveServer2 hiveServer2;

  @BeforeClass
  public void setUp() throws Exception {

    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_RESOURCE_MANAGER, "org.apache.hive.service.cli.admin.UserBasedResourceManager");
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_OPENSESSIONS, 2);
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_EXECSTATEMENTS, 1);
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_FETCHRESULTS, 1);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    String scratchDir = hiveConf.getVar(HiveConf.ConfVars.SCRATCHDIR);
    Path scratchPath = new Path(scratchDir);
    FileSystem fs = scratchPath.getFileSystem(hiveConf);
    fs.mkdirs(scratchPath);
    fs.setPermission(scratchPath, new FsPermission((short) 01777));

    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
    hiveServer2.start();
    Thread.sleep(2000);
  }

  @AfterClass
  public void tearDown() throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }
  }

  @Test
  public void testBasicConnection() throws SQLException {
    HiveConnection connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
    connection.close();
  }

  @Test
  public void testBasicAdminConnection() throws SQLException, TException {
    HiveConnection connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
    TResourceStatusList resourceStatusList = connection.getClient().GetResourceConsumptionList();
    System.out.println(resourceStatusList);
    connection.close();
  }

  @Test
  public void testSingleStatementExecution() throws SQLException {
    HiveConnection connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
    Statement statement = connection.createStatement();
    statement.execute("show tables");
    statement.close();
    connection.close();
  }

  @Test
  // Only 2 connections should go through.
  public void testMaxConnectionsPerUser() throws SQLException {
    HiveConnection connection1 = null;
    HiveConnection connection2 = null;
    HiveConnection connection3 = null;

    try {
      connection1 = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
      connection2 = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
      connection3 = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());

    } catch (SQLException e) {
      Assert.assertNotNull(connection1);
      Assert.assertNotNull(connection2);
      Assert.assertNull(connection3);

    } finally {
      assert connection1 != null;
      connection1.close();
      assert connection2 != null;
      connection2.close();
    }
  }

  @Test
  public void testMultipleExecute() throws SQLException {
    HiveConnection connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
    Statement statement = connection.createStatement();
    statement.execute("show tables");
    statement.execute("show databases");
    statement.execute("show functions");
    statement.execute("show tables");
    statement.execute("show databases");
    statement.execute("show functions");
    statement.close();
    connection.close();
  }

  @Test
  public void testMultipleExecuteQuery() throws SQLException {
    HiveConnection connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
    Statement statement = connection.createStatement();
    statement.executeQuery("show tables");
    statement.executeQuery("show databases");
    statement.executeQuery("show functions");
    statement.executeQuery("show tables");
    statement.executeQuery("show databases");
    statement.executeQuery("show functions");
    statement.close();
    connection.close();
  }
}
