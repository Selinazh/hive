package org.apache.hive.service;

import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.TableauSemanticAnalyzerHook;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.service.server.HiveServer2;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class TestHiveConfOverride extends TestCase{

  private static HiveServer2 hiveServer2;
  private String origHiveSitePath = null;
  private String origHiveConfData = null;
  private static final String QUEUE = "mapred.job.queue.name";

  @BeforeClass
  public void setUp() throws Exception {

    HiveConf hiveConf = new HiveConf();
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname, TableauSemanticAnalyzerHook.class.getName());
    hiveConf.set(QUEUE, "unfunded");
    hiveConf.addToRestrictList(QUEUE);

    updateHiveConf(hiveConf);

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

  /*
   Update Hive-site.xml with the values in HiveConf as the hive.conf.restricted.list is
   set in HiveConf constructor and cannot be overridden after that. The original hive-site
   is backed up and restored during teardown.
   */
  private void updateHiveConf(HiveConf hiveConf) throws Exception {
    if (origHiveSitePath == null) {
      origHiveSitePath = hiveConf.getHiveSiteLocation().getPath();
    }

    File file = new File(origHiveSitePath);
    if (file.exists()) {
      BufferedReader br = new BufferedReader(new FileReader(file));
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line + "\n");
      }
      origHiveConfData = sb.toString();
      br.close();
    }

    FileWriter hiveWriter = new FileWriter(file);
    hiveConf.writeXml(hiveWriter);
    hiveWriter.flush();
    hiveWriter.close();
  }

  @AfterClass
  public void tearDown() throws Exception {
    if (hiveServer2 != null) {
      hiveServer2.stop();
    }

    if (origHiveSitePath != null) {
      File file = new File(origHiveSitePath);
      if (file.exists()) {
        file.delete();
      }

      if (origHiveConfData != null) {
        FileWriter hiveWriter = new FileWriter(file);
        hiveWriter.write(origHiveConfData);
        hiveWriter.close();
        origHiveConfData = null;
        origHiveSitePath = null;
      }
    }
  }

  @Test
  public void testConnectionURLOverride() throws Exception {
    HiveConnection connection = null;

    boolean exceptionSeen = false;

    try {
      // Invalid conf settings should result in a SQLException. 
      connection = new HiveConnection("jdbc:hive2://localhost:10000/default?" + QUEUE +"=default", new Properties());
    } catch (SQLException e) {
      Assert.assertTrue(e.getMessage().startsWith("Failed to open new session:"));
      Assert.assertTrue(e.getMessage().endsWith("It is in the listof parameters that can't be modified at runtime"));
      exceptionSeen = true;
    }

    // connection should not be non-null.
    if (connection != null) {
      connection.close();
    }

    Assert.assertTrue(exceptionSeen);
  }

  @Test
  public void testStatementOverride() throws Exception {
    HiveConnection connection = null;
    Statement stmt = null;
    ResultSet results = null;

    try {
      connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
      stmt = connection.createStatement();

      boolean exceptionSeen = false;

      try {
        // Setting immutable conf variables should result in an exception.
        stmt.execute("set " + QUEUE + "=default");
      } catch (SQLException e) {
        Assert.assertTrue(e.getMessage().startsWith("Error while processing statement: Cannot modify " + QUEUE + " at runtime."));
        exceptionSeen = true;
      }

      Assert.assertTrue("Expected SET " + QUEUE + " to throw an exception", exceptionSeen);

      stmt.execute("set " + QUEUE);
      results = stmt.getResultSet();
      results.next();
      Assert.assertEquals(QUEUE + "=unfunded", results.getString(1));
    } finally {
      if (results != null)
        results.close();
      if (stmt != null)
        stmt.close();
      if (connection != null)
        connection.close();
    }
  }
}
