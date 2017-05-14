/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.pig;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.hcatalog.HcatTestUtils;
import org.apache.hive.hcatalog.MiniGenericCluster;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigImplConstants;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.tools.pigstats.JobStats;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

@RunWith(Parameterized.class)
public class TestHCatLoaderPredicatePushDown {
  private static final Logger LOG = LoggerFactory.getLogger(TestHCatLoaderPredicatePushDown.class);
  private static final String TEST_DATA_DIR = HCatUtil.makePathASafeFileName(System.getProperty("java.io.tmpdir")
                                                + File.separator
                                                + TestHCatLoaderPredicatePushDown.class.getCanonicalName()
                                                + "-" + System.currentTimeMillis());
  private static final String TEST_WAREHOUSE_DIR = TEST_DATA_DIR + "/warehouse";
  private static final String TEXT_DATA_FILE = TEST_DATA_DIR + "/basic.input.data";
  private static final String COLUMNAR_TABLE_NAME_PREFIX = "columnar_table_";

  private static MiniGenericCluster cluster = MiniGenericCluster.buildCluster();
  private static FileSystem clusterFS = cluster.getFileSystem();
  private static Driver driver;
  private static Random random = new Random();
  private static Set<String> tablesCreated = Sets.newHashSet();

  private String tableName;
  private String storageFormat;
  private String tblProperties;

  @Parameterized.Parameters
  public static Collection<Object[]> storageFormatsThatSupportPPD() {
    ArrayList<Object[]> list = Lists.newArrayList();
    list.add(new String[]{
        IOConstants.ORCFILE,
        "(\"orc.stripe.size\"=\"100000\", " +
            "\"orc.row.index.stride\"=\"1000\", " +
            "\"orc.compress\"=\"NONE\" " +
            ")"
    });
    return list;
  }

  public TestHCatLoaderPredicatePushDown(String storageFormat, String tblProperties) {
    this.tableName = COLUMNAR_TABLE_NAME_PREFIX + storageFormat;
    this.storageFormat = storageFormat;
    this.tblProperties = tblProperties;
  }

  @BeforeClass
  public static void setupAllTests() throws Exception {
    setUpCluster();
    setUpLocalFileSystemDirectories();
    setUpClusterFileSystemDirectories();
    setUpHiveDriver();
    createTextData();
  }

  @Before
  public void setupSingleTest() throws Exception {
    if (!tablesCreated.contains(tableName)) {
      createColumnarTable();
      convertTextDataToColumnarStorage();
      tablesCreated.add(tableName);
    }
  }

  @AfterClass
  public static void tearDownAllTests() throws Exception {
    for (String table : tablesCreated) {
      dropTable(table);
    }
    tearDownCluster();
    clearUpLocalFileSystemDirectories();
  }

  private static void setUpCluster() throws Exception {
    cluster = MiniGenericCluster.buildCluster();
    clusterFS = cluster.getFileSystem();
  }

  private static void tearDownCluster() throws Exception {
    cluster.shutDown();
  }

  private static void setUpLocalFileSystemDirectories() {
    File f = new File(TEST_DATA_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
    if(!(new File(TEST_DATA_DIR).mkdirs())) {
      throw new RuntimeException("Could not create test-directory " + TEST_DATA_DIR + " on local filesystem.");
    }
  }

  private static void clearUpLocalFileSystemDirectories() {
    File f = new File(TEST_DATA_DIR);
    if (f.exists()) {
      FileUtil.fullyDelete(f);
    }
  }

  private static void setUpClusterFileSystemDirectories() throws IOException {
    FileSystem clusterFS = cluster.getFileSystem();
    Path warehouseDir = new Path(TEST_WAREHOUSE_DIR);
    if (clusterFS.exists(warehouseDir)) {
      clusterFS.delete(warehouseDir, true);
    }
    clusterFS.mkdirs(warehouseDir);
  }

  private static void setUpHiveDriver() throws IOException {
    HiveConf hiveConf = createHiveConf();
    driver = new Driver(hiveConf);
    driver.setMaxRows(1000);
    SessionState.start(new CliSessionState(hiveConf));
  }

  private static HiveConf createHiveConf() {
    HiveConf hiveConf = new HiveConf(cluster.getConfiguration(), TestHCatLoaderPredicatePushDown.class);
    hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, TEST_WAREHOUSE_DIR);
    return hiveConf;
  }

  /**
   * Create data with schema:
   *    number \t string \t filler_string
   * @throws Exception
   */
  private static void createTextData() throws Exception {
    int LOOP_SIZE = 1000;
    ArrayList<String> input = Lists.newArrayListWithExpectedSize((LOOP_SIZE+1) * LOOP_SIZE);
    for (int i = 1; i <= LOOP_SIZE; i++) {
      String si = i + "";
      for (int j = 1; j <= LOOP_SIZE; j++) {
        String sj = "S" + j + "S";
        input.add(si + "\t" + (i*j) + "\t" + sj);
      }
    }

    // Add nulls.
    for (int i=0; i<LOOP_SIZE; ++i) {
      input.add("\t" + "\t" + "S" + "_null_" + i + "_S");
    }
    HcatTestUtils.createTestDataFile(TEXT_DATA_FILE, input.toArray(new String[input.size()]));
    cluster.getFileSystem().copyFromLocalFile(new Path(TEXT_DATA_FILE), new Path(TEXT_DATA_FILE));
  }

  private void createColumnarTable() throws IOException, CommandNeedRetryException {
    createTable(tableName, "a int, b string, c string", null, storageFormat, tblProperties);
  }

  private void createTable(String tableName, String schema,
                           String partitionedBy, String storageFormat, String tblProperties)
      throws IOException, CommandNeedRetryException {
    String createTable;
    createTable = "create table " + tableName + "(" + schema + ") ";
    if ((partitionedBy != null) && (!partitionedBy.trim().isEmpty())) {
      createTable = createTable + "partitioned by (" + partitionedBy + ") ";
    }
    createTable = createTable + "stored as " + storageFormat;
    createTable += " TBLPROPERTIES " + tblProperties;
    executeStatementOnDriver(createTable);

    String describeTable = "describe formatted " + tableName;
    executeStatementOnDriver(describeTable);
  }

  /**
   * Execute Hive CLI statement
   * @param cmd arbitrary statement to execute
   */
  private void executeStatementOnDriver(String cmd) throws IOException, CommandNeedRetryException {
    LOG.debug("Executing: " + cmd);
    CommandProcessorResponse cpr = driver.run(cmd);
    if(cpr.getResponseCode() != 0) {
      throw new IOException("Failed to execute \"" + cmd + "\". " +
          "Driver returned " + cpr.getResponseCode() +
          " Error: " + cpr.getErrorMessage());
    }
    else {
      List<String> results = Lists.newArrayList();
      if (driver.getResults(results)) {
        System.out.println("Got results: ");
        for (String result : results) {
          System.out.println(result);
        }
      }
      else {
        System.out.println("Got no results!");
      }
    }
  }

  private void convertTextDataToColumnarStorage() throws IOException {
    PigServer server = getPigServer();
    server.setBatchOn();
    int i = 0;
    server.registerQuery("A = load '" + TEXT_DATA_FILE + "' as (a:int, b:chararray, c:chararray);", ++i);

    server.registerQuery("store A into '" + tableName + "' using org.apache.hive.hcatalog.pig.HCatStorer();", ++i);
    server.executeBatch();
  }

  private static void dropTable(String tablename) throws IOException, CommandNeedRetryException {
    driver.run("drop table if exists " + tablename);
  }

  private long runQueryAndGetHdfsBytesReadForAlias(PigServer server, String query, String resultAlias) throws IOException {
    Path outputPath = new Path("/tmp/output_" + random.nextInt());
    if (clusterFS.exists(outputPath)) {
      Assert.assertTrue("Couldn't delete outputPath: " + outputPath, clusterFS.delete(outputPath, true));
    }

    for (String line : query.split("\n")) {
      server.registerQuery(line);
    }
    ExecJob pigJob = server.store(resultAlias, outputPath.getName());
    JobStats jobStats = (JobStats)pigJob.getStatistics().getJobGraph().getSources().get(0);
    Assert.assertTrue("Query should have succeeded.", jobStats.isSuccessful());
    return jobStats.getHdfsBytesRead();
  }

  private Iterator<Tuple> getResultsForAlias(PigServer server, String query, String resultAlias) throws IOException {
    for (String line : query.split("\n")) {
      server.registerQuery(line);
    }
    return server.openIterator(resultAlias);
  }

  private PigServer getPigServer() throws IOException {
    return new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
  }

  private PigServer getPigServerWithPPDDisabled() throws IOException {
    PigServer server = getPigServer();
    server.getPigContext()
        .getProperties()
        .setProperty(
            PigImplConstants.PIG_OPTIMIZER_RULES_KEY,
            ObjectSerializer.serialize(Sets.newHashSet("PredicatePushdownOptimizer")));
    return server;
  }

  private static final String PPD_LOADER     = "org.apache.hive.hcatalog.pig.HCatLoaderWithPredicatePushdown";
  private static final String NON_PPD_LOADER     = "org.apache.hive.hcatalog.pig.HCatLoader";

  private void compareResultsFor(String query, String resultAlias) throws IOException {
    // Compare HDFS Bytes read.
    long bytesReadWithPPD    = runQueryAndGetHdfsBytesReadForAlias(getPigServer(), query, resultAlias);
    long bytesReadWithoutPPD = runQueryAndGetHdfsBytesReadForAlias(getPigServerWithPPDDisabled(), query, resultAlias);

    Assert.assertTrue("HDFSBytesRead with " + PPD_LOADER + "(" + bytesReadWithPPD + ") should not exceed "
                      + NON_PPD_LOADER + "(" + bytesReadWithoutPPD + ").", bytesReadWithPPD <= bytesReadWithoutPPD);

    // Compare results.
    Iterator<Tuple> resultsWithPPD        = getResultsForAlias(getPigServer(), query, resultAlias);
    Iterator<Tuple> resultsWithoutPPD     = getResultsForAlias(getPigServerWithPPDDisabled(), query, resultAlias);
    Iterator<Tuple> resultsWithOldLoader  = getResultsForAlias(getPigServer(), query.replaceAll(PPD_LOADER, NON_PPD_LOADER), resultAlias);

    while (resultsWithPPD.hasNext() && resultsWithoutPPD.hasNext() && resultsWithOldLoader.hasNext()) {
      Tuple ppdLoaderTuple   = resultsWithPPD.next();
      Tuple ppdDisabledTuple = resultsWithoutPPD.next();
      Tuple oldLoaderTuple   = resultsWithOldLoader.next();
      Assert.assertEquals("Results don't match, with and without PPD.", ppdLoaderTuple, ppdDisabledTuple);
      Assert.assertEquals("Results don't match between HCatLoader and HCatLoaderWithPPD.", ppdLoaderTuple, oldLoaderTuple);

    }

    Assert.assertFalse("Query with HCatLoaderPPD returned more results than expected.", resultsWithPPD.hasNext());
    Assert.assertFalse("Query without PPD returned more results than expected.", resultsWithoutPPD.hasNext());
    Assert.assertFalse("Query without HCatLoader returned more results than expected.", resultsWithOldLoader.hasNext());
  }

  private void test(String projection, String filter) throws IOException {
    String query =    "X = LOAD '" + tableName + "' USING " + PPD_LOADER + "();\n"
                    + "X = FOREACH X generate " + projection + " ;\n"
                    + "X = FILTER X by " + filter + " ;";
    compareResultsFor(query, "X");
  }

  @Test
  public void testEmptySelect() throws IOException {
    test("a", "a < 0");
  }

  @Test
  public void testLessThan() throws IOException {
    test("a", "a < 10");
  }

  @Test
  public void testGreaterThanEquals() throws IOException {
    test("a", "a >= 990");
  }

  @Test
  public void testCompoundPredicate() throws IOException {
    test("a,b", "a < 10 AND (b < '20' OR b > '1000')");
  }
}
