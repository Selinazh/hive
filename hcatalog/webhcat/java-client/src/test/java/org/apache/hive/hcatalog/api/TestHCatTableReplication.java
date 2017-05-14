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

package org.apache.hive.hcatalog.api;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hive.hcatalog.NoExitSecurityManager;
import org.apache.hive.hcatalog.cli.SemanticAnalysis.HCatSemanticAnalyzer;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestHCatTableReplication {
  private static final Logger LOG = LoggerFactory.getLogger(TestHCatTableReplication.class);
  private static final String sourceHCatPort = "20101", targetHCatPort = "20102";
  private static HCatClient sourceHCat, targetHCat;
  private static SecurityManager securityManager;

  private static String dbName = "mydb", tableName = "mytable";

  private static class RunMS implements Runnable {

    private final String msPort;
    private List<String> args = Lists.newArrayList();

    public RunMS(String msPort) {
      this.msPort = msPort;
      this.args.add("--hiveconf");
      this.args.add("hive.metastore.event.listeners=org.apache.hive.hcatalog.api.MockMetastoreEventListener");
      this.args.add("-v");
      this.args.add("-p");
      this.args.add(this.msPort);
    }

    public RunMS arg(String arg) {
      this.args.add(arg);
      return this;
    }

    @Override
    public void run() {
      try {
        HiveMetaStore.main(args.toArray(new String[args.size()]));
      } catch (Throwable t) {
        LOG.error("Exiting. Got exception from metastore: ", t);
      }
    }
  } // class RunMS;

  @AfterClass
  public static void tearDown() throws Exception {
    LOG.info("Shutting down metastore.");
    System.setSecurityManager(securityManager);
  }

  @BeforeClass
  public static void startMetaStoreServers() throws Exception {

    // Start up source HCatalog server.
    Thread sourceMetastoreThread = new Thread(new RunMS(sourceHCatPort));
    sourceMetastoreThread.start();
    Thread.sleep(10000);

    securityManager = System.getSecurityManager();
    System.setSecurityManager(new NoExitSecurityManager());
    HiveConf sourceHCatConf = new HiveConf(TestHCatClient.class);
    sourceHCatConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:"
        + sourceHCatPort);
    sourceHCatConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    sourceHCatConf.set(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
        HCatSemanticAnalyzer.class.getName());
    sourceHCatConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
    sourceHCatConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
    sourceHCatConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname,
        "false");
    sourceHCatConf.set("hive.metastore.event.listeners", "org.apache.hive.hcatalog.listener.NotificationListener");
    System.setProperty(HiveConf.ConfVars.PREEXECHOOKS.varname, " ");
    System.setProperty(HiveConf.ConfVars.POSTEXECHOOKS.varname, " ");

    // Start up target HCatalog server.
    Thread targetMetastoreThread = new Thread(new RunMS(targetHCatPort)
        .arg("--hiveconf")
        .arg("javax.jdo.option.ConnectionURL") // Reset, to use a different Derby instance.
        .arg(sourceHCatConf.get("javax.jdo.option.ConnectionURL")
            .replace("metastore", "target_metastore")));
    targetMetastoreThread.start();
    Thread.sleep(10000);
    HiveConf targetHCatConf = new HiveConf(sourceHCatConf);
    targetHCatConf.setVar(HiveConf.ConfVars.METASTOREURIS,
        "thrift://localhost:" + targetHCatPort);

    // Initialize HCatClient instances.
    sourceHCat = HCatClient.create(new Configuration(sourceHCatConf));
    targetHCat = HCatClient.create(new Configuration(targetHCatConf));

    // Create source and target databases.
    sourceHCat.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);
    sourceHCat.createDatabase(HCatCreateDBDesc.create(dbName).build());
    targetHCat.dropDatabase(dbName, true, HCatClient.DropDBMode.CASCADE);
    targetHCat.createDatabase(HCatCreateDBDesc.create(dbName).build());
  }

  private static HCatTable createAndGetSourceTable() throws Exception {
    List<HCatFieldSchema> columnSchema = Arrays.asList(
        new HCatFieldSchema("foo", HCatFieldSchema.Type.INT, ""),
        new HCatFieldSchema("bar", HCatFieldSchema.Type.STRING, "")
    );

    List<HCatFieldSchema> partitionSchema = Arrays.asList(
        new HCatFieldSchema("dt", HCatFieldSchema.Type.STRING, ""),
        new HCatFieldSchema("grid", HCatFieldSchema.Type.STRING, "")
    );

    HCatTable sourceTable = new HCatTable(dbName, tableName).cols(columnSchema).partCols(partitionSchema);
    sourceHCat.createTable(HCatCreateTableDesc.create(sourceTable).build());
    return getTableFrom(sourceHCat);
  }

  private static HCatTable createAndGetTargetTable(HCatTable targetTable) throws Exception {
    targetHCat.createTable(HCatCreateTableDesc.create(targetTable).build());
    return getTableFrom(targetHCat);
  }

  private static HCatTable getTableFrom(HCatClient hcatClient) throws Exception {
    HCatTable updatedTable= hcatClient.getTable(dbName, tableName);
    assertNotNull("Updated table shouldn't have been null for " + dbName + "." + tableName, updatedTable);
    return updatedTable;
  }

  private HCatTable updateTable(HCatClient hcatClient, HCatTable newTable) throws Exception {
    String dbName = newTable.getDbName();
    String tableName = newTable.getTableName();
    hcatClient.updateTableSchema(dbName, tableName, newTable);
    return getTableFrom(hcatClient);
  }

  // Shorthand helpers.
  private HCatTable updateAndGetSourceTable(HCatTable newTable) throws Exception {
    return updateTable(sourceHCat, newTable);
  }
  private HCatTable updateAndGetTargetTable(HCatTable newTable) throws Exception {
    return updateTable(targetHCat, newTable);
  }

  private void assertTableDoesNotexist(HCatClient hcatClient) {
    try {
      hcatClient.getTable(dbName, tableName);
      fail("Table shouldn't have been found.");
    }
    catch (ObjectNotFoundException ignore) {
    }
    catch (Throwable t) {
      fail("Table " + dbName + "." + tableName + " could not be cleaned up.");
    }
  }

  @After
  public void cleanUpTables() {
    try {
      sourceHCat.dropTable(dbName, tableName, true); // Suppress errors if Table isn't found.
      assertTableDoesNotexist(sourceHCat);
      targetHCat.dropTable(dbName, tableName, true); // Suppress errors if Table isn't found.
      assertTableDoesNotexist(targetHCat);
    }
    catch (Exception exception) {
      exception.printStackTrace();
      fail("Unexpected exception during cleanup. " + exception);
    }
  }

  @Test
  public void testTableCopyExcludesBlackListedProperties() throws Exception {

    try {

      HCatTable sourceTable = createAndGetSourceTable();
      HCatTable targetTable = createAndGetTargetTable(sourceTable);

      assertEquals("Source and target tables should not have generated a diff.",
          HCatTable.NO_DIFF, sourceTable.diff(targetTable));

      assertNotSame("Source and target shouldn't be sharing the same MessageBusTopicName",
          sourceTable.getTblProps().get(HCatConstants.HCAT_MSGBUS_TOPIC_NAME),
          targetTable.getTblProps().get(HCatConstants.HCAT_MSGBUS_TOPIC_NAME));


      assertNotSame("Source and target shouldn't be sharing the same last-update-time",
          sourceTable.getTblProps().get(hive_metastoreConstants.DDL_TIME),
          targetTable.getTblProps().get(hive_metastoreConstants.DDL_TIME));
    }
    catch (Throwable exception) {
      exception.printStackTrace();
      fail("Unexpected exception: " + exception);
    }

  }

  @Test
  public void testTableUpdateDoesNotNukePriorBlackListedSettings() throws Exception {
    try {

      HCatTable sourceTable = createAndGetSourceTable();
      String oldJmsTopicName = sourceTable.getTblProps().get(HCatConstants.HCAT_MSGBUS_TOPIC_NAME);
      sourceTable.tableType(HCatTable.Type.EXTERNAL_TABLE);
      sourceTable = updateAndGetSourceTable(sourceTable);

      assertEquals("Could not change Table to External",
          HCatTable.Type.EXTERNAL_TABLE.name(), sourceTable.getTabletype());
      assertEquals("JMS Topic Lost when table-type was changed! ",
          oldJmsTopicName, sourceTable.getTblProps().get(HCatConstants.HCAT_MSGBUS_TOPIC_NAME));

    }
    catch (Throwable exception) {
      exception.printStackTrace();
      fail("Unexpected exception: " + exception);
    }
  }

  @Test
  public void testExternalTablesDoNotGenerateDiffsAgainstManaged() throws Exception {
    try {
      HCatTable sourceTable = createAndGetSourceTable();
      HCatTable targetTable = createAndGetTargetTable(sourceTable);
      targetTable.tableType(HCatTable.Type.EXTERNAL_TABLE);
      targetTable = updateAndGetTargetTable(targetTable);

      assertEquals("Source and target tables should not have generated a diff.",
          HCatTable.NO_DIFF, sourceTable.diff(targetTable));

      assertEquals("Source table should've been MANAGED.",
          HCatTable.Type.MANAGED_TABLE.name(), sourceTable.getTabletype());

      assertEquals("Target table should've been EXTERNAL.",
          HCatTable.Type.EXTERNAL_TABLE.name(), targetTable.getTabletype());

    }
    catch (Throwable exception) {
      exception.printStackTrace();
      fail("Unexpected exception: " + exception);
    }
  }

  private void testUpdateForNonBlackListedTableProperty(
      HCatTable.Type sourceTableType, HCatTable.Type targetTableType) throws Exception {
    try {

      HCatTable sourceTable = createAndGetSourceTable();
      sourceTable.tableType(sourceTableType);
      sourceTable = updateAndGetSourceTable(sourceTable);

      HCatTable targetTable = createAndGetTargetTable(sourceTable);
      targetTable.tableType(targetTableType);
      targetTable = updateAndGetTargetTable(targetTable);

      // Now change source-table's properties.

      Map<String, String> tableProperties = sourceTable.getTblProps();
      tableProperties.put("orc.compress", "NONE");
      tableProperties.put("orc.stripe.size.mb", "64");
      sourceTable = updateAndGetSourceTable(sourceTable);

      EnumSet<HCatTable.TableAttribute> diff = targetTable.diff(sourceTable);

      assertNotSame("Non-empty diff expected.", HCatTable.NO_DIFF, diff);
      assertTrue("Expected TABLE_PROPERTIES to be different.",
          diff.contains(HCatTable.TableAttribute.TABLE_PROPERTIES));
      assertEquals("Expected only TABLE_PROPERTIES to be different.",
                  1, diff.size());

      // Save old targetTable properties.
      String oldTargetTableType = targetTable.getTabletype();
      String oldJmsTopicName = targetTable.getTblProps().get(HCatConstants.HCAT_MSGBUS_TOPIC_NAME);

      assertFalse("Target table shouldn't have contained orc.compress settings before.",
          targetTable.getTblProps().containsKey("orc.compress"));
      assertFalse("Target table shouldn't have contained orc.stripe.size.mb settings before.",
                  targetTable.getTblProps().containsKey("orc.compress"));

      // Update the targetTable
      targetTable = updateAndGetTargetTable(targetTable.resolve(sourceTable, diff));

      assertTrue("Target table should have contained orc.compress settings after update.",
          targetTable.getTblProps().containsKey("orc.compress"));
      assertTrue("Target table should have contained orc.stripe.size.mb settings after update.",
                  targetTable.getTblProps().containsKey("orc.compress"));
      assertEquals("After update, targetTable's type shouldn't have changed.",
          oldTargetTableType, targetTable.getTabletype());
      assertEquals("After update, targetTable's JMS settings shouldn't have changed.",
                   oldJmsTopicName, targetTable.getTblProps().get(HCatConstants.HCAT_MSGBUS_TOPIC_NAME));

    }
    catch (Throwable exception) {
      exception.printStackTrace();
      fail("Unexpected exception: " + exception);
    }
  }

  @Test
  public void testUpdateForNonBlackListedTableProperty_ForAllTableTypeCombinations() throws Exception {
    testUpdateForNonBlackListedTableProperty(HCatTable.Type.MANAGED_TABLE, HCatTable.Type.MANAGED_TABLE);
    cleanUpTables();
    testUpdateForNonBlackListedTableProperty(HCatTable.Type.MANAGED_TABLE, HCatTable.Type.EXTERNAL_TABLE);
    cleanUpTables();
    testUpdateForNonBlackListedTableProperty(HCatTable.Type.EXTERNAL_TABLE, HCatTable.Type.MANAGED_TABLE);
    cleanUpTables();
    testUpdateForNonBlackListedTableProperty(HCatTable.Type.EXTERNAL_TABLE, HCatTable.Type.EXTERNAL_TABLE);
    cleanUpTables();
  }

  private void testUpdatingTableFormatAndSchema(HCatTable.Type sourceTableType,
                                                HCatTable.Type targetTableType) throws Exception {
    try {
      HCatTable sourceTable = createAndGetSourceTable();
      sourceTable.tableType(sourceTableType);
      sourceTable = updateAndGetSourceTable(sourceTable);

      HCatTable targetTable = createAndGetTargetTable(sourceTable);
      targetTable.tableType(targetTableType);
      targetTable = updateAndGetTargetTable(targetTable);

      EnumSet<HCatTable.TableAttribute> diff = targetTable.diff(sourceTable);
      assertEquals("Source and target tables should be equivalent right now.", HCatTable.NO_DIFF, diff);

      // Update source-table schema, and formats.
      List<HCatFieldSchema> newCols = Lists.newArrayList(sourceTable.getCols());
      newCols.add(new HCatFieldSchema("goo_new", HCatFieldSchema.Type.DOUBLE, "new column"));
      sourceTable.cols(newCols)
                 .fileFormat("orcfile")
                 .serdeParam(serdeConstants.FIELD_DELIM, Character.toString('\001'));

      sourceTable = updateAndGetSourceTable(sourceTable);

      diff = targetTable.diff(sourceTable);

      assertTrue("InputFormat should have been flagged as different.",
                 diff.contains(HCatTable.TableAttribute.INPUT_FORMAT));
      assertTrue("OutputFormat should have been flagged as different.",
          diff.contains(HCatTable.TableAttribute.OUTPUT_FORMAT));
      assertTrue("SerDe should have been flagged as different.",
          diff.contains(HCatTable.TableAttribute.SERDE));
      assertTrue("SerDeProperties should have been flagged as different.",
          diff.contains(HCatTable.TableAttribute.SERDE_PROPERTIES));
      assertFalse("TableProperties should not have been flagged at all.",
          diff.contains(HCatTable.TableAttribute.TABLE_PROPERTIES));

      // Save old TableProperties.
      Map<String, String> oldTargetTableProperties = targetTable.getTblProps();
      targetTable = updateAndGetTargetTable(targetTable.resolve(sourceTable, diff));

      assertTrue("Target-table's properties should not have changed.",
          Maps.difference(oldTargetTableProperties, targetTable.getTblProps()).areEqual());
      assertEquals("Should have inherited InputFormat from source.",
          sourceTable.getInputFileFormat(), targetTable.getInputFileFormat());
      assertEquals("Should have inherited OutputFormat from source.",
          sourceTable.getOutputFileFormat(), targetTable.getOutputFileFormat());
      assertEquals("Should have inherited SerDe from source.",
          sourceTable.getSerdeLib(), targetTable.getSerdeLib());
      assertTrue("Should have inherited SerDe properties from source.",
          Maps.difference(sourceTable.getSerdeParams(), targetTable.getSerdeParams()).areEqual());
      assertArrayEquals("Column schemas should now be identical.",
          sourceTable.getCols().toArray(), targetTable.getCols().toArray());
    }
    catch (Exception exception) {
      exception.printStackTrace();
      fail("Unexpected exception: " + exception);
    }
  }

  @Test
  public void testUpdatingTableFormatAndSchema_ForAllTableTypeCombinations() throws Exception {
    testUpdatingTableFormatAndSchema(HCatTable.Type.MANAGED_TABLE, HCatTable.Type.MANAGED_TABLE);
    cleanUpTables();
    testUpdatingTableFormatAndSchema(HCatTable.Type.MANAGED_TABLE, HCatTable.Type.EXTERNAL_TABLE);
    cleanUpTables();
    testUpdatingTableFormatAndSchema(HCatTable.Type.EXTERNAL_TABLE, HCatTable.Type.MANAGED_TABLE);
    cleanUpTables();
    testUpdatingTableFormatAndSchema(HCatTable.Type.EXTERNAL_TABLE, HCatTable.Type.EXTERNAL_TABLE);
    cleanUpTables();
  }
}
