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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.metastore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;

import com.google.common.collect.Lists;

/**
 * Tests hive metastore search support.
 */
public class TestMetastoreSearch extends TestCase {

  // Embedded metastore client with directSQL enabled.
  protected HiveMetaStoreClient client;

  private final String dbName = "searchdb";
  private final String tblName1 = "searchTBL_1";
  private final String tblName2 = "searchTBL_2";

  @Override
  protected void tearDown() throws Exception {
    try {
      // If the test case fails, just drop the database
      if (client != null) {
        silentDropDatabase(dbName);
        client.close();
      }
    } catch (Throwable e) {
      System.err.println("Unable to close metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    } finally {
      super.tearDown();
    }
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    try {
      HiveConf hiveConf = new HiveConf(this.getClass());
      client = new HiveMetaStoreClient(hiveConf, null);

      silentDropDatabase(dbName);
    } catch (Throwable e) {
      System.err.println("Unable to open the metastore");
      System.err.println(StringUtils.stringifyException(e));
      throw new Exception(e);
    }
  }

  private void silentDropDatabase(String dbName) throws TException {
    try {
      for (String db : client.getAllDatabases()) {
        for (String table : client.getTables(db, "*")) {
          client.dropTable(db, table);
        }

        if (! "default".equals(db)) {
          client.dropDatabase(db);
        }
      }
    } catch (NoSuchObjectException ignored) {
    } catch (InvalidOperationException ignored) {
    }
  }

  public void testSearchDirectSQL() throws Exception {

    silentDropDatabase(dbName);

    createDB(dbName);
    createTable(dbName, tblName1, "comments for table_1", "owner1");
    createTable(dbName, tblName2, "comments for table_2", "owner2");

    testTableNameLocationOwner(client);

    String[] locationMatch = {TableSearchResult.LOCATION_MATCH};
    String[] tableCommentsMatch = {TableSearchResult.TABLE_COMMENTS_MATCH};
    String[] colNameAndCommentsMatch = {TableSearchResult.COLUMN_NAME_MATCH, TableSearchResult.COLUMN_COMMENTS_MATCH};
    String[] colNameMatch = {TableSearchResult.COLUMN_NAME_MATCH};
    String[] colCommentsMatch = {TableSearchResult.COLUMN_COMMENTS_MATCH};
    String[] partNameMatch = {TableSearchResult.PARTITION_NAME_MATCH};
    String[] partNameAndCommentsMatch = {TableSearchResult.PARTITION_NAME_MATCH, TableSearchResult.PARTITION_COMMENTS_MATCH};
    String[] partCommentsMatch = {TableSearchResult.PARTITION_COMMENTS_MATCH};
    String[] tablePropertyMatch = {TableSearchResult.TABLE_PROPERTY_MATCH};

    // Search based on table location
    checkResults(client.listTablesByQuery(client.getDatabase(dbName).getLocationUri(), -1), 2, locationMatch);
    checkResults(client.listTablesByQuery(client.getTable(dbName, tblName1).getSd().getLocation(), -1), 1, locationMatch);
    checkResults(client.listTablesByQuery(client.getTable(dbName, tblName2).getSd().getLocation(), -1), 1, locationMatch);

    // Search based on table comments
    checkResults(client.listTablesByQuery("table_1", -1), 1, tableCommentsMatch);
    checkResults(client.listTablesByQuery("table_2", -1), 1, tableCommentsMatch);
    checkResults(client.listTablesByQuery("table", -1), 2, tableCommentsMatch);
    checkResults(client.listTablesByQuery("table", 1), 1, tableCommentsMatch);

    // Search based on column name and comments
    checkResults(client.listTablesByQuery("c1", -1), 2, colNameAndCommentsMatch);
    checkResults(client.listTablesByQuery("c2", -1), 2, colNameAndCommentsMatch);
    checkResults(client.listTablesByQuery("c2", 1), 1, colNameMatch);

    // Search based on column comments
    checkResults(client.listTablesByQuery("comments-c1", -1), 2, colCommentsMatch);
    checkResults(client.listTablesByQuery("comments-c2", -1), 2, colCommentsMatch);
    checkResults(client.listTablesByQuery("comments-c", -1), 2, colCommentsMatch);
    checkResults(client.listTablesByQuery("comments-c", 1), 1, colCommentsMatch);

    // Search based on partition name and comments
    checkResults(client.listTablesByQuery("p1", -1), 2, partNameAndCommentsMatch);
    checkResults(client.listTablesByQuery("p2", -1), 2, partNameAndCommentsMatch);
    checkResults(client.listTablesByQuery("p2", 1), 1, partNameMatch);

    // Search based on partition comments
    checkResults(client.listTablesByQuery("partition-p1", -1), 2, partCommentsMatch);
    checkResults(client.listTablesByQuery("partition-p1", -1), 2, partCommentsMatch);
    checkResults(client.listTablesByQuery("partition-p", -1), 2, partCommentsMatch);
    checkResults(client.listTablesByQuery("partition-p", 1), 1, partCommentsMatch);

    // Search based on table parameters - values
    checkResults(client.listTablesByQuery("production", -1), 1, tablePropertyMatch);
    checkResults(client.listTablesByQuery("research", -1), 1, tablePropertyMatch);
    assertEquals(tblName1.toLowerCase(), client.listTablesByQuery("production", -1).get(0).getTableName());
    assertEquals(tblName2.toLowerCase(), client.listTablesByQuery("research", -1).get(0).getTableName());

    // Search based on table parameters - keys
    checkResults(client.listTablesByQuery("hive.table.type", -1), 2, tablePropertyMatch);

    silentDropDatabase(dbName);
  }

  public void testSearchORM() throws Exception {
    // Disable Metastore DirectSQL to test ORM fallback.
    HiveConf hiveConf = new HiveConf(this.getClass());
    hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL, false);
    HiveMetaStoreClient ormClient = new HiveMetaStoreClient(hiveConf, null);

    try {
      silentDropDatabase(dbName);

      createDB(dbName);
      createTable(dbName, tblName1, "comments for table_1", "owner1");
      createTable(dbName, tblName2, "comments for table_2", "owner2");

      testTableNameLocationOwner(ormClient);

      silentDropDatabase(dbName);
    } finally {
      if (ormClient != null) {
        ormClient.close();
      }
    }
  }

  private void testTableNameLocationOwner(HiveMetaStoreClient testClient) throws TException {

    assertNotNull(testClient.getTable(dbName, tblName1));
    assertNotNull(testClient.getTable(dbName, tblName2));

    // Tags to match search results
    String[] nameMatch = {TableSearchResult.TABLE_NAME_MATCH};
    String[] nameAndLocationMatch = {TableSearchResult.TABLE_NAME_MATCH, TableSearchResult.LOCATION_MATCH};
    String[] ownerMatch = {TableSearchResult.OWNER_MATCH};

    // Search based on name and location
    checkResults(testClient.listTablesByQuery("search", -1), 2, nameAndLocationMatch);
    checkResults(testClient.listTablesByQuery("search tbl", -1), 2, nameAndLocationMatch);
    checkResults(testClient.listTablesByQuery(tblName1, -1), 1, nameAndLocationMatch);
    checkResults(testClient.listTablesByQuery(tblName2, -1), 1, nameAndLocationMatch);
    // Since we have limits, only name is matched
    checkResults(testClient.listTablesByQuery("search tbl", 1), 1, nameMatch);
    checkResults(testClient.listTablesByQuery("search", 1), 1, nameMatch);

    // Search based on owner
    checkResults(testClient.listTablesByQuery(testClient.getTable(dbName, tblName1).getOwner(), -1), 1, ownerMatch);
    checkResults(testClient.listTablesByQuery(testClient.getTable(dbName, tblName2).getOwner(), -1), 1, ownerMatch);
    checkResults(testClient.listTablesByQuery("owner", -1), 2, ownerMatch);
    checkResults(testClient.listTablesByQuery("owner", 1), 1, ownerMatch);

    // Search for non-match
    assertEquals(testClient.listTablesByQuery("this-should-not-match-anything", -1).size(), 0);
  }

  private void checkResults(List<Table> tables, int resultSize, String[] tags) {
    assertNotNull(tables);
    assertEquals(resultSize, tables.size());
    for (String tag : tags) {
      assertTrue(tables.get(0).getParameters().get(TableSearchResult.HIVE_METASTORE_SEARCH_TAGS).contains(tag));
    }
  }

  private void createDB(String dbName) throws TException {
    Database db = new Database();
    db.setName(dbName);
    client.createDatabase(db);
    assertNotNull(client.getDatabase(dbName));
  }

  private void createTable(String dbName, String tblName, String comments, String owner)
      throws TException {

    Table tbl = new Table();

    tbl.setDbName(dbName);
    tbl.setTableName(tblName);
    tbl.setOwner(owner);

    Map<String, String> params = new HashMap<String, String>();
    params.put("comment", comments);
    if (tblName.equalsIgnoreCase(tblName1)) {
      params.put("hive.table.type", "production");
    } else {
      params.put("hive.table.type", "research");
    }
    tbl.setParameters(params);

    ArrayList<FieldSchema> cols = new ArrayList<FieldSchema>(2);
    cols.add(new FieldSchema("c1", serdeConstants.STRING_TYPE_NAME, "comments-c1"));
    cols.add(new FieldSchema("c2", serdeConstants.INT_TYPE_NAME, "comments-c2"));
    addSd(cols, tbl);

    ArrayList<FieldSchema> partCols = Lists.newArrayList(
        new FieldSchema("p1", serdeConstants.STRING_TYPE_NAME, "partition-p1"),
        new FieldSchema("p2", serdeConstants.INT_TYPE_NAME, "partition-p2"));
    tbl.setPartitionKeys(partCols);

    client.createTable(tbl);
    assertNotNull(client.getTable(dbName, tblName));

    addPartition(tbl, Lists.newArrayList("p11", "32"), "part1");
    addPartition(tbl, Lists.newArrayList("p12", "32"), "part2");
    addPartition(tbl, Lists.newArrayList("p13", "31"), "part3");
    addPartition(tbl, Lists.newArrayList("p14", "-33"), "part4");
  }

  private void addSd(ArrayList<FieldSchema> cols, Table tbl) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(cols);
    sd.setCompressed(false);
    sd.setNumBuckets(1);
    sd.setParameters(new HashMap<String, String>());
    sd.setBucketCols(new ArrayList<String>());
    sd.setSerdeInfo(new SerDeInfo());
    sd.getSerdeInfo().setName(tbl.getTableName());
    sd.getSerdeInfo().setParameters(new HashMap<String, String>());
    sd.getSerdeInfo().getParameters()
        .put(serdeConstants.SERIALIZATION_FORMAT, "1");
    sd.setSortCols(new ArrayList<Order>());
    tbl.setSd(sd);
  }

  private void addPartition(Table table, List<String> vals, String location)
      throws TException {

    Partition part = new Partition();
    part.setDbName(table.getDbName());
    part.setTableName(table.getTableName());
    part.setValues(vals);
    part.setParameters(new HashMap<String, String>());
    part.setSd(table.getSd());
    part.getSd().setSerdeInfo(table.getSd().getSerdeInfo());
    part.getSd().setLocation(table.getSd().getLocation() + location);

    client.add_partition(part);
  }
}
