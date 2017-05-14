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

package org.apache.hadoop.hive.ql.metadata;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

public class TestMetadataColumnRestrictionPreEventListener {

  private static final String STRING = "string",
      INT    = "int",
      DATE   = "date";


  private static FieldSchema field(String name, String type) throws Exception {
    return new FieldSchema(name, type, "");
  }

  private static Table getTable(String format, List<FieldSchema> dataFields,
                                List<FieldSchema> partFields) throws Exception {
    return new Table(
        "myth_table", // Irrelevant.
        "myth_db",
        "myth_user",
        0, 0, 0,
        getSD(format, dataFields),
        partFields,
        Collections.<String, String>emptyMap(),
        "",
        "",
        TableType.MANAGED_TABLE.name()
    );
  }

  private static StorageDescriptor getSD(String format, List<FieldSchema> dataFields) {
    return new StorageDescriptor(
        dataFields,
        "hdfs://nn-1.myth.net:8020/projects/myth.db/myth.tbl",
        format.equalsIgnoreCase(IOConstants.AVROFILE)?
            AvroContainerInputFormat.class.getName()
            : OrcInputFormat.class.getName(),
        format.equalsIgnoreCase(IOConstants.AVROFILE)?
            AvroContainerOutputFormat.class.getName()
            : OrcOutputFormat.class.getName(),
        true,
        -1,
        new SerDeInfo(
            format,
            format.equalsIgnoreCase(IOConstants.AVROFILE)?
                AvroSerDe.class.getName()
                : OrcSerde.class.getName(),
            Collections.<String, String>emptyMap()
        ),
        Collections.<String>emptyList(),
        Collections.<Order>emptyList(),
        Collections.<String, String>emptyMap()
    );
  }

  private static PreCreateTableEvent getCreateTableEvent(Table table) {
    return new PreCreateTableEvent(table, null);
  }

  private static PreAlterTableEvent getAlterTableEvent(Table oldDefinition, Table newDefinition) {
    return new PreAlterTableEvent(oldDefinition, newDefinition, null);
  }

  private static MetadataColumnRestrictionPreEventListener listener(Configuration conf) throws HiveException {
    return new MetadataColumnRestrictionPreEventListener(conf);
  }

  @SafeVarargs
  private static Configuration conf(Pair<String, String> ... properties) {
    Configuration conf = new Configuration(false); // Don't load defaults.
    // Settings for testing.
    conf.set(HiveConf.ConfVars.METADATA_RESTRICTIONS_BLOCK_DROP_TABLE_COLUMNS.varname, "true");
    conf.set(HiveConf.ConfVars.METADATA_RESTRICTIONS_BLOCK_ADD_TABLE_COLUMNS_IN_MIDDLE.varname, "true");
    conf.set(HiveConf.ConfVars.METADATA_RESTRICTIONS_BLOCK_DROP_STRUCT_COLUMNS.varname, "true");
    conf.set(HiveConf.ConfVars.METADATA_RESTRICTIONS_BLOCK_ADD_STRUCT_COLUMNS_IN_MIDDLE.varname, "true");

    // Overrides.
    for (Pair<String, String> setting : properties) {
      conf.set(setting.getKey(), setting.getValue());
    }
    return conf;
  }

  private static Pair<String, String> pair(String key, String value) {
    return Pair.of(key, value);
  }

  @Test
  public void testPartKeyRestrictionsEnforced() throws Exception {

    try {
      listener(conf(
          pair(HiveConf.ConfVars.METADATA_PARTITION_KEY_TYPE_RESTRICTIONS_ENABLED.varname, "true")
      )).onEvent(
          getCreateTableEvent(
              getTable(
                  IOConstants.ORCFILE,
                  Lists.newArrayList(field("foo", STRING)),
                  Lists.newArrayList(field("dt", DATE), field("thyme", INT))
              )
          )
      );
      fail("Creation of Table with DATE partition-keys should have failed.");
    }
    catch (MetaException ignore) {
      // Success!!
    }
    catch(Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }

  @Test
  public void testOverridePartKeyRestrictions() throws Exception {

    try {
      listener(
          conf(
              pair( HiveConf.ConfVars.METADATA_PARTITION_KEY_TYPE_RESTRICTIONS_ENABLED.varname, "true"),
              pair( HiveConf.ConfVars.METADATA_PARTITION_KEY_TYPE_RESTRICTIONS_PERMITTED_LIST.varname, "int,string,date")
          )
      ).onEvent(
          getCreateTableEvent(
              getTable(
                  IOConstants.ORCFILE,
                  Lists.newArrayList(field("foo", STRING)),
                  Lists.newArrayList(field("dt", DATE), field("thyme", INT))
              )
          )
      );
      // Success!!
    }
    catch (MetaException unexpectedMetaException) {
      fail("Creation of Table with DATE partition-keys should have succeeded. " + unexpectedMetaException.getMessage());
      unexpectedMetaException.printStackTrace();
    }
    catch(Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }

  @Test
  public void testDroppingFirstColumnIsBlocked() throws Exception {

    try {

      Table oldTable = getTable(
          IOConstants.ORCFILE,
          Lists.newArrayList(field("foo", STRING), field("bar", STRING), field("goo", STRING)),
          Lists.newArrayList(field("dt", DATE), field("thyme", INT))
      );

      Table newTable = new Table(oldTable);
      // Drop the last column
      newTable.getSd().setCols(Lists.newArrayList(field("bar", STRING), field("goo", STRING)));

      listener(conf()).onEvent(
          getAlterTableEvent(oldTable, newTable)
      );
      fail("Drop-columns should have failed!");
    }
    catch (MetaException ignore) {
      // Success!
    }
    catch (Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }

  @Test
  public void testDroppingMiddleColumnIsBlocked() throws Exception {

    try {

      Table oldTable = getTable(
          IOConstants.ORCFILE,
          Lists.newArrayList(field("foo", STRING), field("bar", STRING), field("goo", STRING)),
          Lists.newArrayList(field("dt", DATE), field("thyme", INT))
      );

      Table newTable = new Table(oldTable);
      // Drop the last column
      newTable.getSd().setCols(Lists.newArrayList(field("foo", STRING), field("goo", STRING)));

      listener(conf()).onEvent(
          getAlterTableEvent(oldTable, newTable)
      );
      fail("Drop-columns should have failed!");
    }
    catch (MetaException ignore) {
      // Success!
    }
    catch (Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }
  @Test
  public void testDroppingLastColumnIsBlocked() throws Exception {

    try {

      Table oldTable = getTable(
          IOConstants.ORCFILE,
          Lists.newArrayList(field("foo", STRING), field("bar", STRING), field("goo", STRING)),
          Lists.newArrayList(field("dt", DATE), field("thyme", INT))
      );

      Table newTable = new Table(oldTable);
      // Drop the last column
      newTable.getSd().setCols(Lists.newArrayList(field("foo", STRING), field("bar", STRING)));

      listener(conf()).onEvent(
          getAlterTableEvent(oldTable, newTable)
      );
      fail("Drop-columns should have failed!");
    }
    catch (MetaException ignore) {
      // Success!
    }
    catch (Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }

  @Test
  public void testReorderColumnsDisabled() throws Exception {

    try {

      Table oldTable = getTable(
          IOConstants.ORCFILE,
          Lists.newArrayList(field("foo", STRING), field("bar", STRING), field("goo", STRING)),
          Lists.newArrayList(field("dt", DATE), field("thyme", INT))
      );

      Table newTable = new Table(oldTable);
      // Put last column first.
      newTable.getSd().setCols(Lists.newArrayList(field("goo", STRING), field("foo", STRING), field("bar", STRING)));

      listener(conf()).onEvent(
          getAlterTableEvent(oldTable, newTable)
      );
      fail("Column re-ordering should have failed!");
    }
    catch (MetaException ignore) {
      // Success!
    }
    catch (Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }

  @Test
  public void testAddColumnsInTheMiddleDisabled() throws Exception {

    try {

      Table oldTable = getTable(
          IOConstants.ORCFILE,
          Lists.newArrayList(field("foo", STRING), field("bar", STRING), field("goo", STRING)),
          Lists.newArrayList(field("dt", DATE), field("thyme", INT))
      );

      Table newTable = new Table(oldTable);
      // Insert column in the middle.
      newTable.getSd().setCols(Lists.newArrayList(
          field("foo", STRING), field("bar", STRING), field("car", INT), field("goo", STRING)));

      listener(conf()).onEvent(
          getAlterTableEvent(oldTable, newTable)
      );
      fail("Column insertion in the middle should have failed!");
    }
    catch (MetaException ignore) {
      // Success!
    }
    catch (Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }

  @Test
  public void testAddColumnsAtTheEndAllowed() throws Exception {

    try {

      Table oldTable = getTable(
          IOConstants.ORCFILE,
          Lists.newArrayList(field("foo", STRING), field("bar", STRING), field("goo", STRING)),
          Lists.newArrayList(field("dt", DATE), field("thyme", INT))
      );

      Table newTable = new Table(oldTable);
      // Insert column in the middle.
      newTable.getSd().setCols(Lists.newArrayList(
          field("foo", STRING), field("bar", STRING), field("goo", STRING), field("zoo", INT)));

      listener(conf()).onEvent(
          getAlterTableEvent(oldTable, newTable)
      );
      // Success!
    }
    catch (MetaException ignore) {
      fail("Column insertion at the end should have worked!");
    }
    catch (Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }

  @Test
  public void testStructColumnDropDisabled() throws Exception {

    try {

      Table oldTable = getTable(
          IOConstants.ORCFILE,
          Lists.newArrayList(field("foo", STRING), field("bar", "struct<a:int,b:string,c:date>")),
          Lists.newArrayList(field("dt", DATE), field("thyme", INT))
      );

      Table newTable = new Table(oldTable);
      // Drop the last column
      newTable.getSd().setCols(Lists.newArrayList(field("foo", STRING), field("bar", "struct<a:int,c:date>")));

      listener(conf()).onEvent(
          getAlterTableEvent(oldTable, newTable)
      );
      fail("Dropping columns from a struct member should have failed!");
    }
    catch (MetaException ignore) {
      // Success!
    }
    catch (Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }

  @Test
  public void testStructColumnReorderingDisabled() throws Exception {

    try {

      Table oldTable = getTable(
          IOConstants.ORCFILE,
          Lists.newArrayList(field("foo", STRING), field("bar", "struct<a:int,b:string,c:date>")),
          Lists.newArrayList(field("dt", DATE), field("thyme", INT))
      );

      Table newTable = new Table(oldTable);
      // Reorder columns: Move c to the middle.
      newTable.getSd().setCols(Lists.newArrayList(
          field("foo", STRING), field("bar", "struct<a:int,c:date,b:string>")));

      listener(conf()).onEvent(
          getAlterTableEvent(oldTable, newTable)
      );
      fail("Reordering columns in a struct member should have failed!");
    }
    catch (MetaException ignore) {
      // Success!
    }
    catch (Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }

  @Test
  public void testAddingColumnsToMiddleOfStructDisabled() throws Exception {

    try {

      Table oldTable = getTable(
          IOConstants.ORCFILE,
          Lists.newArrayList(field("foo", STRING), field("bar", "struct<a:int,b:string,c:date>")),
          Lists.newArrayList(field("dt", DATE), field("thyme", INT))
      );

      Table newTable = new Table(oldTable);
      // Add column in the middle.
      newTable.getSd().setCols(Lists.newArrayList(
          field("foo", STRING), field("bar", "struct<a:int,newColumn:string,b:string,c:date>")));

      listener(conf()).onEvent(
          getAlterTableEvent(oldTable, newTable)
      );
      fail("Adding columns in the middle of a struct should have failed!");
    }
    catch (MetaException ignore) {
      // Success!
    }
    catch (Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }

  @Test
  public void testAddingColumnsToMiddleOfStructInAnArrayDisabled() throws Exception {

    try {

      Table oldTable = getTable(
          IOConstants.ORCFILE,
          Lists.newArrayList(field("foo", STRING), field("bar", "array<struct<a:int,b:string,c:date>>")),
          Lists.newArrayList(field("dt", DATE), field("thyme", INT))
      );

      Table newTable = new Table(oldTable);
      // Add column in the middle.
      newTable.getSd().setCols(Lists.newArrayList(
          field("foo", STRING), field("bar", "array<struct<a:int,newColumn:string,b:string,c:date>>")));

      listener(conf()).onEvent(
          getAlterTableEvent(oldTable, newTable)
      );
      fail("Adding columns in the middle of a struct should have failed!");
    }
    catch (MetaException ignore) {
      // Success!
    }
    catch (Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }

  @Test
  public void testAddingColumnsToEndOfStructAllowed() throws Exception {

    try {

      Table oldTable = getTable(
          IOConstants.ORCFILE,
          Lists.newArrayList(field("foo", STRING), field("bar", "struct<a:int,b:string,c:date>")),
          Lists.newArrayList(field("dt", DATE), field("thyme", INT))
      );

      Table newTable = new Table(oldTable);
      // Add column to the end of the struct.
      newTable.getSd().setCols(Lists.newArrayList(
          field("foo", STRING), field("bar", "struct<a:int,b:string,c:date,newColumn:string>")));

      listener(conf()).onEvent(
          getAlterTableEvent(oldTable, newTable)
      );
      // Success!
    }
    catch (MetaException unexpectedMetaException) {
      fail("Adding columns to the end of a struct should have succeeded!" + unexpectedMetaException.getMessage());
      unexpectedMetaException.printStackTrace();
    }
    catch (Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }

  @Test
  public void testAddingColumnsToEndOfArrayOfStructsAllowed() throws Exception {

    try {

      Table oldTable = getTable(
          IOConstants.ORCFILE,
          Lists.newArrayList(field("foo", STRING), field("bar", "array<struct<a:int,b:string,c:date>>")),
          Lists.newArrayList(field("dt", DATE), field("thyme", INT))
      );

      Table newTable = new Table(oldTable);
      // Add column to the end of the struct.
      newTable.getSd().setCols(Lists.newArrayList(
          field("foo", STRING), field("bar", "array<struct<a:int,b:string,c:date,newColumn:string>>")));

      listener(conf()).onEvent(
          getAlterTableEvent(oldTable, newTable)
      );
      // Success!
    }
    catch (MetaException unexpectedMetaException) {
      fail("Adding columns to the end of a struct should have succeeded!" + unexpectedMetaException.getMessage());
      unexpectedMetaException.printStackTrace();
    }
    catch (Throwable unexpected) {
      fail("Unexpected exception: " + unexpected.getMessage());
      unexpected.printStackTrace();
    }
  }
}
