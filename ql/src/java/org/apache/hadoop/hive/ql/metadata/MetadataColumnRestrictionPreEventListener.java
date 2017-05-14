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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This MetaStorePreEventListener blocks column-schema-related metadata-changes that are not supported.
 * Specifically, this class supports blocking:
 *  1. Creation of tables with partition key types that are not listed in the metastore conf.
 *  2. Dropping table-columns (in any position).
 *  3. Adding columns anywhere but at the end of the table.
 *  4. Dropping struct fields (for first level columns, or nested columns).
 *  5. Add fields to structs anywhere but at the end.
 *
 * This class currently handles only the following kinds of column-schema changes/events:
 *  1. Dropping columns: ALTER TABLE DROP COLUMN
 *  2. Appending columns: ALTER TABLE ADD COLUMN
 *  3. Changing a single column: ALTER TABLE CHANGE COLUMN
 *  4. Replacing the column schema entirely: ALTER TABLE REPLACE COLUMNS
 */
public class MetadataColumnRestrictionPreEventListener extends MetaStorePreEventListener {

  public static final Log LOG = LogFactory.getLog(MetadataColumnRestrictionPreEventListener.class);

  private boolean enforcePartitionKeyRestrictions = true;
  private boolean blockDropTableColumns = true;
  private boolean blockDropStructColumns = true;
  private boolean blockAddTableColumnsInTheMiddle = true;
  private boolean blockAddStructFieldsInTheMiddle = true;
  private Set<String> permitted_partition_key_types = Sets.newHashSet();

  public MetadataColumnRestrictionPreEventListener(Configuration config) throws HiveException {
    super(config);

    enforcePartitionKeyRestrictions = HiveConf.getBoolVar(config,
        HiveConf.ConfVars.METADATA_PARTITION_KEY_TYPE_RESTRICTIONS_ENABLED);
    // Scan conf for permitted partition-column types.
    if (enforcePartitionKeyRestrictions) {
      for (String permittedPartKeyType :
          HiveConf.getVar(config,
              HiveConf.ConfVars.METADATA_PARTITION_KEY_TYPE_RESTRICTIONS_PERMITTED_LIST)
              .trim().split(",")) {
        permittedPartKeyType = permittedPartKeyType.trim().toLowerCase();
        if (StringUtils.isNotBlank(permittedPartKeyType)) {
          permitted_partition_key_types.add(permittedPartKeyType);
        }
      }
    }

    blockDropTableColumns = HiveConf.getBoolVar(config,
                                           HiveConf.ConfVars.METADATA_RESTRICTIONS_BLOCK_DROP_TABLE_COLUMNS);
    blockDropStructColumns = HiveConf.getBoolVar(config,
                                           HiveConf.ConfVars.METADATA_RESTRICTIONS_BLOCK_DROP_STRUCT_COLUMNS);
    blockAddTableColumnsInTheMiddle = HiveConf.getBoolVar(config,
        HiveConf.ConfVars.METADATA_RESTRICTIONS_BLOCK_ADD_TABLE_COLUMNS_IN_MIDDLE);
    blockAddStructFieldsInTheMiddle = HiveConf.getBoolVar(config,
        HiveConf.ConfVars.METADATA_RESTRICTIONS_BLOCK_ADD_STRUCT_COLUMNS_IN_MIDDLE);
  }

  private void onEvent(PreAlterTableEvent alterTableEvent) throws MetaException {
    Table oldTable = alterTableEvent.getOldTable();
    Table newTable = alterTableEvent.getNewTable();
    String tableName = oldTable.getDbName() + "." + oldTable.getTableName();

    enforceColumnChangeRestrictions(tableName, oldTable, newTable);
  }

  private void enforceColumnChangeRestrictions(String tableName,
                                               Table oldTable,
                                               Table newTable) throws MetaException {

    detectDropColumns(tableName, oldTable.getSd().getCols(), newTable.getSd().getCols());
    detectAddColumnsInTheMiddle(tableName, oldTable.getSd().getCols(), newTable.getSd().getCols());
    throwIfNotSameOrExtended(tableName, oldTable.getSd().getCols(), newTable.getSd().getCols());

  }

  static private class PositionAndTypeInfo {
    final int position;
    final TypeInfo typeInfo;

    PositionAndTypeInfo(int position, TypeInfo typeInfo) {
      this.position = position;
      this.typeInfo = typeInfo;
    }

    PositionAndTypeInfo(int position, String typeString) {
      this(position, TypeInfoUtils.getTypeInfoFromTypeString(typeString));
    }

    public boolean equals(Object that) {
      return
          that != null
       && that instanceof PositionAndTypeInfo
       && this.position == ((PositionAndTypeInfo) that).position
       && this.typeInfo.equals(((PositionAndTypeInfo) that).typeInfo);
    }
  }

  private Map<String, PositionAndTypeInfo> buildIndex(List<FieldSchema> columns) {
    Map<String, PositionAndTypeInfo> index = Maps.newHashMapWithExpectedSize(columns.size());
    for (int i=0; i<columns.size(); ++i) {
      index.put(columns.get(i).getName(), new PositionAndTypeInfo(i, columns.get(i).getType()));
    }
    return index;
  }

  private Map<String, PositionAndTypeInfo> buildIndex(List<String> columnNames, List<TypeInfo> columnTypes) {
    Map<String, PositionAndTypeInfo> index = Maps.newHashMapWithExpectedSize(columnNames.size());
    assert columnNames.size() == columnTypes.size()
        : "Unexpected sizes for columnNames " + columnNames.size() + " and columnTypes " + columnTypes.size();
    for(int i = 0; i < columnNames.size(); ++i) {
      index.put(columnNames.get(i), new PositionAndTypeInfo(i, columnTypes.get(i)));
    }
    return index;
  }

  private static Set<String> getColumnNames(List<FieldSchema> fields) {
    return Sets.newHashSet(
        Iterables.transform(
            fields, new Function <FieldSchema, String>() {
              @Override
              public String apply(FieldSchema input) {
                return input.getName();
              }
            })
    );
  }

  private void detectDropColumns(String qualifiedTableName,
                                 List<FieldSchema> oldColumns,
                                 List<FieldSchema> newColumns) throws MetaException {
    if (!blockDropTableColumns) {
      // Bail early.
      LOG.info("Drop-columns (and thus, renaming/reordering columns) is permitted. " +
          "Skipping check for dropped columns.");
      return;
    }

    Set<String> oldColumnNames = getColumnNames(oldColumns);
    Set<String> newColumnNames = getColumnNames(newColumns);


    final Set<String> droppedColumns = Sets.difference(oldColumnNames, newColumnNames);
    final Set<String> addedColumns   = Sets.difference(newColumnNames, oldColumnNames);

    if (droppedColumns.size() > 0) {
      // Some columns definitely missing.
      // If one has been replaced, it could be a renamed-column (ALTER TABLE CHANGE COLUMN).

      if (droppedColumns.size() == 1 && addedColumns.size() == 1) {
        throw new MetaException(
            "Renaming columns is not permitted. "
                + "Detected that column \"" + droppedColumns.iterator().next() + "\""
                + " is being replaced with column \"" + addedColumns.iterator().next()+ "\""
                + " in table " + qualifiedTableName);
      }
      else {
        // Either more than one column is being dropped,
        // or several new columns are being added.
        // Either way, it's not a column-rename/replacement.
        throw new MetaException(
            "Dropping columns is not permitted. "
                + "Detected that the following columns are being dropped in table " + qualifiedTableName + ": "
                + droppedColumns
        );
      }
    }
    else {

      assert droppedColumns.size() == 0
          : "Can't have a negative number of columns missing from old table definition.";
      // This could be a case of columns being added, with no columns being dropped.
      // Check that the all the old columns maintain their relative positions. No reordering permitted.
      // (If new columns are inserted in the middle/end, ignore them.)

      throwIfOldColumnsReordered(qualifiedTableName, oldColumns, newColumns, addedColumns);

    }
    LOG.info("No dropped columns detected in table: " + qualifiedTableName);
  }


  private void throwIfOldStructFieldsAreReordered(String qualifiedTableName,
                                                  String structColumnName,
                                                  List<String> oldStructFields,
                                                  List<String> newStructFields,
                                                  final Set<String> ignoredStructFields) throws MetaException {

    Iterator<String> iOld = oldStructFields.iterator();
    Iterator<String> iNew = Iterators.filter(newStructFields.iterator(), new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return !ignoredStructFields.contains(input);
      }
    });

    int pos = 0;
    while (iOld.hasNext()) {
      assert iNew.hasNext()
          : "The new columns list should have at least all the fields from the old schema";

      ++pos;
      String oldFieldName = iOld.next();
      String newFieldName = iNew.next();

      if (!oldFieldName.equals(newFieldName)) {
        throw new MetaException(
            "Fields reordered in table " + qualifiedTableName +
            (structColumnName == null? "" : (", struct " + structColumnName)) + "! " +
            "Change detected at index " + pos + ". " +
            "OldField == " + oldFieldName + ". NewField == " + newFieldName);
      }
    }
    LOG.info("Did not detect column-reordering in table " + qualifiedTableName
           + (structColumnName == null? "" : (", struct " + structColumnName)));
  }

  private void throwIfOldColumnsReordered(String qualifiedTableName,
                                          List<FieldSchema> oldColumns,
                                          List<FieldSchema> newColumns,
                                          final Set<String> ignoredColumns) throws MetaException {

    Iterator<FieldSchema> iOld = oldColumns.iterator();
    Iterator<FieldSchema> iNew = Iterators.filter(newColumns.iterator(), new Predicate<FieldSchema>() {
      @Override
      public boolean apply(FieldSchema input) {
        return !ignoredColumns.contains(input.getName());
      }
    });

    int pos = 0;
    while (iOld.hasNext()) {
      assert iNew.hasNext()
          : "The new columns list should have at least all the columns from the old schema";

      ++pos;
      String oldColumnName = iOld.next().getName();
      String newColumnName = iNew.next().getName();

      if (!oldColumnName.equals(newColumnName)) {
        throw new MetaException(
            "Columns reordered in table " + qualifiedTableName + "! Change detected at index " + pos + ". " +
                "OldColumn == " + oldColumnName + ". NewColumn == " + newColumnName);
      }
    }
    LOG.info("Did not detect column-reordering in table " + qualifiedTableName);
  }

  private void detectAddColumnsInTheMiddle(String qualifiedTableName,
                                           List<FieldSchema> oldColumns,
                                           List<FieldSchema> newColumns
                                           ) throws MetaException {

    if (!blockAddTableColumnsInTheMiddle) {
      LOG.info("Add-columns is not blocked for any position in the table-schema.");
      return;
    }

    Map<String, PositionAndTypeInfo> newColumnIndex = buildIndex(newColumns);

    Set<String> oldColumnNames = getColumnNames(oldColumns);
    Set<String> newColumnNames = newColumnIndex.keySet();

    Set<String> addedColumns   = Sets.difference(newColumnNames, oldColumnNames);

    // Check that all added columns at the end of a table's column schema.
    // One gotcha: Columns might have been dropped or re-ordered in the new schema.

    // Find highest index of all old-columns that are also in the new column schema.
    int positionOfLastOldColumn = -1;
    String  nameOfLastOldColumn = "";
    for (String oldColumn : Sets.intersection(oldColumnNames, newColumnNames)) {
      if (newColumnIndex.get(oldColumn).position > positionOfLastOldColumn) {
        positionOfLastOldColumn = newColumnIndex.get(oldColumn).position;
        nameOfLastOldColumn = oldColumn;
      }
    }

    // For each addedColumn, ensure that its column-index exceeds that of all old columns that haven't been dropped.
    for (String addedColumn : addedColumns) {
      if (newColumnIndex.get(addedColumn).position < positionOfLastOldColumn) {
        throw new MetaException("Addition of columns in the middle of a table-schema is not supported! " +
            "Found column \"" + addedColumn + "\" before column \"" + nameOfLastOldColumn + "\".");
      }
    }

    // At this point, any added columns are at the end of the schema.
    // Ensure that no old columns were re-ordered. Ignore newly added columns.

    throwIfOldColumnsReordered(qualifiedTableName, oldColumns, newColumns, addedColumns);
  }

  private void detectDropStructFields(String qualifiedTableName,
                                      String structName,
                                      List<String> oldFields,
                                      List<String> newFields) throws MetaException {
    if (!blockDropStructColumns) {
      // Bail early.
      LOG.info("Dropping struct fields (and thus, renaming/reordering columns) is permitted. " +
          "Skipping check for dropped struct fields.");
      return;
    }

    Set<String> oldFieldNames = Sets.newHashSet(oldFields);
    Set<String> newFieldNames = Sets.newHashSet(newFields);

    final Set<String> droppedFields = Sets.difference(oldFieldNames, newFieldNames);
    final Set<String> addedFields   = Sets.difference(newFieldNames, oldFieldNames);

    if (droppedFields.size() > 0) {
      throw new MetaException(
        "Dropping struct fields is not permitted. Detected that the following fields are being dropped " +
            "in struct " + structName + " in table " + qualifiedTableName + ": " + droppedFields );
    }
    else {

      assert droppedFields.size() == 0
          : "Can't have a negative number of dropped struct fields.";
      // This could be a case of fields being added, with no fields being dropped.
      // Check that the all the old fields maintain their relative positions. No reordering permitted.
      // (If new fields are inserted in the middle/end, ignore them.)

      throwIfOldStructFieldsAreReordered(qualifiedTableName, structName, oldFields, newFields, addedFields);

    }
    LOG.info("No dropped columns detected in table: " + qualifiedTableName);
  }

  private void detectAdditionOfStructFieldsInTheMiddle(String qualifiedTableName,
                                                       String structName,
                                                       List<String> oldFieldNames,
                                                       List<String> newFieldNames,
                                                       List<TypeInfo> newFieldTypes)
      throws MetaException {

    if (!blockAddStructFieldsInTheMiddle) {
      LOG.info("Addition of fields is not blocked for structs in the table-schema.");
      return;
    }

    Map<String, PositionAndTypeInfo> newFieldIndex = buildIndex(newFieldNames, newFieldTypes);

    Set<String> oldFieldNameSet = Sets.newHashSet(oldFieldNames);
    Set<String> newFieldNameSet = newFieldIndex.keySet();

    Set<String> addedFields   = Sets.difference(newFieldNameSet, oldFieldNameSet);

    // Check that all added fields are at the end of the struct.
    // Note: Fields might have been dropped or re-ordered in the new schema.

    // Find highest index of all old-fields that are also in the new field schema of the struct.
    int positionOfLastOldField = -1;
    String  nameOfLastOldField = "";
    for (String oldField : Sets.intersection(oldFieldNameSet, newFieldNameSet)) {
      if (newFieldIndex.get(oldField).position > positionOfLastOldField) {
        positionOfLastOldField = newFieldIndex.get(oldField).position;
        nameOfLastOldField = oldField;
      }
    }

    // For each addedField, ensure that its field-index exceeds that of all old fields that haven't been dropped.
    for (String addedField : addedFields) {
      if (newFieldIndex.get(addedField).position < positionOfLastOldField) {
        throw new MetaException("Addition of fields in the middle of a struct is not supported! " +
            "Found field \"" + addedField + "\" before field \"" + nameOfLastOldField + "\".");
      }
    }

    // All added fields are at the end of the struct.
    // Ensure that no old columns were re-ordered. Ignore newly added columns.

    throwIfOldStructFieldsAreReordered(qualifiedTableName, structName, oldFieldNames, newFieldNames, addedFields);
  }

  private void throwIfNotSameOrExtended(String qualifiedTableName,
                                        List<FieldSchema> oldColumns,
                                        List<FieldSchema> newColumns) throws MetaException {
    Map<String, PositionAndTypeInfo> oldColumnIndex = buildIndex(oldColumns);
    Map<String, PositionAndTypeInfo> newColumnIndex = buildIndex(newColumns);

    // For every column in the old schema, check if the type in the new schema is of a compatible type.
    for (String column : oldColumnIndex.keySet()) {
      throwIfNotSameOrExtended(qualifiedTableName,
                               column,
                               oldColumnIndex.get(column).typeInfo,
                               (newColumnIndex.containsKey(column)? newColumnIndex.get(column).typeInfo : null));
    }

    LOG.info("All columns in table " + qualifiedTableName + " evolved correctly.");
  }

  private void throwIfNotSameOrExtended(String tableName,
                                        String qualifiedColumnName,
                                        TypeInfo oldColumnTypeInfo,
                                        TypeInfo newColumnTypeInfo) throws MetaException {

    if (newColumnTypeInfo == null) {
      LOG.info("Skipping type-evolution check for " + tableName + "." + qualifiedColumnName
             + ". Not found in the new schema.");
      return;
    }

    String oldColumnTypeName = oldColumnTypeInfo.getTypeName();
    String newColumnTypeName = newColumnTypeInfo.getTypeName();

    if (!oldColumnTypeInfo.equals(newColumnTypeInfo)) {
      LOG.info("Possible type mismatch detected for column:" + qualifiedColumnName);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Old type for " + qualifiedColumnName + " == " + oldColumnTypeName);
        LOG.debug("New type for " + qualifiedColumnName + " == " + newColumnTypeName);
      }

      switch (oldColumnTypeInfo.getCategory()) {

        case PRIMITIVE: {
          if (!newColumnTypeInfo.accept(oldColumnTypeInfo)) {
            throw new MetaException("Column-type changed for " + qualifiedColumnName + ". " +
                newColumnTypeName + " is not a promotion of " + oldColumnTypeName);
          }
          break;
        }

        case LIST: {
          if (!newColumnTypeInfo.getCategory().equals(ObjectInspector.Category.LIST)) {
            throw new MetaException("Column-type for " + qualifiedColumnName + " is no longer a LIST!");
          }
          // Compare element types.
          throwIfNotSameOrExtended(
              tableName,
              qualifiedColumnName + ".element_type",
              ((ListTypeInfo) (oldColumnTypeInfo)).getListElementTypeInfo(),
              ((ListTypeInfo) (newColumnTypeInfo)).getListElementTypeInfo()
          );
          break;
        }

        case MAP: {
          if (!newColumnTypeInfo.getCategory().equals(ObjectInspector.Category.MAP)) {
            throw new MetaException("Column-type for " + qualifiedColumnName + " is no longer a MAP!");
          }
          MapTypeInfo oldMapTypeInfo = (MapTypeInfo) oldColumnTypeInfo;
          MapTypeInfo newMapTypeInfo = (MapTypeInfo) newColumnTypeInfo;
          // Compare key-types.
          throwIfNotSameOrExtended(
              tableName,
              qualifiedColumnName + ".key_type",
              oldMapTypeInfo.getMapKeyTypeInfo(),
              newMapTypeInfo.getMapKeyTypeInfo()
          );
          // Compare value-types.
          throwIfNotSameOrExtended(
              tableName,
              qualifiedColumnName + ".value_type",
              oldMapTypeInfo.getMapValueTypeInfo(),
              newMapTypeInfo.getMapValueTypeInfo()
          );
          break;
        }

        case STRUCT: {
          if (!newColumnTypeInfo.getCategory().equals(ObjectInspector.Category.STRUCT)) {
            throw new MetaException("Column-type for " + qualifiedColumnName + " is no longer a STRUCT!");
          }
          StructTypeInfo oldStructTypeInfo = (StructTypeInfo) oldColumnTypeInfo;
          StructTypeInfo newStructTypeInfo = (StructTypeInfo) newColumnTypeInfo;

          detectDropStructFields(tableName,
                                 qualifiedColumnName,
                                 oldStructTypeInfo.getAllStructFieldNames(),
                                 newStructTypeInfo.getAllStructFieldNames());

          detectAdditionOfStructFieldsInTheMiddle(tableName,
                                                  qualifiedColumnName,
                                                  oldStructTypeInfo.getAllStructFieldNames(),
                                                  newStructTypeInfo.getAllStructFieldNames(),
                                                  newStructTypeInfo.getAllStructFieldTypeInfos()
                                                  );

          Map<String, PositionAndTypeInfo> newStructFieldIndex = buildIndex(newStructTypeInfo.getAllStructFieldNames(),
                                                                            newStructTypeInfo.getAllStructFieldTypeInfos());

          for (int i=0; i<oldStructTypeInfo.getAllStructFieldNames().size(); ++i) {
            String structMemberFieldName = oldStructTypeInfo.getAllStructFieldNames().get(i);
            throwIfNotSameOrExtended(
                tableName,
                qualifiedColumnName + "." + structMemberFieldName,
                oldStructTypeInfo.getAllStructFieldTypeInfos().get(i),
                newStructFieldIndex.containsKey(structMemberFieldName)?
                    newStructFieldIndex.get(structMemberFieldName).typeInfo : null
            );
          }

          break;
        }

        case UNION: { // TODO: Add support re-ordering union-members.

          LOG.warn("Detected union column '" + qualifiedColumnName + "' in table '" + tableName + "'. " +
              "Union-support in Hive is sketchy. Will attempt to detect schema evolution on best effort basis.");

          if (!newColumnTypeInfo.getCategory().equals(ObjectInspector.Category.UNION)) {
            throw new MetaException("Column-type for " + qualifiedColumnName + " is no longer a UNION!");
          }

          UnionTypeInfo oldUnionTypeInfo = (UnionTypeInfo) oldColumnTypeInfo;
          UnionTypeInfo newUnionTypeInfo = (UnionTypeInfo) newColumnTypeInfo;
          if (blockDropStructColumns &&
              oldUnionTypeInfo.getAllUnionObjectTypeInfos().size()
                  > newUnionTypeInfo.getAllUnionObjectTypeInfos().size()) {
            throw new MetaException("Columns being dropped from " + qualifiedColumnName);
          }

          for (int i = 0; i < oldUnionTypeInfo.getAllUnionObjectTypeInfos().size(); ++i) {
            // Check that the underlying type-infos are compatible.
            throwIfNotSameOrExtended(
                tableName,
                qualifiedColumnName + "." + "union_member_type",
                oldUnionTypeInfo.getAllUnionObjectTypeInfos().get(i),
                newUnionTypeInfo.getAllUnionObjectTypeInfos().get(i)
            );
          }
          break;
        }
      }
    }
    LOG.info("Schemas deemed compatible for " + qualifiedColumnName +
        "\toldSchema == {" + oldColumnTypeName +
        "}\tnewSchema == {" + newColumnTypeName + "}");

  }

  private void throwIfUsingUnpermittedPartitionKeyType(Table table) throws MetaException {
    if (enforcePartitionKeyRestrictions) {
      LOG.info("Partition-key restrictions are in place!");
      String tableName = table.getDbName() + "." + table.getTableName();

      for (FieldSchema partitionKeyFieldSchema : table.getPartitionKeys()) {
        if (!permitted_partition_key_types.contains(partitionKeyFieldSchema.getType().toLowerCase())) {
          throw new MetaException("Table " + tableName + " uses disallowed partition key-type: "
              + partitionKeyFieldSchema.getName());
        }
      }
    }
  }

  private void onEvent(PreCreateTableEvent createTableEvent) throws MetaException {
    Table table = createTableEvent.getTable();
    throwIfUsingUnpermittedPartitionKeyType(table);
    String tableName = table.getDbName() + "." + table.getTableName();
    LOG.info("Create-table permitted for " + tableName);
  }

  @Override
  public void onEvent(PreEventContext context) throws MetaException,
                                                      NoSuchObjectException,
                                                      InvalidOperationException {
    if (context instanceof PreAlterTableEvent) {
      onEvent((PreAlterTableEvent)context);
    }
    else
    if (context instanceof PreCreateTableEvent) {
      onEvent((PreCreateTableEvent)context);
    }
    else {
      LOG.debug("Unrestricted operation: " + context.getClass().getSimpleName());
    }
  }
}
