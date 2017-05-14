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
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStorePreEventListener;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.StrictAvroSerDe;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class PerFileFormatMetadataRestrictionPreEventListener extends MetaStorePreEventListener {

  public static final Log LOG = LogFactory.getLog(PerFileFormatMetadataRestrictionPreEventListener.class);

  private static final String CONFIG_PREFIX_METADATA_RESTRICTIONS = "hive.metadata.restrictions.for.";
  private static final String CONFIG_PREFIX_PERMITTED_SERDES = "hive.metadata.permitted.serdes.for.";
  private static Map<String, String> serde_to_fileformat = Maps.newHashMap();
  private static Map<String, Map<MetadataOperationType, String>> fileformat_specific_error_messages = Maps.newHashMap();

  static {
    serde_to_fileformat.put(AvroSerDe.class.getName(),       IOConstants.AVROFILE.toLowerCase());
    serde_to_fileformat.put(StrictAvroSerDe.class.getName(), IOConstants.AVROFILE.toLowerCase());

    fileformat_specific_error_messages.put(IOConstants.AVROFILE.toLowerCase(),
        new HashMap<MetadataOperationType, String>() {{
          put(MetadataOperationType.ALTER_TABLE_MODIFY_COLUMNS,
              "Update the avro.schema.literal or avro.schema.url contents instead.");
        }});
  }

  private enum MetadataOperationType {
    ALTER_TABLE_MODIFY_COLUMNS,
    ALTER_TABLE_SET_SERDE,
    CREATE_TABLE_SET_SERDE
  }

  private Map<String, EnumSet<MetadataOperationType>> metadataRestrictions_per_fileFormat = Maps.newHashMap();
  private Map<String, List<String>> permitted_serDe_classes_per_fileFormat = Maps.newHashMap();

  public PerFileFormatMetadataRestrictionPreEventListener(Configuration config) throws HiveException {
    super(config);

    // By default.
    metadataRestrictions_per_fileFormat.put(IOConstants.AVROFILE.toLowerCase(),
        EnumSet.of(MetadataOperationType.ALTER_TABLE_MODIFY_COLUMNS));

    // Scan conf for operation-restriction settings.
    Map<String, String> restrictionsFromConfig = config.getValByRegex(CONFIG_PREFIX_METADATA_RESTRICTIONS + "(.*)");
    for (Map.Entry<String, String> entry : restrictionsFromConfig.entrySet()) {
      String fileFormat = entry.getKey().replaceAll(CONFIG_PREFIX_METADATA_RESTRICTIONS, "").toLowerCase();
      EnumSet<MetadataOperationType> restriction = EnumSet.noneOf(MetadataOperationType.class);
      for (String opType : entry.getValue().split(",")) {
        try {
          LOG.info("Adding restriction on " + opType + " for file-format " + fileFormat);
          restriction.add(MetadataOperationType.valueOf(opType));
        } catch (IllegalArgumentException exception) {
          LOG.error("Invalid MetadataOperationType: " + opType, exception);
        }
      }
      metadataRestrictions_per_fileFormat.put(fileFormat, restriction);
      LOG.info("Final metadata restrictions on file-format:" + fileFormat + " are: " +
          metadataRestrictions_per_fileFormat.get(fileFormat));
    }

    // Scan conf for permitted-serdes per fileformat.
    Map<String, String> permittedSerDeSettings = config.getValByRegex(CONFIG_PREFIX_PERMITTED_SERDES + "(.*)");
    for (Map.Entry<String, String> entry : permittedSerDeSettings.entrySet()) {
      String fileFormat = entry.getKey().replaceAll(CONFIG_PREFIX_PERMITTED_SERDES, "").toLowerCase();
      List<String> permittedSerDeList = Lists.newArrayList();
      for (String serDeClass : entry.getValue().split(",")) {
        permittedSerDeList.add(serDeClass.trim());
      }
      permitted_serDe_classes_per_fileFormat.put(fileFormat, permittedSerDeList);
      LOG.info("Final metadata restrictions on file-format:" + fileFormat + " are: " +
          permitted_serDe_classes_per_fileFormat.get(fileFormat));
    }
  }

  private static boolean equivalent(Iterable<FieldSchema> oldColumns, Iterable<FieldSchema> newColumns) {
    Iterator<FieldSchema> iOldColumn = oldColumns.iterator();
    Iterator<FieldSchema> iNewColumn = newColumns.iterator();

    while ( iOldColumn.hasNext() && iNewColumn.hasNext() ) {
      if (!equivalent(iOldColumn.next(), iNewColumn.next())) {
        return false;
      }
    }

    return !iOldColumn.hasNext() && !iNewColumn.hasNext();
  }

  private static boolean equivalent(FieldSchema oldColumn, FieldSchema newColumn) {
    org.apache.hadoop.hive.serde2.typeinfo.TypeInfo oldColumnTypeInfo =
                                           TypeInfoUtils.getTypeInfoFromTypeString(oldColumn.getType());
    org.apache.hadoop.hive.serde2.typeinfo.TypeInfo newColumnTypeInfo =
                                           TypeInfoUtils.getTypeInfoFromTypeString(newColumn.getType());
    return oldColumnTypeInfo.equals(newColumnTypeInfo);
  }

  private static MetadataOperationType getOperationType(PreAlterTableEvent alterTableEvent) throws MetaException {
    Table oldTable = alterTableEvent.getOldTable();
    Table newTable = alterTableEvent.getNewTable();
    if (!equivalent(oldTable.getSd().getCols(), newTable.getSd().getCols())) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Column mismatch detected!");
        LOG.debug("Old columns = " + oldTable.getSd().getCols());
        LOG.debug("New columns = " + newTable.getSd().getCols());
      }
      return MetadataOperationType.ALTER_TABLE_MODIFY_COLUMNS;
    }
    else
    if (        !oldTable.getSd().getSerdeInfo().getSerializationLib()
         .equals(newTable.getSd().getSerdeInfo().getSerializationLib())) {
      return MetadataOperationType.ALTER_TABLE_SET_SERDE;
    }
    return null;
  }

  private static String getFileFormatFor(org.apache.hadoop.hive.metastore.api.Table table) {
    return serde_to_fileformat.get(table.getSd().getSerdeInfo().getSerializationLib());
  }

  private static String getFormatSpecificMessage(String fileFormat, MetadataOperationType operation) {
    if (    fileFormat != null
         && operation != null
         &&  fileformat_specific_error_messages.containsKey(fileFormat)
         &&  fileformat_specific_error_messages.get(fileFormat).containsKey(operation)) {
      return fileformat_specific_error_messages.get(fileFormat).get(operation);
    }

    return "";
  }

  private void throwIfTableUsesUnpermittedSerDe(Table table,
                                                String fileFormat,
                                                MetadataOperationType opType) throws MetaException {
    String serDeClassName = table.getSd().getSerdeInfo().getSerializationLib();
    List<String> permittedSerDeList = permitted_serDe_classes_per_fileFormat.get(fileFormat);
    if (permittedSerDeList != null && !permittedSerDeList.contains(serDeClassName)) {
      String errorMessage = "Table with file-format " + fileFormat + " cannot use " + serDeClassName
          + " as a SerDe. Permitted SerDe classes are: " + permittedSerDeList + ". "
          + getFormatSpecificMessage(fileFormat, opType);
      throw new MetaException(errorMessage);
    }
  }

  private void onEvent(PreAlterTableEvent alterTableEvent) throws MetaException {
    Table oldTable = alterTableEvent.getOldTable();
    Table newTable = alterTableEvent.getNewTable();
    String fileFormat = getFileFormatFor(oldTable);
    String tableName = oldTable.getDbName() + "." + oldTable.getTableName();

    if (metadataRestrictions_per_fileFormat.get(fileFormat) != null) {
      MetadataOperationType opType = getOperationType(alterTableEvent);
      if (LOG.isDebugEnabled()) {
        LOG.debug("MetadataRestrictionPreEventListener detected ALTER_TABLE event. "
            + " tableName: " + tableName
            + " fileFormat: " + fileFormat
            + " opType: " + opType);
      }

      if (opType != null && metadataRestrictions_per_fileFormat.get(fileFormat).contains(opType)) {

        switch (opType) {

          case ALTER_TABLE_MODIFY_COLUMNS: {
            String errorMessage = "Operation: " + opType + " restricted for table " + tableName
                + " with file-format " + fileFormat + ". "
                + getFormatSpecificMessage(fileFormat, opType);
            throw new MetaException(errorMessage);
          }

          case ALTER_TABLE_SET_SERDE: {
            throwIfTableUsesUnpermittedSerDe(newTable, getFileFormatFor(newTable), opType);
            break;
          }

          default:
            assert false : "Unexpected opType : " + opType;
        }
      }

      LOG.info(opType + " permitted for " + tableName);
    }
  }

  private void onEvent(PreCreateTableEvent createTableEvent) throws MetaException {
    Table table = createTableEvent.getTable();
    String fileFormat = getFileFormatFor(table);
    String tableName = table.getDbName() + "." + table.getTableName();

    MetadataOperationType opType = MetadataOperationType.CREATE_TABLE_SET_SERDE;
    if (metadataRestrictions_per_fileFormat.get(fileFormat) != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("MetadataRestrictionPreEventListener detected CREATE_TABLE event. "
            + " tableName: " + tableName
            + " fileFormat: " + fileFormat
            + " opType: " + opType);
      }

      throwIfTableUsesUnpermittedSerDe(table, fileFormat, opType);
    }
    LOG.info(opType + "permitted for " + tableName);
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
