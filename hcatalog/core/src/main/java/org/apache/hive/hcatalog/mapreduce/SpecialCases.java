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
package org.apache.hive.hcatalog.mapreduce;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.avro.AvroSerDe;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hive.common.util.ReflectionUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This class is a place to put all the code associated with
 * Special cases. If there is a corner case required to make
 * a particular format work that is above and beyond the generic
 * use, it belongs here, for example. Over time, the goal is to
 * try to minimize usage of this, but it is a useful overflow
 * class that allows us to still be as generic as possible
 * in the main codeflow path, and call attention to the special
 * cases here.
 *
 * Note : For all methods introduced here, please document why
 * the special case is necessary, providing a jira number if
 * possible.
 */
public class SpecialCases {

  static final private Log LOG = LogFactory.getLog(SpecialCases.class);

  public static void addSpecialCasesParametersToInputJobProperties(
      Configuration conf, InputJobInfo jobInfo, Class<? extends InputFormat> ifClass) throws Exception {

    LOG.debug("SpecialCases::addSpecialCasesParametersToInputJobProperties().");
    // Prefetch Avro table-schema from Schema URL.
    if (ifClass == AvroContainerInputFormat.class) {
      Table table = jobInfo.getTableInfo().getTable();
      LOG.debug("Avro table found: " + table.getDbName() + "." + table.getTableName());
      prefetchAvroSchemaIntoTblProperties(jobInfo.getTableInfo(), conf);
    }
  }

  private static void prefetchAvroSchemaIntoTblProperties(HCatTableInfo tableInfo, Configuration conf) {
    try {
      Table table = tableInfo.getTable();
      if (!table.getParameters().containsKey(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName())
          && table.getParameters().containsKey(AvroSerdeUtils.AvroTableProperties.SCHEMA_URL.getPropName())  ) {
        LOG.info("Attempting to prefetch SCHEMA_LITERAL, from SCHEMA_URL, in the write path.");
        String serDeLib = table.getSd().getSerdeInfo().getSerializationLib();
        Deserializer deserializer = ReflectionUtil.newInstance(
            conf.getClassByName(serDeLib)
                .asSubclass(Deserializer.class),
            conf);
        Properties serdeProps = InternalUtil.getSerdeProperties(tableInfo, tableInfo.getDataColumns());
        // Initializing the Avro SerDe should pre-fetch schema from SCHEMA_URL, and store it in SCHEMA_LITERAL.
        SerDeUtils.initializeSerDe(deserializer, conf, serdeProps, null);
        // If initialize() succeeded, fetch SCHEMA_LITERAL and preserve in table-properties.
        String schemaLiteral = serdeProps.getProperty(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(),
            "");
        if (StringUtils.isNotBlank(schemaLiteral)) {
          LOG.info("Prefetch succeeded for Avro schema for table " + table.getDbName() + "." + table.getTableName());
          if (LOG.isDebugEnabled()) {
            LOG.debug("Prefetched SCHEMA_LITERAL == " + schemaLiteral);
          }
          table.getParameters().put(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(),
              schemaLiteral);
        } else {
          LOG.warn("Could not prefetch Avro schema from SCHEMA_URL!");
        }
      }
      else {
        LOG.info("Skipping Avro-schema prefetch. " +
            "Either avro.schema.literal is (already) available, or avro.schema.url is not.");
      }
    }
    catch(Exception exception) {
      LOG.warn("Could not prefetch Avro schema from SCHEMA_URL: " + exception);
    }
  }

  /**
   * Method to do any file-format specific special casing while
   * instantiating a storage handler to write. We set any parameters
   * we want to be visible to the job in jobProperties, and this will
   * be available to the job via jobconf at run time.
   *
   * This is mostly intended to be used by StorageHandlers that wrap
   * File-based OutputFormats such as FosterStorageHandler that wraps
   * RCFile, ORC, etc.
   *
   * @param jobProperties : map to write to
   * @param jobInfo : information about this output job to read from
   * @param ofclass : the output format in use
   */
  public static void addSpecialCasesParametersToOutputJobProperties(
      Configuration conf, Map<String, String> jobProperties,
      OutputJobInfo jobInfo, Class<? extends OutputFormat> ofclass) {
    if (ofclass == RCFileOutputFormat.class) {
      // RCFile specific parameter
      jobProperties.put(HiveConf.ConfVars.HIVE_RCFILE_COLUMN_NUMBER_CONF.varname,
          Integer.toOctalString(
              jobInfo.getOutputSchema().getFields().size()));
    } else if (ofclass == OrcOutputFormat.class) {
      // Special cases for ORC
      // We need to check table properties to see if a couple of parameters,
      // such as compression parameters are defined. If they are, then we copy
      // them to job properties, so that it will be available in jobconf at runtime
      // See HIVE-5504 for details
      Map<String, String> tableProps = jobInfo.getTableInfo().getTable().getParameters();
      for (OrcFile.OrcTableProperties property : OrcFile.OrcTableProperties.values()){
        String propName = property.getPropName();
        if (tableProps.containsKey(propName)){
          jobProperties.put(propName,tableProps.get(propName));
        }
      }
    } else if (ofclass == AvroContainerOutputFormat.class) {
      // Special cases for Avro. As with ORC, we make table properties that
      // Avro is interested in available in jobconf at runtime
      prefetchAvroSchemaIntoTblProperties(jobInfo.getTableInfo(), conf);
      Map<String, String> tableProps = jobInfo.getTableInfo().getTable().getParameters();
      for (AvroSerdeUtils.AvroTableProperties property : AvroSerdeUtils.AvroTableProperties.values()) {
        String propName = property.getPropName();
        if (tableProps.containsKey(propName)){
          String propVal = tableProps.get(propName);
          jobProperties.put(propName,propVal);
        }
      }

      // If SCHEMA_LITERAL couldn't be constructed from SerDe initialization,
      // attempt construction from column-names/types.
      if (!jobProperties.containsKey(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName())) {
        Properties properties = new Properties();
        properties.put("name", jobInfo.getTableName());

        List<String> colNames = jobInfo.getOutputSchema().getFieldNames();
        List<TypeInfo> colTypes = new ArrayList<TypeInfo>();
        for (HCatFieldSchema field : jobInfo.getOutputSchema().getFields()) {
          colTypes.add(TypeInfoUtils.getTypeInfoFromTypeString(field.getTypeString()));
        }

        jobProperties.put(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(),
            AvroSerDe.getSchemaFromCols(properties, colNames, colTypes, null).toString());
      }
     }
  }

  /**
   * Method to do any storage-handler specific special casing while instantiating a
   * HCatLoader
   *
   * @param conf : configuration to write to
   * @param tableInfo : the table definition being used
   */
  public static void addSpecialCasesParametersForHCatLoader(
      Configuration conf, HCatTableInfo tableInfo) {
    if ((tableInfo == null) || (tableInfo.getStorerInfo() == null)){
      return;
    }
    String shClass = tableInfo.getStorerInfo().getStorageHandlerClass();
    if ((shClass != null) && shClass.equals("org.apache.hadoop.hive.hbase.HBaseStorageHandler")){
      // NOTE: The reason we use a string name of the hive hbase handler here is
      // because we do not want to introduce a compile-dependency on the hive-hbase-handler
      // module from within hive-hcatalog.
      // This parameter was added due to the requirement in HIVE-7072
      conf.set("pig.noSplitCombination", "true");
    }
  }

}
