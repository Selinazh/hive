package org.apache.hive.hcatalog.api;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.hive.hcatalog.common.HCatException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;
import java.util.Properties;

/**
 * Utilities for making copies of Avro tables (to, say, a different HDFS cluster).
 */
public class HCatAvroTableReplicationUtil {

  private static final Logger LOG = LoggerFactory.getLogger(HCatAvroTableReplicationUtil.class);

  private Configuration conf;

  /**
   * Constructor, to keep a copy of the conf, so that it doesn't need to be passed to each member function.
   * @param conf Configuration object, with cluster settings.
   */
  public HCatAvroTableReplicationUtil(Configuration conf) {
    this.conf = conf;
  }

  public boolean isAvroTable(HCatTable table) throws HCatException {
    return table.getInputFileFormat().equals(AvroContainerInputFormat.class.getName());
  }

  /**
   * Retrieves the schema-URL, if set in TBLPROPERTIES.
   * @param table The HCatTable in question.
   * @return Avro-schema-URL, if set. Null, if unset.
   * @throws HCatException On failure.
   */
  public Path getSchemaURL(HCatTable table) throws HCatException {
    String pathString = table.getTblProps().get(AvroSerdeUtils.AvroTableProperties.SCHEMA_URL.getPropName());
    if (StringUtils.isBlank(pathString)) {
      return null;
    }
    else {
      Path schemaURL = new Path(pathString);
      try {
        if (schemaURL.isAbsoluteAndSchemeAuthorityNull()) {
          FileSystem tableFS = new Path(table.getLocation()).getFileSystem(conf);
          return new Path(tableFS.getUri().toString() + schemaURL);
        }
        else {
          return schemaURL;
        }
      }
      catch (IOException ioException) {
        throw new HCatException("Could not retrieve schema URL for table "
                              + table.getDbName() + "." + table.getTableName(), ioException);
      }
    }
  }

  /**
   * Sets schema-URL in TBLPROPERTIES.
   * @param table The HCatTable in question.
   * @param path schema-URL, i.e. path to schema-file, on HDFS.
   * @throws HCatException On failure.
   */
  public void setSchemaURL(HCatTable table, Path path) throws HCatException {
    table.getTblProps().put(AvroSerdeUtils.AvroTableProperties.SCHEMA_URL.getPropName(), path.toString());
  }

  /**
   * Fetches the schema-literal from TBLPROPERTIES.
   * @param table The HCatTable in question.
   * @return schema-literal, if set. Null, if unset.
   * @throws HCatException
   */
  public String getSchemaLiteral(HCatTable table) throws HCatException {
    String schemaLiteral = table.getTblProps().get(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName());
    return StringUtils.isBlank(schemaLiteral)? null : schemaLiteral;
  }

  /**
   * Removes schema-URL from TBLPROPERTIES.
   * @param table The HCatTable in question.
   * @throws HCatException On failure.
   */
  public void unsetSchemaLiteral(HCatTable table) throws HCatException {
    table.getTblProps().remove(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName());
  }

  /**
   * Compares "source" and "target" tables for their schema-URL, and determines if they differ,
   * as per schema-URL settings. The schemas are deemed different if:
   *   1. The schema-files have different file-names. (E.g. schema.v1.avsc vs schema.v2.avsc).
   *   2. The schema-files have the same name, but different schema definitions.
   *
   * @param source The "source" HCatTable, i.e. the replicant.
   * @param target The "target" HCatTable, i.e. the replica.
   * @return True, if the schema definition of the "source" is deemed different from the target. False, otherwise.
   * @throws HCatException On failure.
   */
  public boolean mustHandleSchemaChangeFromURL(HCatTable source, HCatTable target) throws HCatException {

    Path sourceSchemaURL = getSchemaURL(source);
    if (sourceSchemaURL == null || StringUtils.isNotBlank(getSchemaLiteral(source))) {
      LOG.info("Source table " + source + " either has no avro.schema.url, or has avro.schema.literal set. " +
               "Schema-URL can be ignored.");
      return false;
    }

    if (StringUtils.isNotBlank(getSchemaLiteral(target))) {
      LOG.info("Source table " + source + " uses avro.schema.url, " +
               "but target " + target + " already has avro.schema.literal set. " +
               "Removing avro.schema.literal setting from target.");
      unsetSchemaLiteral(target);
    }

    Path targetSchemaURL = getSchemaURL(target);
    if (targetSchemaURL == null) {
      LOG.info("Source table " + source + " has avro.schema.url, but target " + target + " does not.");
      return true;
    }

    // Now both source and target tables are using avro.schema.url. Check file-names.
    if (!sourceSchemaURL.getName().equals(targetSchemaURL.getName())) { // File-names differ. Assume they're different.
      LOG.info("Source and target tables use schema-URLs with different file-names! Assuming schema changed.");
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source schemaURL == " + sourceSchemaURL + ", Target schemaURL == " + targetSchemaURL);
      }
      return true;
    }

    try {
      // Schema file-names are identical. Check if the schemas are materially different:
      Properties sourceTableProperties = new Properties();
      sourceTableProperties.putAll(source.getTblProps());
      Schema sourceSchema = AvroSerdeUtils.determineSchemaOrThrowException(conf, sourceTableProperties);

      Properties targetTableProperties = new Properties();
      targetTableProperties.putAll(target.getTblProps());
      Schema targetSchema = AvroSerdeUtils.determineSchemaOrThrowException(conf, targetTableProperties);

      if (LOG.isDebugEnabled()) {
        LOG.debug("Comparing Source-schema: " + sourceSchema + ", targetSchema: " + targetSchema);
      }
      return !sourceSchema.equals(targetSchema);
    }
    catch (Exception exception) {
      throw new HCatException("Could not compare Avro table schemas.", exception);
    }
  }

  /**
   * Method to diff Avro tables. If source and target differ in terms of column schema, and if the source has
   * schema URL/literal set, then the column-schema changes are ignored. Schema-changes are deferred to URL/literal.
   * @param source The Source Table.
   * @param target The Target Table.
   * @return The set of table-attributes that differ between source and target
   * (excluding COLUMNS, if schema URL/literal is set).
   * @throws HCatException On failure.
   */
  public EnumSet<HCatTable.TableAttribute> diff(HCatTable source, HCatTable target) throws HCatException {
    EnumSet<HCatTable.TableAttribute> diff = source.diff(target);
    if (isAvroTable(source) && (getSchemaLiteral(source) != null || getSchemaURL(source) != null)
         && diff.contains(HCatTable.TableAttribute.COLUMNS)) {
      LOG.warn("Ignoring column-changes" +
               " between source " + source.getDbName() + "." + source.getTableName() +
                   " and target " + target.getDbName() + "." + target.getTableName() +
               " Schema changes will be accounted for, using the schema URL/literal.");
      diff.remove(HCatTable.TableAttribute.COLUMNS);
    }
    return diff;
  }

}
