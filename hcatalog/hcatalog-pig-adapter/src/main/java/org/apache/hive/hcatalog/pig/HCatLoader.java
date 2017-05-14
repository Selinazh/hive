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

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.Credentials;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatContext;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.Pair;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.InputJobInfo;
import org.apache.hive.hcatalog.mapreduce.SpecialCases;
import org.apache.pig.Expression;
import org.apache.pig.Expression.BinaryExpression;
import org.apache.pig.LoadPredicatePushdown;
import org.apache.pig.PigException;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceStatistics;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.util.UDFContext;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pig {@link org.apache.pig.LoadFunc} to read data from HCat
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HCatLoader extends    HCatBaseLoader
                        implements LoadPredicatePushdown {
  private static final Logger LOG = LoggerFactory.getLogger(HCatLoader.class);

  private static final String PARTITION_FILTER = "partition.filter"; // for future use

  private HCatInputFormat hcatInputFormat = null;
  private String dbName;
  private String tableName;
  private String hcatServerUri;
  private String partitionFilterString;
  private final PigHCatUtil phutil = new PigHCatUtil();
  private Properties udfProperties = null;
  private Job localJob = null;

  // Signature for wrapped loader, see comments in LoadFuncBasedInputDriver.initialize
  final public static String INNER_SIGNATURE = "hcatloader.inner.signature";
  final public static String INNER_SIGNATURE_PREFIX = "hcatloader_inner_signature";
  final private static String HCAT_KEY_INPUT_DATA_SIZE_MB = HCatConstants.HCAT_KEY_JOB_INFO + ".input.size.mb";
  // A hash map which stores job credentials. The key is a signature passed by Pig, which is
  //unique to the load func and input file name (table, in our case).
  private static Map<String, Credentials> jobCredentials = new HashMap<String, Credentials>();
  private static final Map<String, String> cacheForSerializedJobInfo = Maps.newHashMap();

  private static final String PREDICATE_FOR_PUSHDOWN_SUFFIX = TableScanDesc.FILTER_EXPR_CONF_STR;
  private boolean predicatePushdownEnabled = false;
  private Map<String, TypeInfo> typeInfoMap = null;

  private static final Set<Byte> SUPPORTED_PREDICATE_PUSHDOWN_DATA_TYPES = Sets.newHashSet(
      DataType.BOOLEAN,
      DataType.INTEGER,
      DataType.LONG,
      DataType.FLOAT,
      DataType.DOUBLE,
      DataType.DATETIME,
      DataType.CHARARRAY,
      DataType.BIGINTEGER,
      DataType.BIGDECIMAL
  );

  @Override
  public InputFormat<?, ?> getInputFormat() throws IOException {
    if (hcatInputFormat == null) {
      hcatInputFormat = new HCatInputFormat(((InputJobInfo)(HCatUtil.deserialize(cacheForSerializedJobInfo.get(signature)))));
      // Note: The cached inputJobInfo should not be cleaned-up at this point.
      // It is possible that Pig might call getInputFormat() multiple times for the same signature, with different
      // loader instances. We can't use WeakReferences either, because clean-up could happen between calls to
      // getInputFormat().
      // The best we can do is cache the serialized (and compressed) payload. (See InputJobInfo.writeObject()).
      // TODO: These hacks are avoidable with better interfaces in Pig.
    }
    return hcatInputFormat;
  }

  @Override
  public String relativeToAbsolutePath(String location, Path curDir) throws IOException {
    return location;
  }

  private void restoreLoaderSpecificStateFromUDFContext(Job job, Properties udfProps) throws IOException {
    for (Enumeration<Object> emr = udfProps.keys(); emr.hasMoreElements(); ) {
      PigHCatUtil.getConfigFromUDFProperties(udfProps,
          job.getConfiguration(), emr.nextElement().toString());
    }
  }

  private void setProjectionSchemaInfoInUDFContext(Job job, Properties udfProps) throws IOException {
    Job localJob = getLocalJobClone(job);
    RequiredFieldList requiredFieldsInfo = (RequiredFieldList) udfProps.get(PRUNE_PROJECTION_INFO);
    boolean localJobConfHasChanged = false;
    final String PROJECTIONS_PUSHED_DOWN_TO_JOB_CONF = "hcat.loader.projections.pushed.down.to.job.conf";
    if (requiredFieldsInfo != null) {
      // pushProjection() was called.
      if (!udfProps.containsKey(PROJECTIONS_PUSHED_DOWN_TO_JOB_CONF)) { // Protect against pushing projections twice.
        // Required-fields were never set.
        // Store projection information in local job-instance.
        ArrayList<Integer> columnIds   = Lists.newArrayListWithExpectedSize(requiredFieldsInfo.getFields().size());
        ArrayList<String>  columnNames = Lists.newArrayListWithExpectedSize(requiredFieldsInfo.getFields().size());
        for (RequiredField rf : requiredFieldsInfo.getFields()) {
          columnIds.add(rf.getIndex());
          columnNames.add(rf.getAlias());
        }
        ColumnProjectionUtils.appendReadColumns(localJob.getConfiguration(), columnIds, columnNames);
        outputSchema = phutil.getHCatSchema(requiredFieldsInfo.getFields(), signature, this.getClass());
        HCatInputFormat.setOutputSchema(localJob, outputSchema);
        udfProps.put(PROJECTIONS_PUSHED_DOWN_TO_JOB_CONF, true);
        localJobConfHasChanged = true;
      }
      else {
        // OutputSchema was already serialized. Skip serialization. Restore from requiredFieldsInfo.
        outputSchema = phutil.getHCatSchema(requiredFieldsInfo.getFields(), signature, this.getClass());
      }
    }
    else {
      // pushProjection() hasn't been called yet.
      // If this is the Pig backend, no projections were ever pushed. Assume all columns have to be read.
      if (HCatUtil.checkJobContextIfRunningFromBackend(job)) {
        ColumnProjectionUtils.setReadAllColumns(localJob.getConfiguration());
        outputSchema = (HCatSchema) udfProps.get(HCatConstants.HCAT_TABLE_SCHEMA);
        HCatInputFormat.setOutputSchema(localJob, outputSchema);
        localJobConfHasChanged = true;
      }
      // If this is the Pig frontend, pushProjection() might still be called later.
    }

    LOG.debug("outputSchema=" + outputSchema);

    // Store modified localJobConf settings to UDFContext.
    if (localJobConfHasChanged) {
      storeDifferenceToUDFProperties(localJob.getConfiguration(), job.getConfiguration(), udfProps);
    }
  }

  /**
   * (localConf - jobConf) -> udfProperties
   */
  protected static void storeDifferenceToUDFProperties(Configuration localConf,
                                                     Configuration jobConf,
                                                     Properties udfProperties) {
    for (Entry<String, String> localJobKeyValue : localConf) {
      String jobConfValue = jobConf.getRaw(localJobKeyValue.getKey());
      if ( jobConfValue==null || !localJobKeyValue.getValue().equals(jobConfValue) ) {
        udfProperties.put(localJobKeyValue.getKey(), localJobKeyValue.getValue());
      }
    }
  }

  /**
   * Get clone of the current Job, local to this HCatLoader instance.
   */
  protected Job getLocalJobClone(Job job) throws IOException {
    if (localJob == null) {
      localJob = new Job(job.getConfiguration());
      localJob.getCredentials().addAll(job.getCredentials());
    }
    return localJob;
  }

  protected Properties getUdfProperties() {
    return (udfProperties == null?
              udfProperties = UDFContext.getUDFContext().getUDFProperties(this.getClass(), new String[]{signature})
            : udfProperties);
  }

  @Override
  public void setLocation(String location, Job job) throws IOException {
    HCatContext.INSTANCE.setConf(job.getConfiguration()).getConf().get()
        .setBoolean(HCatConstants.HCAT_DATA_TINY_SMALL_INT_PROMOTION, true);

    Properties udfProps = getUdfProperties();
    job.getConfiguration().set(INNER_SIGNATURE, INNER_SIGNATURE_PREFIX + "_" + signature);
    Pair<String, String> dbTablePair = PigHCatUtil.getDBTableNames(location);
    dbName = dbTablePair.first;
    tableName = dbTablePair.second;

    if (udfProps.containsKey(HCatConstants.HCAT_PIG_LOADER_LOCATION_SET)) {

      // setLocation() has been called on this Loader before.
      // Don't call HCatInputFormat.setInput(). Why?
      //  1. Can be expensive. E.g. Metastore.getPartitions().
      //  2. Can't call getPartitions() in backend.
      // Instead, restore settings from UDFContext.
      restoreLoaderSpecificStateFromUDFContext(job, udfProps);

      if (!HCatUtil.checkJobContextIfRunningFromBackend(job)) {
        // Combine credentials and credentials from job takes precedence for freshness
        Credentials crd = jobCredentials.get(INNER_SIGNATURE_PREFIX + "_" + signature);
        job.getCredentials().addAll(crd);
      }
    } else {
      // This is the first time setLocation() was called.
      // Must bear the full cost of setInput().
      Job localJob = getLocalJobClone(job); // Leave actual jobConf unchanged.
      HCatInputFormat.setInput(localJob, dbName, tableName, getPartitionFilterString());

      String serializedInputJobInfo = localJob.getConfiguration().get(HCatConstants.HCAT_KEY_JOB_INFO);
      // Place in cache, awaiting HCatInputFormat construction.
      cacheForSerializedJobInfo.put(signature, serializedInputJobInfo);
      InputJobInfo inputJobInfo = (InputJobInfo) HCatUtil.deserialize(serializedInputJobInfo);

      SpecialCases.addSpecialCasesParametersForHCatLoader(localJob.getConfiguration(),
          inputJobInfo.getTableInfo());

      // Prefetch data-size statistics.
      long dataSizeInMB = getSizeInBytes(inputJobInfo) / 1024 / 1024;
      LOG.info("Prefetched value for dataSizeInMB: " + dataSizeInMB);
      localJob.getConfiguration().setLong(HCAT_KEY_INPUT_DATA_SIZE_MB, dataSizeInMB);

      // Clear PartInfo from inputJobInfo, before serialization (to reduce size of udfProps).
      LOG.info("Number of part infos: " + inputJobInfo.getPartitions().size());
      inputJobInfo.getPartitions().clear();
      String serializedInputJobInfoWithoutPartInfo = HCatUtil.serialize(inputJobInfo);
      localJob.getConfiguration().set(HCatConstants.HCAT_KEY_JOB_INFO, serializedInputJobInfoWithoutPartInfo);

      LOG.info("Before clearing PartInfo, serializedInputJobInfo size == " + serializedInputJobInfo.length());
      LOG.info("After  clearing PartInfo, serializedInputJobInfo size == " + serializedInputJobInfoWithoutPartInfo.length());

      // Store changed properties in localJob into UDF-properties.
      storeDifferenceToUDFProperties(localJob.getConfiguration(), job.getConfiguration(), udfProps);

      // ... and that's all she wrote.
      udfProps.put(HCatConstants.HCAT_PIG_LOADER_LOCATION_SET, true);

      // Store credentials in a private hash map and not the udf context to
      // make sure they are not public.
      Credentials crd = new Credentials();
      crd.addAll(localJob.getCredentials()); // In case they were updated in setInput().
      jobCredentials.put(INNER_SIGNATURE_PREFIX + "_" + signature, crd);
    }

    // setLocation() could be called several times, before and after pushProjection() is called.
    // Must check and store projection-columns to UDFContext.
    setProjectionSchemaInfoInUDFContext(job, udfProps);

    // Handle pushdown predicate.

    predicatePushdownEnabled = job.getConfiguration()
             .getBoolean(HCatConstants.HCAT_PIG_LOADER_PREDICATE_PUSHDOWN_ENABLED,
                        false);

    if (predicatePushdownEnabled) {
      LOG.info("Predicate push-down is enabled for HCatLoader.");
      String pushdownPredicate = udfProps.getProperty(signature + PREDICATE_FOR_PUSHDOWN_SUFFIX);
      if (StringUtils.isNotBlank(pushdownPredicate)) {
        LOG.info("Pushing down predicate.");
        Job localJob = getLocalJobClone(job);
        HCatInputFormat.setPushdownPredicate(localJob, pushdownPredicate);
        storeDifferenceToUDFProperties(localJob.getConfiguration(), job.getConfiguration(), udfProps);
      } else {
        LOG.info("Predicate is empty/blank. Skipping push-down-predicate.");
      }
    }
    else {
      LOG.info("Predicate push-down is disabled for HCatLoader.");
    }
  }

  @Override
  public String[] getPartitionKeys(String location, Job job)
    throws IOException {
    Table table = phutil.getTable(location,
      hcatServerUri != null ? hcatServerUri : PigHCatUtil.getHCatServerUri(job),
      PigHCatUtil.getHCatServerPrincipal(job),
      job);   // Pass job to initialize metastore conf overrides
    List<FieldSchema> tablePartitionKeys = table.getPartitionKeys();
    String[] partitionKeys = new String[tablePartitionKeys.size()];
    for (int i = 0; i < tablePartitionKeys.size(); i++) {
      partitionKeys[i] = tablePartitionKeys.get(i).getName();
    }
    return partitionKeys;
  }

  @Override
  public ResourceSchema getSchema(String location, Job job) throws IOException {
    HCatContext.INSTANCE.setConf(job.getConfiguration()).getConf().get()
      .setBoolean(HCatConstants.HCAT_DATA_TINY_SMALL_INT_PROMOTION, true);

    Table table = phutil.getTable(location,
      hcatServerUri != null ? hcatServerUri : PigHCatUtil.getHCatServerUri(job),
      PigHCatUtil.getHCatServerPrincipal(job),

      // Pass job to initialize metastore conf overrides for embedded metastore case
      // (hive.metastore.uris = "").
      job);
    HCatSchema hcatTableSchema = HCatUtil.getTableSchemaWithPtnCols(table);
    try {
      PigHCatUtil.validateHCatTableSchemaFollowsPigRules(hcatTableSchema);
    } catch (IOException e) {
      throw new PigException(
        "Table schema incompatible for reading through HCatLoader :" + e.getMessage()
          + ";[Table schema was " + hcatTableSchema.toString() + "]"
        , PigHCatUtil.PIG_EXCEPTION_CODE, e);
    }
    storeInUDFContext(signature, HCatConstants.HCAT_TABLE_SCHEMA, hcatTableSchema);
    outputSchema = hcatTableSchema;
    return PigHCatUtil.getResourceSchema(hcatTableSchema);
  }

  @Override
  public void setPartitionFilter(Expression partitionFilter) throws IOException {
    // convert the partition filter expression into a string expected by
    // hcat and pass it in setLocation()

    partitionFilterString = getHCatComparisonString(partitionFilter);

    // store this in the udf context so we can get it later
    storeInUDFContext(signature,
      PARTITION_FILTER, partitionFilterString);
  }

  /**
   * Get statistics about the data to be loaded. Only input data size is implemented at this time.
   */
  @Override
  public ResourceStatistics getStatistics(String location, Job job) throws IOException {
    try {
      ResourceStatistics stats = new ResourceStatistics();
      stats.setmBytes(Long.parseLong(getUdfProperties().getProperty(HCAT_KEY_INPUT_DATA_SIZE_MB)));
      return stats;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  private String getPartitionFilterString() {
    if (partitionFilterString == null) {
      Properties props = UDFContext.getUDFContext().getUDFProperties(
        this.getClass(), new String[]{signature});
      partitionFilterString = props.getProperty(PARTITION_FILTER);
    }
    return partitionFilterString;
  }

  private String getHCatComparisonString(Expression expr) {
    if (expr instanceof BinaryExpression) {
      // call getHCatComparisonString on lhs and rhs, and and join the
      // results with OpType string

      // we can just use OpType.toString() on all Expression types except
      // Equal, NotEqualt since Equal has '==' in toString() and
      // we need '='
      String opStr = null;
      switch (expr.getOpType()) {
      case OP_EQ:
        opStr = " = ";
        break;
      default:
        opStr = expr.getOpType().toString();
      }
      BinaryExpression be = (BinaryExpression) expr;
      return "(" + getHCatComparisonString(be.getLhs()) +
        opStr +
        getHCatComparisonString(be.getRhs()) + ")";
    } else {
      // should be a constant or column
      return expr.toString();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public List<String> getPredicateFields(String location, Job job) throws IOException {
    if (predicatePushdownEnabled) {
      LOG.info("Predicate push-down is enabled for HCatLoader(" + location + ").");
      List<ResourceSchema.ResourceFieldSchema> allFields = Arrays.asList(getSchema(location, job).getFields());
      Iterable<ResourceSchema.ResourceFieldSchema> filteredPredicateFields
          = Iterables.filter(allFields, new Predicate<ResourceSchema.ResourceFieldSchema>() {
        @Override
        public boolean apply(ResourceSchema.ResourceFieldSchema input) {
          return SUPPORTED_PREDICATE_PUSHDOWN_DATA_TYPES.contains(input.getType());
        }
      });

      // Return the names of the filtered predicate-fields.
      return Lists.newArrayList(
          Iterables.transform(filteredPredicateFields, new Function<ResourceSchema.ResourceFieldSchema, String>() {
            @Override
            public String apply(ResourceSchema.ResourceFieldSchema input) {
              return input.getName();
            }
          }));
    }
    else {
      LOG.info("Predicate push-down is disabled for HCatLoader(" + location + ").");
      return (List<String>)Collections.EMPTY_LIST;
    }
  }

  @Override
  public List<Expression.OpType> getSupportedExpressionTypes() {
    return Arrays.asList(
        Expression.OpType.OP_EQ,
        Expression.OpType.OP_NE,
        Expression.OpType.OP_GT,
        Expression.OpType.OP_GE,
        Expression.OpType.OP_LT,
        Expression.OpType.OP_LE,
        Expression.OpType.OP_IN,
        Expression.OpType.OP_BETWEEN,
        Expression.OpType.OP_NULL,
        Expression.OpType.OP_NOT,
        Expression.OpType.OP_AND,
        Expression.OpType.OP_OR
    );
  }

  @Override
  public void setPushdownPredicate(Expression predicate) throws IOException {
    LOG.info("HCatLoaderWithPredicatePushdown::setPushdownPredicate(). Predicate == " + predicate);
    ExprNodeDesc hiveExpression = getHiveExpressionFor(predicate);
    try {
      LOG.debug("HiveExpression: " + hiveExpression);
      ExprNodeGenericFuncDesc genericFuncDesc = (ExprNodeGenericFuncDesc)hiveExpression;
      UDFContext.getUDFContext()
          .getUDFProperties(getClass(), new String[]{signature})
          .setProperty(signature + PREDICATE_FOR_PUSHDOWN_SUFFIX,
              Utilities.serializeExpression(genericFuncDesc));
    }
    catch (Exception exception) {
      throw new IOException("Invalid pushdown-predicate received: " +
          "PigExpr == (" + predicate + "). " +
          "HiveExpr == (" + hiveExpression + ")", exception);
    }
  }

  // Helper functions for predicate push-down.

  private ExprNodeDesc getHiveExpressionFor(Expression expression) throws IOException {

    if (expression instanceof Expression.BinaryExpression) {
      return getHiveExpressionFor((Expression.BinaryExpression)expression);
    }
    else
    if (expression instanceof Expression.UnaryExpression) {
      return getHiveExpressionFor((Expression.UnaryExpression)expression);
    }
    else
    if (expression instanceof Expression.Const) {
      assert expression.getOpType().equals(Expression.OpType.TERM_CONST);
      return new ExprNodeConstantDescConstructor().apply(((Expression.Const)expression).getValue());
    }
    else
    if (expression instanceof Expression.Column) {
      assert expression.getOpType().equals(Expression.OpType.TERM_COL);
      Expression.Column columnExpression = (Expression.Column) expression;
      return new ExprNodeColumnDesc(getTypeInfoMap().get(columnExpression.getName()),
          columnExpression.getName(),
          "TableNameNotSet!", // Table-name isn't required, for predicate-pushdown.
          false);
    }
    else {
      throw new IOException("Could not convert pig's push-down predicate "
          + "(" + expression + ") into a Hive expression.");
    }
  }

  private ExprNodeGenericFuncDesc getHiveExpressionFor(Expression.BinaryExpression binaryPredicate) throws IOException {

    List<ExprNodeDesc> arguments = Lists.newArrayList();
    // Add LHS column expression.
    arguments.add(getHiveExpressionFor(binaryPredicate.getLhs()));

    Expression.OpType opType = binaryPredicate.getOpType();
    if (opType.equals(Expression.OpType.OP_IN)) {
      // Add RHS value-list, as constant values.
      // TODO: Short circuit for values that aren't BigInt/BigDecimal/DateTime.
      arguments.addAll(
          Lists.newArrayList(
              Iterators.transform(
                  ((Expression.InExpression) binaryPredicate.getRhs()).getValues().iterator(),
                  new ExprNodeConstantDescConstructor())
          )
      );
    }
    else {
      arguments.add(getHiveExpressionFor(binaryPredicate.getRhs()));
    }

    try {
      return ExprNodeGenericFuncDesc.newInstance(getHiveFunctionFor(opType), arguments);
    }
    catch (UDFArgumentException exception) {
      throw new IOException("Could not convert pig's push-down predicate "
          + "(" + binaryPredicate + ") into a Hive expression.", exception);
    }

  }

  private static class ExprNodeConstantDescConstructor implements Function<Object, ExprNodeConstantDesc> {

    @Override
    public ExprNodeConstantDesc apply(Object input) {
      if (input instanceof BigInteger) {
        input = new BigDecimal((BigInteger)input);
      }
      else
      if (input instanceof DateTime) {
        input = new Timestamp(((DateTime)input).getMillis());
      }

      return new ExprNodeConstantDesc(input);
    }
  }

  private ExprNodeGenericFuncDesc getHiveExpressionFor(Expression.UnaryExpression unaryPredicate) throws IOException {

    try {
      return ExprNodeGenericFuncDesc.newInstance(
          getHiveFunctionFor(unaryPredicate.getOpType()),
          Collections.singletonList(getHiveExpressionFor(unaryPredicate.getExpression()))
      );
    }
    catch (UDFArgumentException exception) {
      throw new IOException("Could not convert pig's push-down predicate "
          + "(" + unaryPredicate + ") into a Hive expression.", exception);
    }

  }

  private GenericUDF getHiveFunctionFor(Expression.OpType operator) throws IOException {
    switch (operator) {

      // <Binary>
      case OP_AND:      return new GenericUDFOPAnd();
      case OP_OR:       return new GenericUDFOPOr();
      case OP_EQ:       return new GenericUDFOPEqual();
      case OP_NE:       return new GenericUDFOPNotEqual();
      case OP_LT:       return new GenericUDFOPLessThan();
      case OP_LE:       return new GenericUDFOPEqualOrLessThan();
      case OP_GT:       return new GenericUDFOPGreaterThan();
      case OP_GE:       return new GenericUDFOPEqualOrGreaterThan();
      case OP_BETWEEN:  return new GenericUDFBetween();
      case OP_IN:       return new GenericUDFIn();
      // </Binary>

      // <Unary>
      case OP_NOT:      return new GenericUDFOPNot();
      case OP_NULL:     return new GenericUDFOPNull();
      // </Unary>

      default:
        throw new IOException("Unsupported operator for predicate push-down: " + operator);
    }
  }

  private Map<String, TypeInfo> getTypeInfoMap() {
    if (typeInfoMap == null) {

      typeInfoMap = Maps.newHashMapWithExpectedSize(outputSchema.size());

      for (FieldSchema field : HCatSchemaUtils.getFieldSchemas(outputSchema.getFields())) {
        typeInfoMap.put(field.getName(), TypeInfoUtils.getTypeInfoFromTypeString(field.getType()));
      }
    }

    return typeInfoMap;

  }

}
