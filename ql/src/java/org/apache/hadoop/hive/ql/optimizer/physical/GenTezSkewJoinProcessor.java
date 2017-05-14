package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import com.google.common.base.Joiner;

public class GenTezSkewJoinProcessor {

  private static final Log LOG = LogFactory.getLog(GenTezSkewJoinProcessor.class.getName());

  private GenTezSkewJoinProcessor() {
    assert(false);
  }

  /**
   * Tez skew join rehash the skewed key in ReducerSinkOps. It partitions skewed records from big table and broadcasts
   * the same from small tables.
   *
   * To identify the skewed key, prior to the join, a sampling task on big table is added to generate skewed 
   * key distribution and expected partition numbers for each key.
   *
   * For example, for a query
   *
   * SELECT A.key, B.* FROM A JOIN B on A.key = B.key WHERE A.value > 0;
   *
   * The sampling query to be:
   *
   * INSERT INTO DIRECTORY tmp_file
   *  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
   * STORED AS TEXTFILE
   *  SELECT A.key, count(1) as key_count_sample,
   *         CEIL(key_count_sample / 500) -- expected partition number, given default skewed key threshold 100K
   *  FROM A
   *  TABLESAMPLE (bucket 1 out of 200 on RAND()) -- default sampling rate 1/200
   *  WHERE A.value > 0
   *  GROUP BY A.key
   *  HAVING CEIL(key_count_sample / 500) > 1
   *
   * @param op
   * @param currentTsk
   * @param parseContext
   * @throws SemanticException
   */
  public static void processSkewJoin(CommonMergeJoinOperator op, Task<? extends Serializable> currentTsk,
      ParseContext parseContext) throws SemanticException {
    // no support for RIGHT OUTER JOIN: right outer join will generate wrong results if the number of skewed rows is
    // smaller than partition number. e.g table A (1, v1), (1, v2) table B (1, b), if the partition number is 3, result will be
    // will be (1, V1, b), (1, v2, b), (1, null, b)
    // because expected partition number is calculated using sampling, we can not guarantee the number of skewed row
    // is always larger than partition number.
    for (JoinCondDesc joinCondDesc : op.getConf().getConds()) {
      if (joinCondDesc.getType() == JoinDesc.RIGHT_OUTER_JOIN || joinCondDesc.getType() == JoinDesc.FULL_OUTER_JOIN) {
        return ;
      }
    }

    TableScanOperator tsop = findTableScanOpforBigTable(currentTsk, op, parseContext);
    if (tsop == null) {
      return;
    }

    ReduceSinkOperator reduceSinkOp = findReduceSinkOperator(tsop);
    if (reduceSinkOp == null) {
      return;
    }

    // find the ReducerSinkOperator for big table to find the join key and mark the rehash logic
    ReduceSinkDesc rsDesc = reduceSinkOp.getConf();
    // find the join key column name and type, it marked in ReduceSinkOperator.keyCols
    List<String> skewedColumns = new ArrayList<String>();
    List<String> skewedColTypes = new ArrayList<String>();
    RowSchema schema = reduceSinkOp.getSchema();
    ReduceSinkDesc desc = reduceSinkOp.getConf();
    List<ExprNodeDesc> keyCols = desc.getKeyCols();
    ArrayList<String> keyColNames = desc.getOutputKeyColumnNames();
    for (int i = 0; i < keyCols.size(); i++) {
      ExprNodeDesc keyColDesc = keyCols.get(i);
      if ( keyColDesc instanceof ExprNodeColumnDesc) {
        // join key is a column
        ColumnInfo column = schema.getColumnInfo(Utilities.ReduceField.KEY + "." + keyColNames.get(i));
        if (column == null) {
          return;   // key in values
        }
        skewedColumns.add(column.getAlias());
        skewedColTypes.add(keyColDesc.getTypeString());
      } else if (keyColDesc instanceof ExprNodeConstantDesc){
        // join key is a value, it might be due to a constant folding
        TypeInfo typeInfo = keyColDesc.getTypeInfo();
        switch (typeInfo.getCategory()) {
        case PRIMITIVE:
          skewedColTypes.add(keyColDesc.getTypeString());
          skewedColumns.add(((ExprNodeConstantDesc) keyColDesc).getValue().toString());
          break;
        default:
          // not support complex data type rehash.
          return;
        }
      } else {
        return ;
      }
    }

    if (skewedColumns.size() == 0) {
      return;
    }

    // generate sampling task
    String tmpFile = parseContext.getContext().getMRTmpPath().toUri().toString() + Path.SEPARATOR + "distkeys";
    QueryPlan plan = generateTableSampleTask(tsop, skewedColumns, tmpFile, parseContext);
    if (plan != null) {
      // wire sampling task
      ArrayList<Task<? extends Serializable>> tasks  = plan.getRootTasks();
      tasks.get(0).addDependentTask(currentTsk);
      parseContext.replaceRootTask(currentTsk, tasks);
      // set partition flag for big table
      rsDesc.setRePartitionNeeded(true);
      // set skew key info for all tables
      for (Operator<? extends OperatorDesc> reducerOp: op.getOriginalParentsOp()) {
        // update skew key info for both tables
        ((ReduceSinkDesc)reducerOp.getConf()).setSkewedKeyDistPath(tmpFile);
        ((ReduceSinkDesc)reducerOp.getConf()).setSkewedKeyCols(skewedColumns);
        ((ReduceSinkDesc)reducerOp.getConf()).setSkewedKeyType(skewedColTypes);
        ((ReduceSinkDesc)reducerOp.getConf()).setHasSkewedKey(true);
      }
    }
  }

  private static QueryPlan generateTableSampleTask(TableScanOperator tsop, List<String> joinColumns, String tmpFile,
      ParseContext parseCtx) {
    int maxRows = HiveConf.getIntVar(parseCtx.getConf(), HiveConf.ConfVars.HIVESKEWJOINKEY);
    float sampleRate = parseCtx.getConf().getFloatVar(HiveConf.ConfVars.HIVE_OPTIMIZE_SKEWJOIN_SAMPLING_TEZ);
    if (sampleRate > 1 || sampleRate <= 0 ) {
      return null;
    }

    Table table = tsop.getConf().getTableMetadata();
    ExprNodeGenericFuncDesc predicate = tsop.getConf().getFilterExpr();
    ExprNodeDesc partFilter = tsop.getConf().getPartPrunerExpr();
    int denominator = (int)(1/sampleRate);
    Joiner columnList = Joiner.on(",");
    String columns = columnList.join(joinColumns.iterator());
    StringBuilder sqlCommand = new StringBuilder("INSERT OVERWRITE DIRECTORY ")
	.append( "\"" + tmpFile + "\" ")
	.append(" ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' ")
	.append(" STORED AS TEXTFILE ")
	.append(" SELECT ")
	.append(columns)
	.append(", count(1) as key_count_sample")
	.append(", CEIL(key_count_sample / (" + sampleRate + " * " + maxRows + "))")
	.append(" FROM " + table.getTTable().getDbName() + "." + table.getTTable().getTableName() + " ")
	.append(" TABLESAMPLE (bucket 1"  + " out of " + denominator + " on RAND())");

    StringBuilder filter = new StringBuilder();
    if (predicate != null) {
      filter.append(" (" + predicate.getExprString() + ")");
    }
    if (partFilter != null) {
      if (filter.length() > 0) {
        filter.append(" AND ");
      } 
      filter.append(" (" + partFilter.getExprString() + ")");
    }

    if (filter.length() > 0) {
      sqlCommand.append(" WHERE " + filter.toString());
    }
    sqlCommand.append(" GROUP BY " + columns);
    sqlCommand.append(" HAVING CEIL(key_count_sample / (" + sampleRate + " * " + maxRows + ")) > 1");
 
    HiveConf queryConf = new HiveConf(parseCtx.getConf(), SkewJoinProcFactory.class);
    HiveConf.setBoolVar(queryConf, HiveConf.ConfVars.COMPRESSRESULT, false);
    LOG.info("Skew join table sampling query : " + sqlCommand);
    Driver driver = new Driver(queryConf);
    driver.compile(sqlCommand.toString(), false);
    return driver.getPlan();
  }

  // This is a hack to get the TableScanOperator for big table.
  // Since big table position is only marked in CommonMergeJoinDesc and only available in reducers. we need to trace
  // back the input name for big table, to find the corresponding MapWork.
  private static TableScanOperator findTableScanOpforBigTable(Task<? extends Serializable> currTask,
          CommonMergeJoinOperator op, ParseContext parseContext) {
    MergeJoinWork mergeJoinWork = null;
    // find MergeJoinWork has CommonMergeJoinDesc
    for (BaseWork work : ((TezWork)currTask.getWork()).getAllWork()) {
      if (work instanceof MergeJoinWork 
        && ((MergeJoinWork)work).getMergeJoinOperator() == op) {
        mergeJoinWork = (MergeJoinWork)work;
      }
    }
    if ( mergeJoinWork == null) {
      return null;
    }
    ReduceWork reduceWork = (ReduceWork)(mergeJoinWork).getMainWork();
    String inputName = reduceWork.getTagToInput().get(mergeJoinWork.getMergeJoinOperator().getConf().getPosBigTable());
    for (MapWork mwork : currTask.getMapWork()){
      if (mwork.getName().equals(inputName)){
        // find the TableScanOperator which belongs to the root operators - eliminate sub query case.
        for (Operator<? extends OperatorDesc> value : mwork.getAliasToWork().values()) {
          if (parseContext.getTopOps().containsValue(value)) {
            return (TableScanOperator)value;
          }
        }
      }
    }
    return null;
  }

  private static ReduceSinkOperator findReduceSinkOperator(Operator<? extends OperatorDesc> op) {
    if (op instanceof ReduceSinkOperator) {
      return (ReduceSinkOperator)op;
    }
    for (Operator<? extends OperatorDesc> child : op.getChildOperators()){
      ReduceSinkOperator result = findReduceSinkOperator(child);
      if (result != null) {
        return result;
      }
    }
    return null;
  }
}
