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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.ConditionalTask;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.MergeJoinWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TezWork;

/**
 * An implementation of PhysicalPlanResolver. It iterator each task with a rule
 * dispatcher for its reducer operator tree, for task with join op in reducer,
 * it will try to add a conditional task associated a list of skew join tasks.
 */
public class SkewJoinResolver implements PhysicalPlanResolver {
  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    Dispatcher disp = new SkewJoinTaskDispatcher(pctx);
    GraphWalker ogw = new DefaultGraphWalker(disp);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getRootTasks());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  /**
   * Iterator a task with a rule dispatcher for its reducer operator tree.
   */
  class SkewJoinTaskDispatcher implements Dispatcher {

    private PhysicalContext physicalContext;

    public SkewJoinTaskDispatcher(PhysicalContext context) {
      super();
      physicalContext = context;
    }

    @Override
    public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
        throws SemanticException {
      Task<? extends Serializable> task = (Task<? extends Serializable>) nd;
      ArrayList<Node> topNodes = new ArrayList<Node>();
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      SkewJoinProcCtx skewJoinProcContext = new SkewJoinProcCtx(task,
          physicalContext.getParseContext());

      if (task.getWork() instanceof MapredWork){
        // MapredWork
        ReduceWork reduceWork = ((MapredWork) task.getWork()).getReduceWork();
        if (reduceWork != null) {
          topNodes.add(reduceWork.getReducer());
        }
        opRules.put(new RuleRegExp("R1",
            CommonJoinOperator.getOperatorName() + "%"),
            SkewJoinProcFactory.getJoinProc());
      } else if (task.getWork() instanceof TezWork){
        // TezWork
        for (BaseWork work : ((TezWork)task.getWork()).getAllWork()) {
          topNodes.addAll(work.getAllRootOperators());
        }
        opRules.put(new RuleRegExp("R1",
            CommonMergeJoinOperator.getOperatorName() + "%"),
            SkewJoinProcFactory.getJoinProc());
      } else {
        return null;
      }

      if (topNodes.size() == 0) {
        return null;
      }

      // The dispatcher fires the processor corresponding to the closest
      // matching rule and passes the context along
      Dispatcher disp = new DefaultRuleDispatcher(SkewJoinProcFactory
          .getDefaultProc(), opRules, skewJoinProcContext);
      GraphWalker ogw = new DefaultGraphWalker(disp);

      ogw.startWalking(topNodes, null);
      return null;
    }

    public PhysicalContext getPhysicalContext() {
      return physicalContext;
    }

    public void setPhysicalContext(PhysicalContext physicalContext) {
      this.physicalContext = physicalContext;
    }
  }

  /**
   * A container of current task and parse context.
   */
  public static class SkewJoinProcCtx implements NodeProcessorCtx {
    private Task<? extends Serializable> currentTask;
    private ParseContext parseCtx;

    public SkewJoinProcCtx(Task<? extends Serializable> task,
        ParseContext parseCtx) {
      currentTask = task;
      this.parseCtx = parseCtx;
    }

    public Task<? extends Serializable> getCurrentTask() {
      return currentTask;
    }

    public void setCurrentTask(Task<? extends Serializable> currentTask) {
      this.currentTask = currentTask;
    }

    public ParseContext getParseCtx() {
      return parseCtx;
    }

    public void setParseCtx(ParseContext parseCtx) {
      this.parseCtx = parseCtx;
    }
  }
}
