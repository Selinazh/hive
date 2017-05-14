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

package org.apache.hive.service.cli.operation;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.metrics.Metrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Schema;
import org.apache.hadoop.hive.ql.session.OperationLog;
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.OperationType;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.RowSetFactory;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.thrift.TActiveOperations;
import org.apache.hive.service.cli.thrift.TOperationStatus;
import org.apache.log4j.Appender;
import org.apache.log4j.Logger;

/**
 * OperationManager.
 *
 */
public class OperationManager extends AbstractService {
  public static final String OPERATIONS_METRIC_NAME = "Operations";

  private final Log LOG = LogFactory.getLog(OperationManager.class.getName());

  private final Map<OperationHandle, Operation> handleToOperation =
      new ConcurrentHashMap<OperationHandle, Operation>();

  public OperationManager() {
    super(OperationManager.class.getSimpleName());
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_ENABLED)) {
      initOperationLogCapture(hiveConf.getVar(
        HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL));
    } else {
      LOG.debug("Operation level logging is turned off");
    }
    super.init(hiveConf);
    initializeMetrics();
  }

  @Override
  public synchronized void start() {
    super.start();
    // TODO
  }

  @Override
  public synchronized void stop() {
    // TODO
    super.stop();
  }

  private void initOperationLogCapture(String loggingMode) {
    // Register another Appender (with the same layout) that talks to us.
    Appender ap = new LogDivertAppender(this, OperationLog.getLoggingLevel(loggingMode));
    Logger.getRootLogger().addAppender(ap);
  }

  public ExecuteStatementOperation newExecuteStatementOperation(HiveSession parentSession,
      String statement, Map<String, String> confOverlay, boolean runAsync)
          throws HiveSQLException {
    ExecuteStatementOperation executeStatementOperation = ExecuteStatementOperation
        .newExecuteStatementOperation(parentSession, statement, confOverlay, runAsync);
    addOperation(executeStatementOperation);
    return executeStatementOperation;
  }

  public GetTypeInfoOperation newGetTypeInfoOperation(HiveSession parentSession) {
    GetTypeInfoOperation operation = new GetTypeInfoOperation(parentSession);
    addOperation(operation);
    return operation;
  }

  public GetCatalogsOperation newGetCatalogsOperation(HiveSession parentSession) {
    GetCatalogsOperation operation = new GetCatalogsOperation(parentSession);
    addOperation(operation);
    return operation;
  }

  public GetSchemasOperation newGetSchemasOperation(HiveSession parentSession,
      String catalogName, String schemaName) {
    GetSchemasOperation operation = new GetSchemasOperation(parentSession, catalogName, schemaName);
    addOperation(operation);
    return operation;
  }

  public MetadataOperation newGetTablesOperation(HiveSession parentSession,
      String catalogName, String schemaName, String tableName,
      List<String> tableTypes) {
    MetadataOperation operation =
        new GetTablesOperation(parentSession, catalogName, schemaName, tableName, tableTypes);
    addOperation(operation);
    return operation;
  }

  public GetTableTypesOperation newGetTableTypesOperation(HiveSession parentSession) {
    GetTableTypesOperation operation = new GetTableTypesOperation(parentSession);
    addOperation(operation);
    return operation;
  }

  public GetColumnsOperation newGetColumnsOperation(HiveSession parentSession,
      String catalogName, String schemaName, String tableName, String columnName) {
    GetColumnsOperation operation = new GetColumnsOperation(parentSession,
        catalogName, schemaName, tableName, columnName);
    addOperation(operation);
    return operation;
  }

  public GetFunctionsOperation newGetFunctionsOperation(HiveSession parentSession,
      String catalogName, String schemaName, String functionName) {
    GetFunctionsOperation operation = new GetFunctionsOperation(parentSession,
        catalogName, schemaName, functionName);
    addOperation(operation);
    return operation;
  }

  public Operation getOperation(OperationHandle operationHandle) throws HiveSQLException {
    Operation operation = getOperationInternal(operationHandle);
    if (operation == null) {
      throw new HiveSQLException("Invalid OperationHandle: " + operationHandle);
    }
    return operation;
  }

  private synchronized Operation getOperationInternal(OperationHandle operationHandle) {
    return handleToOperation.get(operationHandle);
  }

  private synchronized Operation removeTimedOutOperation(OperationHandle operationHandle) {
    Operation operation = handleToOperation.get(operationHandle);
    if (operation != null && operation.isTimedOut(System.currentTimeMillis())) {
      handleToOperation.remove(operationHandle);
      return operation;
    }
    return null;
  }

  private synchronized void addOperation(Operation operation) {
    handleToOperation.put(operation.getHandle(), operation);
    updateOperationsMetric();
  }

  private synchronized Operation removeOperation(OperationHandle opHandle) {
    updateOperationsMetric();
    return handleToOperation.remove(opHandle);
  }

  public OperationStatus getOperationStatus(OperationHandle opHandle)
      throws HiveSQLException {
    return getOperation(opHandle).getStatus();
  }

  public void cancelOperation(OperationHandle opHandle) throws HiveSQLException {
    Operation operation = getOperation(opHandle);
    OperationState opState = operation.getStatus().getState();
    if (opState == OperationState.CANCELED ||
        opState == OperationState.CLOSED ||
        opState == OperationState.FINISHED ||
        opState == OperationState.ERROR ||
        opState == OperationState.UNKNOWN) {
      // Cancel should be a no-op in either cases
      LOG.debug(opHandle + ": Operation is already aborted in state - " + opState);
    }
    else {
      LOG.debug(opHandle + ": Attempting to cancel from state - " + opState);
      operation.cancel();
    }
  }

  public void closeOperation(OperationHandle opHandle) throws HiveSQLException {
    Operation operation = removeOperation(opHandle);
    if (operation == null) {
      throw new HiveSQLException("Operation does not exist!");
    }
    operation.close();
  }

  public TableSchema getOperationResultSetSchema(OperationHandle opHandle)
      throws HiveSQLException {
    return getOperation(opHandle).getResultSetSchema();
  }

  public RowSet getOperationNextRowSet(OperationHandle opHandle)
      throws HiveSQLException {
    return getOperation(opHandle).getNextRowSet();
  }

  public RowSet getOperationNextRowSet(OperationHandle opHandle,
      FetchOrientation orientation, long maxRows)
          throws HiveSQLException {
    return getOperation(opHandle).getNextRowSet(orientation, maxRows);
  }

  public RowSet getOperationLogRowSet(OperationHandle opHandle,
      FetchOrientation orientation, long maxRows)
          throws HiveSQLException {
    // get the OperationLog object from the operation
    OperationLog operationLog = getOperation(opHandle).getOperationLog();
    if (operationLog == null) {
      throw new HiveSQLException("Couldn't find log associated with operation handle: " + opHandle);
    }

    // read logs
    List<String> logs;
    try {
      logs = operationLog.readOperationLog(isFetchFirst(orientation), maxRows);
    } catch (SQLException e) {
      throw new HiveSQLException(e.getMessage(), e.getCause());
    }


    // convert logs to RowSet
    TableSchema tableSchema = new TableSchema(getLogSchema());
    RowSet rowSet = RowSetFactory.create(tableSchema, getOperation(opHandle).getProtocolVersion());
    for (String log : logs) {
      rowSet.addRow(new String[] {log});
    }

    return rowSet;
  }

  private boolean isFetchFirst(FetchOrientation fetchOrientation) {
    //TODO: Since OperationLog is moved to package o.a.h.h.ql.session,
    // we may add a Enum there and map FetchOrientation to it.
    if (fetchOrientation.equals(FetchOrientation.FETCH_FIRST)) {
      return true;
    }
    return false;
  }

  private Schema getLogSchema() {
    Schema schema = new Schema();
    FieldSchema fieldSchema = new FieldSchema();
    fieldSchema.setName("operation_log");
    fieldSchema.setType("string");
    schema.addToFieldSchemas(fieldSchema);
    return schema;
  }

  public OperationLog getOperationLogByThread() {
    return OperationLog.getCurrentOperationLog();
  }

  public List<Operation> removeExpiredOperations(OperationHandle[] handles) {
    List<Operation> removed = new ArrayList<Operation>();
    for (OperationHandle handle : handles) {
      Operation operation = removeTimedOutOperation(handle);
      if (operation != null) {
        LOG.warn("Operation " + handle + " is timed-out and will be closed");
        removed.add(operation);
      }
    }
    return removed;
  }

  public int getOperationCount() {
    return handleToOperation.size();
  }

  private void initializeMetrics() {
    try {
      Metrics.initializeScope(OPERATIONS_METRIC_NAME);
    } catch (Throwable ignore) {
      // Metrics errors are not critical, ignore and continue
      LOG.warn("Failed to initialize Operation metrics, ignoring", ignore);
    }
  }

  private void updateOperationsMetric() {
    try {
      int activeOperations = getOperationCount();
      LOG.info("Updating metrics: activeOperations=" + activeOperations);
      Metrics.set(OPERATIONS_METRIC_NAME, activeOperations);
    } catch (Throwable ignore) {
      LOG.warn("Unable to update Operation metrics", ignore);
    }
  }

  private Map<OperationHandle, Operation> getHandleToOperationClone() {
    return new HashMap<>(handleToOperation);
  }

  public TActiveOperations getActiveOperations() {
    TActiveOperations activeOperations = new TActiveOperations();
    Map<OperationHandle, Operation> handleOperationMap = getHandleToOperationClone();
    for (Map.Entry<OperationHandle, Operation> handle : handleOperationMap.entrySet()) {
      activeOperations.addToActiveOperations(getOperationStatusForHandle(handle.getKey(), handle.getValue()));
    }
    return activeOperations;
  }

  /*
   * This constructs one operation status. The methods to obtain user/session are written to avoid
   * NPE in all possible ways. This API could be called during the Operation construction and one/more
   * could be empty.
   */
  private TOperationStatus getOperationStatusForHandle(OperationHandle operationHandle, Operation op) {
    TOperationStatus operationStatus = new TOperationStatus();
    operationStatus.setUser(getUserName(op));
    operationStatus.setHost(getIpAddress(op));
    operationStatus.setSessionID(getSessionId(op));
    operationStatus.setOperationHandleID(getOperationHandleID(operationHandle));
    operationStatus.setTezSessionID(getTezSessionId(op));
    try {
      operationStatus.setOperationStatus(getOperationStatus(operationHandle).getState().toTOperationState());
    } catch (HiveSQLException ignore) {
      LOG.warn("Cannot get status for ophandle: " + op.getHandle(), ignore);
    }
    operationStatus.setOperationType(op.getType().toTOperationType());
    if (op.getType() == OperationType.EXECUTE_STATEMENT) {
      operationStatus.setQuery(((ExecuteStatementOperation) op).getStatement());
    }
    operationStatus.setTrackingURL(getTrackingURL(op));
    return operationStatus;
  }

  private String getUserName(Operation op) {
    return op.getParentSession() != null ? op.getParentSession().getUserName() : "NA";
  }

  private String getIpAddress(Operation op) {
    return op.getParentSession() != null ? op.getParentSession().getIpAddress() : "NA";
  }

  private String getSessionId(Operation op) {
    if (op.getParentSession() != null && op.getParentSession().getSessionState() != null) {
      return op.getParentSession().getSessionState().getSessionId();
    } else {
      return "NA";
    }
  }

  private String getOperationHandleID(OperationHandle handle) {
    if (handle.getHandleIdentifier() != null) {
      return handle.getHandleIdentifier().getPublicId().toString();
    } else {
      return "NA";
    }
  }

  private String getTezSessionId(Operation op) {
    if (op.getParentSession() != null && op.getParentSession().getSessionState() != null &&
        op.getParentSession().getSessionState().getTezSession() != null) {
      return op.getParentSession().getSessionState().getTezSession().getSessionId();
    } else {
      return "NA";
    }
  }

  private String getTrackingURL(Operation op) {
    if (op.getParentSession() != null && op.getParentSession().getSessionState() != null &&
        op.getParentSession().getSessionState().getTezSession() != null) {
      return op.getParentSession().getSessionState().getTezSession().getAppURL();
    } else {
      return "NA";
    }
  }
}
