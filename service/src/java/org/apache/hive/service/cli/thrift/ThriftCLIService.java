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

package org.apache.hive.service.cli.thrift;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.security.auth.login.LoginException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.metrics.Metrics;
import org.apache.hadoop.hive.common.thrift.HiveThriftChainedEventHandler;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hive.service.AbstractService;
import org.apache.hive.service.ServiceException;
import org.apache.hive.service.ServiceUtils;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.hive.service.auth.TSetIpAddressProcessor;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.GetInfoType;
import org.apache.hive.service.cli.GetInfoValue;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.TableSchema;
import org.apache.hive.service.cli.admin.ResourceManager;
import org.apache.hive.service.cli.admin.ResourceManagerFactory;
import org.apache.hive.service.cli.operation.Operation;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.cli.session.SessionManager;
import org.apache.hive.service.server.HiveServer2;
import org.apache.log4j.NDC;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

/**
 * ThriftCLIService.
 *
 */
public abstract class ThriftCLIService extends AbstractService implements TCLIService.Iface, Runnable {

  public static final Log LOG = LogFactory.getLog(ThriftCLIService.class.getName());

  protected CLIService cliService;
  private static final TStatus OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS);
  protected static HiveAuthFactory hiveAuthFactory;
  protected static ResourceManager resourceManager;

  protected int portNum;
  protected InetAddress serverIPAddress;
  protected String hiveHost;
  protected TServer server;
  protected org.eclipse.jetty.server.Server httpServer;

  private boolean isStarted = false;
  protected boolean isEmbedded = false;

  protected HiveConf hiveConf;

  protected int minWorkerThreads;
  protected int maxWorkerThreads;
  protected long workerKeepAliveTime;

  protected HiveThriftChainedEventHandler serverEventHandler;
  protected ThreadLocal<ServerContext> currentServerContext;

  public enum ThriftCliFunctions {
    GetDelegationToken,
    CancelDelegationToken,
    RenewDelegationToken,
    OpenSession,
    CloseSession,
    GetInfo,
    ExecuteStatement,
    GetTypeInfo,
    GetCatalogs,
    GetSchemas,
    GetTables,
    GetTableTypes,
    GetColumns,
    GetFunctions,
    GetOperationStatus,
    CancelOperation,
    CloseOperation,
    GetResultSetMetadata,
    FetchResults
  }

  public static final String AUDIT_FORMAT =
    "ugi=%s\t" + // ugi
    "ip=%s\t" + // remote IP
    "cmd=%s"; // command
  public static final Log auditLog = LogFactory.getLog(ThriftCLIService.class.getName() + ".audit");

  private static final ThreadLocal<Formatter> auditFormatter =
    new ThreadLocal<Formatter>() {
      @Override
      protected Formatter initialValue() {
        return new Formatter(new StringBuilder(AUDIT_FORMAT.length() * 4));
      }
    };

  static class ThriftCLIServerContext implements ServerContext {
    private SessionHandle sessionHandle = null;

    public void setSessionHandle(SessionHandle sessionHandle) {
      this.sessionHandle = sessionHandle;
    }

    public SessionHandle getSessionHandle() {
      return sessionHandle;
    }
  }

  public static final String HS_SERVICE_METRICS_PREFIX = "HS";

  public ThriftCLIService(CLIService service, String serviceName) {
    super(serviceName);
    this.cliService = service;
    currentServerContext = new ThreadLocal<ServerContext>();
    serverEventHandler = new HiveThriftChainedEventHandler();
  }

  public CLIService getCLIService() {
    return cliService;
  }

  @Override
  public synchronized void init(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    // Initialize common server configs needed in both binary & http modes
    String portString;
    hiveHost = System.getenv("HIVE_SERVER2_THRIFT_BIND_HOST");
    if (hiveHost == null) {
      hiveHost = hiveConf.getVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST);
    }
    try {
      if (hiveHost != null && !hiveHost.isEmpty()) {
        serverIPAddress = InetAddress.getByName(hiveHost);
      } else {
        serverIPAddress = InetAddress.getLocalHost();
      }
    } catch (UnknownHostException e) {
      throw new ServiceException(e);
    }
    // HTTP mode
    if (HiveServer2.isHTTPTransportMode(hiveConf)) {
      workerKeepAliveTime =
          hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_WORKER_KEEPALIVE_TIME,
              TimeUnit.SECONDS);
      portString = System.getenv("HIVE_SERVER2_THRIFT_HTTP_PORT");
      if (portString != null) {
        portNum = Integer.valueOf(portString);
      } else {
        portNum = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_HTTP_PORT);
      }
    }
    // Binary mode
    else {
      workerKeepAliveTime =
          hiveConf.getTimeVar(ConfVars.HIVE_SERVER2_THRIFT_WORKER_KEEPALIVE_TIME, TimeUnit.SECONDS);
      portString = System.getenv("HIVE_SERVER2_THRIFT_PORT");
      if (portString != null) {
        portNum = Integer.valueOf(portString);
      } else {
        portNum = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT);
      }
    }
    minWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MIN_WORKER_THREADS);
    maxWorkerThreads = hiveConf.getIntVar(ConfVars.HIVE_SERVER2_THRIFT_MAX_WORKER_THREADS);
    initializeRateLimiter();
    super.init(hiveConf);
    if (hiveConf.getBoolVar(ConfVars.METASTORE_METRICS_ENABLED)) {
      try {
        Metrics.init();
        // initialize all metrics to 0, otherwise they become available lazily
        // and a system probing for it may fail if metrics are absent initially
        for (ThriftCliFunctions function : ThriftCliFunctions.values()) {
          Metrics.initializeScope(function.name());
        }
        ThriftCLIMetricsProcessor.initMetrics();
      } catch (Exception e) {
        // log exception, but ignore inability to start
        LOG.error("error in Metrics init: " + e.getClass().getName() + " "
                + e.getMessage(), e);
      }
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    if (!isStarted && !isEmbedded) {
      new Thread(this).start();
      isStarted = true;
    }
  }

  @Override
  public synchronized void stop() {
    if (isStarted && !isEmbedded) {
      if(server != null) {
        server.stop();
        LOG.info("Thrift server has stopped");
      }
      if((httpServer != null) && httpServer.isStarted()) {
        try {
          httpServer.stop();
          LOG.info("Http server has stopped");
        } catch (Exception e) {
          LOG.error("Error stopping Http server: ", e);
        }
      }
      isStarted = false;
    }
    super.stop();
  }

  public int getPortNumber() {
    return portNum;
  }

  public InetAddress getServerIPAddress() {
    return serverIPAddress;
  }

  private void initializeRateLimiter() {
    resourceManager = ResourceManagerFactory.getDefaultResourceManager();
  }

  private final void logAuditEvent(String username, String msg) throws HiveSQLException {
    if (msg == null) {
      return;
    }

    final Formatter fmt = auditFormatter.get();
    ((StringBuilder) fmt.out()).setLength(0);

    String address = getIpAddress();
    if (address == null) {
      address = "unknown-ip-addr";
    }

    auditLog.info(fmt.format(AUDIT_FORMAT, username, address, msg).toString());
  }

  public void startFunction(ThriftCliFunctions function, String msg, String username) throws HiveSQLException {
    logAuditEvent(username, function + msg);
    try {
      Metrics.startScope(function.name());
    } catch (IOException e) {
      LOG.debug("Exception when starting metrics scope"
          + e.getClass().getName() + " " + e.getMessage(), e);
    }
  }

  public void startFunction(ThriftCliFunctions function, String msg, OperationHandle handle) throws HiveSQLException {
    String username = null;

    try {
      Operation op = cliService.getSessionManager().getOperationManager().getOperation(handle);

      if (op != null) {
        HiveSession session = op.getParentSession();

        username = session.getUsername();
      } else {
        username = getUserName();
      }
    } catch (HiveSQLException e) {
      username = getUserName();
    }

    startFunction(function, msg, username);
  }

  public void startFunction(ThriftCliFunctions function, String msg, SessionHandle handle) throws HiveSQLException {
    String username = null;

    try {
      HiveSession session = cliService.getSessionManager().getSession(handle);

      username = session.getUsername();
    } catch (HiveSQLException e) {
      username = getUserName();
    }

    startFunction(function, msg, username);
  }

  public void startFunction(ThriftCliFunctions function, String msg) throws HiveSQLException {
    startFunction(function, msg, getUserName());
  }

  public void startFunction(ThriftCliFunctions function, OperationHandle handle) throws HiveSQLException {
    startFunction(function, "", handle);
  }

  public void startFunction(ThriftCliFunctions function, SessionHandle handle) throws HiveSQLException {
    startFunction(function, "", handle);
  }

  public void startFunction(ThriftCliFunctions function) throws HiveSQLException {
    startFunction(function, "");
  }

  public void endFunction(ThriftCliFunctions function) {
    try {
      Metrics.endScope(function.name());
    } catch (IOException e) {
      LOG.debug("Exception when closing metrics scope" + e);
    }
  }


  private String getRemoteUser() {
    UserGroupInformation ugi;
    try {
      ugi = Utils.getUGI();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return ugi.getUserName();
  }

  private String getRemoteHost() {
    if (hiveAuthFactory != null
        && hiveAuthFactory.getRemoteHost() != null) {
      return hiveAuthFactory.getRemoteHost().toString();
    } else {
      return null;
    }
  }

  @Override
  public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq req)
      throws TException {
    TGetDelegationTokenResp resp = new TGetDelegationTokenResp();

    if (hiveAuthFactory == null) {
      resp.setStatus(unsecureTokenErrorStatus());
    } else {
      try {
        SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
        startFunction(ThriftCliFunctions.GetDelegationToken, sessionHandle);
        String token = cliService.getDelegationToken(
            sessionHandle,
            hiveAuthFactory, req.getOwner(), req.getRenewer());
        resp.setDelegationToken(token);
        resp.setStatus(OK_STATUS);
      } catch (HiveSQLException e) {
        LOG.error("Error obtaining delegation token", e);
        TStatus tokenErrorStatus = HiveSQLException.toTStatus(e);
        tokenErrorStatus.setSqlState("42000");
        resp.setStatus(tokenErrorStatus);
      } finally {
        endFunction(ThriftCliFunctions.GetDelegationToken);
      }
    }
    return resp;
  }

  @Override
  public TCancelDelegationTokenResp CancelDelegationToken(TCancelDelegationTokenReq req)
      throws TException {
    TCancelDelegationTokenResp resp = new TCancelDelegationTokenResp();

    if (hiveAuthFactory == null) {
      resp.setStatus(unsecureTokenErrorStatus());
    } else {
      try {
        SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
        startFunction(ThriftCliFunctions.CancelDelegationToken, sessionHandle);
        cliService.cancelDelegationToken(sessionHandle,
            hiveAuthFactory, req.getDelegationToken());
        resp.setStatus(OK_STATUS);
      } catch (HiveSQLException e) {
        LOG.error("Error canceling delegation token", e);
        resp.setStatus(HiveSQLException.toTStatus(e));
      } finally {
        endFunction(ThriftCliFunctions.CancelDelegationToken);
      }
    }
    return resp;
  }

  @Override
  public TRenewDelegationTokenResp RenewDelegationToken(TRenewDelegationTokenReq req)
      throws TException {
    TRenewDelegationTokenResp resp = new TRenewDelegationTokenResp();
    if (hiveAuthFactory == null) {
      resp.setStatus(unsecureTokenErrorStatus());
    } else {
      try {
        SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
        startFunction(ThriftCliFunctions.RenewDelegationToken, sessionHandle);
        cliService.renewDelegationToken(sessionHandle,
            hiveAuthFactory, req.getDelegationToken());
        resp.setStatus(OK_STATUS);
      } catch (HiveSQLException e) {
        LOG.error("Error obtaining renewing token", e);
        resp.setStatus(HiveSQLException.toTStatus(e));
      } finally {
        endFunction(ThriftCliFunctions.RenewDelegationToken);
      }
    }
    return resp;
  }

  private TStatus unsecureTokenErrorStatus() {
    TStatus errorStatus = new TStatus(TStatusCode.ERROR_STATUS);
    errorStatus.setErrorMessage("Delegation token only supported over remote " +
        "client with kerberos authentication");
    return errorStatus;
  }

  private void checkGlobalResourceAvailability() throws SQLException {
    resourceManager.hasMaxConnectionsBeenReached(getRemoteUser());
  }

  @Override
  public TOpenSessionResp OpenSession(TOpenSessionReq req) throws TException {
    LOG.info("Client protocol version: " + req.getClient_protocol());
    TOpenSessionResp resp = new TOpenSessionResp();
    try {
      String userName = getRemoteUser();
      if (userName == null) {
        userName = req.getUsername();
      }
      checkGlobalResourceAvailability();
      resourceManager.acquireResource(ThriftCliFunctions.OpenSession.name(), getRemoteUser(), getRemoteHost());
      // TODO: send the SessionHandle to the startFunction method.
      startFunction(ThriftCliFunctions.OpenSession, "", getUserName(req));
      SessionHandle sessionHandle = getSessionHandle(req, resp);
      DisconnectCleanupEventHandler.addSessionID(sessionHandle.toTSessionHandle(), userName);
      resp.setSessionHandle(sessionHandle.toTSessionHandle());
      // TODO: set real configuration map
      resp.setConfiguration(new HashMap<String, String>());
      resp.setStatus(OK_STATUS);
      ThriftCLIServerContext context =
        (ThriftCLIServerContext)currentServerContext.get();
      if (context != null) {
        context.setSessionHandle(sessionHandle);
      }
    } catch (Exception e) {
      LOG.warn("Error opening session: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.OpenSession);
    }
    return resp;
  }

  private String getIpAddress() {
    String clientIpAddress;
    // Http transport mode.
    // We set the thread local ip address, in ThriftHttpServlet.
    if (cliService.getHiveConf().getVar(
        ConfVars.HIVE_SERVER2_TRANSPORT_MODE).equalsIgnoreCase("http")) {
      clientIpAddress = SessionManager.getIpAddress();
    }
    else {
      // Kerberos
      if (isKerberosAuthMode()) {
        clientIpAddress = hiveAuthFactory.getIpAddress();
      }
      // Except kerberos, NOSASL
      else {
        clientIpAddress = TSetIpAddressProcessor.getUserIpAddress();
      }
    }
    LOG.debug("Client's IP Address: " + clientIpAddress);
    return clientIpAddress;
  }

  /**
   * Returns the effective username.
   * 1. If hive.server2.allow.user.substitution = false: the username of the connecting user
   * 2. If hive.server2.allow.user.substitution = true: the username of the end user,
   * that the connecting user is trying to proxy for.
   * This includes a check whether the connecting user is allowed to proxy for the end user.
   * @param req
   * @return
   * @throws HiveSQLException
   */
  private String getUserName(TOpenSessionReq req) throws HiveSQLException {
    String userName = getUserName();
    if (userName == null) {
      userName = req.getUsername();
    }

    userName = getShortName(userName);
    String effectiveClientUser = getProxyUser(userName, req.getConfiguration(), getIpAddress());
    LOG.debug("Client's username: " + effectiveClientUser);
    return effectiveClientUser;
  }

  private String getUserName() throws HiveSQLException {
    String userName = null;
    // Kerberos
    if (isKerberosAuthMode()) {
      userName = hiveAuthFactory.getRemoteUser();
    }
    // Except kerberos, NOSASL
    if (userName == null) {
      userName = TSetIpAddressProcessor.getUserName();
    }
    // Http transport mode.
    // We set the thread local username, in ThriftHttpServlet.
    if (cliService.getHiveConf().getVar(
        ConfVars.HIVE_SERVER2_TRANSPORT_MODE).equalsIgnoreCase("http")) {
      userName = SessionManager.getUserName();
    }

    return userName;
  }

  private String getShortName(String userName) {
    String ret = null;
    if (userName != null) {
      int indexOfDomainMatch = ServiceUtils.indexOfDomainMatch(userName);
      ret = (indexOfDomainMatch <= 0) ? userName :
          userName.substring(0, indexOfDomainMatch);
    }

    return ret;
  }

  /**
   * Create a session handle
   * @param req
   * @param res
   * @return
   * @throws HiveSQLException
   * @throws LoginException
   * @throws IOException
   */
  SessionHandle getSessionHandle(TOpenSessionReq req, TOpenSessionResp res)
      throws HiveSQLException, LoginException, IOException {
    String userName = getUserName(req);
    String ipAddress = getIpAddress();
    TProtocolVersion protocol = getMinVersion(CLIService.SERVER_VERSION,
        req.getClient_protocol());
    SessionHandle sessionHandle;
    if (cliService.getHiveConf().getBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS) &&
        (userName != null)) {
      String delegationTokenStr = getDelegationToken(userName);
      sessionHandle = cliService.openSessionWithImpersonation(protocol, userName,
          req.getPassword(), ipAddress, req.getConfiguration(), delegationTokenStr);
    } else {
      sessionHandle = cliService.openSession(protocol, userName, req.getPassword(),
          ipAddress, req.getConfiguration());
    }
    res.setServerProtocolVersion(protocol);
    return sessionHandle;
  }


  private String getDelegationToken(String userName)
      throws HiveSQLException, LoginException, IOException {
    if (userName == null || !cliService.getHiveConf().getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION)
        .equalsIgnoreCase(HiveAuthFactory.AuthTypes.KERBEROS.toString())) {
      return null;
    }
    try {
      return cliService.getDelegationTokenFromMetaStore(userName);
    } catch (UnsupportedOperationException e) {
      // The delegation token is not applicable in the given deployment mode
    }
    return null;
  }

  private TProtocolVersion getMinVersion(TProtocolVersion... versions) {
    TProtocolVersion[] values = TProtocolVersion.values();
    int current = values[values.length - 1].getValue();
    for (TProtocolVersion version : versions) {
      if (current > version.getValue()) {
        current = version.getValue();
      }
    }
    for (TProtocolVersion version : values) {
      if (version.getValue() == current) {
        return version;
      }
    }
    throw new IllegalArgumentException("never");
  }

  @Override
  public TCloseSessionResp CloseSession(TCloseSessionReq req) throws TException {
    TCloseSessionResp resp = new TCloseSessionResp();
    try {
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      startFunction(ThriftCliFunctions.CloseSession, sessionHandle);
      CloseSessionHandle(req.getSessionHandle());
      resp.setStatus(OK_STATUS);
      ThriftCLIServerContext context =
        (ThriftCLIServerContext)currentServerContext.get();
      if (context != null) {
        context.setSessionHandle(null);
      }
    } catch (Exception e) {
      LOG.warn("Error closing session: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.CloseSession);
    }
    return resp;
  }

  public void CloseSessionHandle(TSessionHandle tSessionHandle) throws HiveSQLException {
    try {
      SessionHandle sessionHandle = new SessionHandle(tSessionHandle);
      startFunction(ThriftCliFunctions.CloseSession, sessionHandle);
      NDC.push("SESSION-ID: " + sessionHandle.getHandleIdentifier().toString());
      cliService.closeSession(sessionHandle);
    } finally {
      NDC.pop();
      endFunction(ThriftCliFunctions.CloseSession);
      resourceManager.releaseResource(ThriftCliFunctions.OpenSession.name(), getRemoteUser(), getRemoteHost());
    }
  }

  @Override
  public TGetInfoResp GetInfo(TGetInfoReq req) throws TException {
    TGetInfoResp resp = new TGetInfoResp();
    try {
      checkGlobalResourceAvailability();
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      startFunction(ThriftCliFunctions.GetInfo, sessionHandle);
      GetInfoValue getInfoValue =
          cliService.getInfo(sessionHandle,
              GetInfoType.getGetInfoType(req.getInfoType()));
      resp.setInfoValue(getInfoValue.toTGetInfoValue());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting info: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.GetInfo);
    }
    return resp;
  }

  @Override
  public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq req) throws TException {
    boolean resourceAcquired = false;

    TExecuteStatementResp resp = new TExecuteStatementResp();
    try {
      checkGlobalResourceAvailability();
      resourceAcquired = resourceManager.acquireResource(ThriftCliFunctions.ExecuteStatement.name(), getRemoteUser(), getRemoteHost());
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      NDC.push("SESSION-ID: " + sessionHandle.getHandleIdentifier().toString());
      String statement = req.getStatement();
      startFunction(ThriftCliFunctions.ExecuteStatement,
          "\tstmt={" + statement.replaceAll("[\\r\\n\\t]", " ") + "}", sessionHandle);
      Map<String, String> confOverlay = req.getConfOverlay();
      Boolean runAsync = req.isRunAsync();
      OperationHandle operationHandle = runAsync ?
          cliService.executeStatementAsync(sessionHandle, statement, confOverlay)
          : cliService.executeStatement(sessionHandle, statement, confOverlay);
          resp.setOperationHandle(operationHandle.toTOperationHandle());
          resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error executing statement: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      NDC.pop();
      endFunction(ThriftCliFunctions.ExecuteStatement);
      if (resourceAcquired) {
        resourceManager.releaseResource(ThriftCliFunctions.ExecuteStatement.name(), getRemoteUser(), getRemoteHost());
      }
    }
    return resp;
  }

  @Override
  public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq req) throws TException {
    TGetTypeInfoResp resp = new TGetTypeInfoResp();
    try {
      checkGlobalResourceAvailability();
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      startFunction(ThriftCliFunctions.GetTypeInfo, sessionHandle);
      OperationHandle operationHandle = cliService.getTypeInfo(sessionHandle);
      resp.setOperationHandle(operationHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting type info: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.GetTypeInfo);
    }
    return resp;
  }

  @Override
  public TGetCatalogsResp GetCatalogs(TGetCatalogsReq req) throws TException {
    TGetCatalogsResp resp = new TGetCatalogsResp();
    try {
      checkGlobalResourceAvailability();
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      startFunction(ThriftCliFunctions.GetCatalogs, sessionHandle);
      OperationHandle opHandle = cliService.getCatalogs(sessionHandle);
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting catalogs: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.GetCatalogs);
    }
    return resp;
  }

  @Override
  public TGetSchemasResp GetSchemas(TGetSchemasReq req) throws TException {
    TGetSchemasResp resp = new TGetSchemasResp();
    try {
      checkGlobalResourceAvailability();
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      startFunction(ThriftCliFunctions.GetSchemas, "\tcatalog=" + req.getCatalogName()
          + "\tschema=" + req.getSchemaName(), sessionHandle);
      OperationHandle opHandle = cliService.getSchemas(
          sessionHandle, req.getCatalogName(), req.getSchemaName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting schemas: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.GetSchemas);
    }
    return resp;
  }

  @Override
  public TGetTablesResp GetTables(TGetTablesReq req) throws TException {
    TGetTablesResp resp = new TGetTablesResp();
    try {
      checkGlobalResourceAvailability();
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      startFunction(ThriftCliFunctions.GetTables, "\tcatalog=" + req.getCatalogName()
          + "\tschema=" + req.getSchemaName() + "\ttable=" + req.getTableName(), sessionHandle);
      OperationHandle opHandle = cliService
          .getTables(sessionHandle, req.getCatalogName(),
              req.getSchemaName(), req.getTableName(), req.getTableTypes());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting tables: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.GetTables);
    }
    return resp;
  }

  @Override
  public TGetTableTypesResp GetTableTypes(TGetTableTypesReq req) throws TException {
    TGetTableTypesResp resp = new TGetTableTypesResp();
    try {
      checkGlobalResourceAvailability();
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      startFunction(ThriftCliFunctions.GetTableTypes, sessionHandle);
      OperationHandle opHandle = cliService.getTableTypes(sessionHandle);
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting table types: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.GetTableTypes);
    }
    return resp;
  }

  @Override
  public TGetColumnsResp GetColumns(TGetColumnsReq req) throws TException {
    TGetColumnsResp resp = new TGetColumnsResp();
    try {
      checkGlobalResourceAvailability();
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      startFunction(ThriftCliFunctions.GetColumns, "\tcatalog=" + req.getCatalogName() +
          "\tschema=" + req.getSchemaName() + "\ttable=" + req.getTableName() +
          "\tcolumn=" + req.getColumnName(), sessionHandle);
      OperationHandle opHandle = cliService.getColumns(
          sessionHandle,
          req.getCatalogName(),
          req.getSchemaName(),
          req.getTableName(),
          req.getColumnName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting columns: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.GetColumns);
    }
    return resp;
  }

  @Override
  public TGetFunctionsResp GetFunctions(TGetFunctionsReq req) throws TException {
    TGetFunctionsResp resp = new TGetFunctionsResp();
    try {
      checkGlobalResourceAvailability();
      SessionHandle sessionHandle = new SessionHandle(req.getSessionHandle());
      startFunction(ThriftCliFunctions.GetFunctions, "\tcatalog=" + req.getCatalogName() +
          "\tschema=" + req.getSchemaName() + "\tfunction=" + req.getFunctionName(), sessionHandle);
      OperationHandle opHandle = cliService.getFunctions(
          sessionHandle, req.getCatalogName(),
          req.getSchemaName(), req.getFunctionName());
      resp.setOperationHandle(opHandle.toTOperationHandle());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting functions: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.GetFunctions);
    }
    return resp;
  }

  @Override
  public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq req) throws TException {
    TGetOperationStatusResp resp = new TGetOperationStatusResp();
    try {
      checkGlobalResourceAvailability();
      OperationHandle opHandle = new OperationHandle(req.getOperationHandle());
      startFunction(ThriftCliFunctions.GetOperationStatus, opHandle);
      OperationStatus operationStatus = cliService.getOperationStatus(opHandle);
      resp.setOperationState(operationStatus.getState().toTOperationState());
      HiveSQLException opException = operationStatus.getOperationException();
      if (opException != null) {
        resp.setSqlState(opException.getSQLState());
        resp.setErrorCode(opException.getErrorCode());
        resp.setErrorMessage(opException.getMessage());
      }
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting operation status: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.GetOperationStatus);
    }
    return resp;
  }

  @Override
  public TCancelOperationResp CancelOperation(TCancelOperationReq req) throws TException {
    TCancelOperationResp resp = new TCancelOperationResp();
    try {
      OperationHandle opHandle = new OperationHandle(req.getOperationHandle());
      startFunction(ThriftCliFunctions.CancelOperation, opHandle);
      cliService.cancelOperation(opHandle);
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error cancelling operation: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.CancelOperation);
    }
    return resp;
  }

  @Override
  public TCloseOperationResp CloseOperation(TCloseOperationReq req) throws TException {
    TCloseOperationResp resp = new TCloseOperationResp();
    try {
      OperationHandle opHandle = new OperationHandle(req.getOperationHandle());
      startFunction(ThriftCliFunctions.CloseOperation, opHandle);
      cliService.closeOperation(opHandle);
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error closing operation: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.CloseOperation);
    }
    return resp;
  }

  @Override
  public TGetResultSetMetadataResp GetResultSetMetadata(TGetResultSetMetadataReq req)
      throws TException {
    TGetResultSetMetadataResp resp = new TGetResultSetMetadataResp();
    try {
      checkGlobalResourceAvailability();
      OperationHandle opHandle = new OperationHandle(req.getOperationHandle());
      startFunction(ThriftCliFunctions.GetResultSetMetadata, opHandle);
      TableSchema schema = cliService.getResultSetMetadata(opHandle);
      resp.setSchema(schema.toTTableSchema());
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error getting result set metadata: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.GetResultSetMetadata);
    }
    return resp;
  }

  @Override
  public TFetchResultsResp FetchResults(TFetchResultsReq req) throws TException {
    boolean resourceAcquired = false;
    TFetchResultsResp resp = new TFetchResultsResp();
    try {
      checkGlobalResourceAvailability();
      resourceAcquired = resourceManager.acquireResource(ThriftCliFunctions.FetchResults.name(), getRemoteUser(), getRemoteHost());
      OperationHandle opHandle = new OperationHandle(req.getOperationHandle());
      startFunction(ThriftCliFunctions.FetchResults, opHandle);
      NDC.push("OP-ID:" + opHandle.getHandleIdentifier().toString());
      RowSet rowSet = cliService.fetchResults(opHandle,
          FetchOrientation.getFetchOrientation(req.getOrientation()),
          req.getMaxRows(),
          FetchType.getFetchType(req.getFetchType()));
      resp.setResults(rowSet.toTRowSet());
      resp.setHasMoreRows(false);
      resp.setStatus(OK_STATUS);
    } catch (Exception e) {
      LOG.warn("Error fetching results: ", e);
      resp.setStatus(HiveSQLException.toTStatus(e));
    } finally {
      endFunction(ThriftCliFunctions.FetchResults);
      NDC.pop();
      if (resourceAcquired) {
        resourceManager.releaseResource(ThriftCliFunctions.FetchResults.name(), getRemoteUser(), getRemoteHost());
      }
    }
    return resp;
  }

  /*
   * Get the resource count or metrics from HS2 server
   */
  @Override
  public TResourceStatusList GetResourceConsumptionList() throws TException {
    TResourceStatusList resourceStatusList = resourceManager.getResourceConsumption();
    addHS2Resources(resourceStatusList, "SessionCount",
        cliService.getSessionManager().getOpenSessionCount());
    addHS2Resources(resourceStatusList, "OperationCount",
        cliService.getSessionManager().getOperationManager().getOperationCount());
    return resourceStatusList;
  }

  private void addHS2Resources(TResourceStatusList resourceStatusList, String resType, int usedCap) {
    TResourceStatus resourceStatus = new TResourceStatus();
    resourceStatus.setMaxCapacity("N/A");
    resourceStatus.setUsedCapacity(Integer.toString(usedCap));
    resourceStatus.setResourceName("ALL");
    resourceStatus.setResourceType(resType);
    resourceStatusList.addToResourcesConsumed(resourceStatus);
  }

  /*
   * Gets the status of all active operations in HiveServer2.
   * Intended to be used only by the Admin API
   */
  @Override
  public TActiveOperations GetActiveOperations() {
    return cliService.getSessionManager().getOperationManager().getActiveOperations();
  }

  /**
   * Get the thread info in HiveServer2.
   * Reference org.apache.hadoop.util.ReflectionUtils.printThreadInfo for the string format.
   */
  @Override
  public String GetStacks() {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    ReflectionUtils.printThreadInfo(new PrintStream(stream), "");
    return stream.toString();
  }

  /**
   * Get configuration for HiveServer2. It will filter out any configuration key
   * starts with the list in hive.server2.getconfigurations.blacklist.
   */
  @Override
  public TConfigurations GetConfigurations() {

    HiveConf conf = new HiveConf();
    TConfigurations result = new TConfigurations();
    Iterator<Map.Entry<String, String>> ite = conf.iterator();
    String blacklist = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_GET_CONF_BLACKLIST);
    String[] filter = blacklist.split(",|;");
    while (ite.hasNext()) {
      Map.Entry<String, String> entry = ite.next();
      if (!match(filter, entry.getKey())) {
        TConfigurationEntry confEntry = new TConfigurationEntry();
        confEntry.setParamName(entry.getKey());
        confEntry.setParamValue(entry.getValue());
        result.addToEntries(confEntry);
      }
    }
    return result;
  }

  private boolean match(String[] filter, String key) {
    for (String r : filter) {
      if (key.startsWith(r)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public abstract void run();

  /**
   * If the proxy user name is provided then check privileges to substitute the user.
   * @param realUser
   * @param sessionConf
   * @param ipAddress
   * @return
   * @throws HiveSQLException
   */
  private String getProxyUser(String realUser, Map<String, String> sessionConf,
      String ipAddress) throws HiveSQLException {
    String proxyUser = null;
    // Http transport mode.
    // We set the thread local proxy username, in ThriftHttpServlet.
    if (cliService.getHiveConf().getVar(
        ConfVars.HIVE_SERVER2_TRANSPORT_MODE).equalsIgnoreCase("http")) {
      proxyUser = SessionManager.getProxyUserName();
      LOG.debug("Proxy user from query string: " + proxyUser);
    }

    if (proxyUser == null && sessionConf != null && sessionConf.containsKey(HiveAuthFactory.HS2_PROXY_USER)) {
      String proxyUserFromThriftBody = sessionConf.get(HiveAuthFactory.HS2_PROXY_USER);
      LOG.debug("Proxy user from thrift body: " + proxyUserFromThriftBody);
      proxyUser = proxyUserFromThriftBody;
    }

    if (proxyUser == null) {
      return realUser;
    }

    // check whether substitution is allowed
    if (!hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ALLOW_USER_SUBSTITUTION)) {
      throw new HiveSQLException("Proxy user substitution is not allowed");
    }

    // If there's no authentication, then directly substitute the user
    if (HiveAuthFactory.AuthTypes.NONE.toString().
        equalsIgnoreCase(hiveConf.getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION))) {
      return proxyUser;
    }

    // Verify proxy user privilege of the realUser for the proxyUser
    HiveAuthFactory.verifyProxyAccess(realUser, proxyUser, ipAddress, hiveConf);
    LOG.debug("Verified proxy user: " + proxyUser);
    return proxyUser;
  }

  private boolean isKerberosAuthMode() {
    return cliService.getHiveConf().getVar(ConfVars.HIVE_SERVER2_AUTHENTICATION)
        .equalsIgnoreCase(HiveAuthFactory.AuthTypes.KERBEROS.toString());
  }
}
