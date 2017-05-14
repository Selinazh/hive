/*
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

/**
 * This Event Handler takes care of cleaning up resources due to disconnected sessions.
 */
public class DisconnectCleanupEventHandler implements TServerEventHandler {

  private static final Log LOG = LogFactory.getLog(DisconnectCleanupEventHandler.class);

  private static ThriftCLIService thriftCliService = null;
  private static HiveConf hiveConf = null;

  /*
   * ThreadLocal state required for cleaning up resources for each Thrift Worker thread.
   */
  private static ThreadLocal<String> threadLocalUser =
      new ThreadLocal<String>() {
        @Override
        protected synchronized String initialValue() {
          return null;
        }
      };
  private static ThreadLocal<TSessionHandle> threadLocalSessionHandle =
      new ThreadLocal<TSessionHandle>() {
        @Override
        protected synchronized TSessionHandle initialValue() {
          return null;
        }
      };

  public DisconnectCleanupEventHandler(ThriftCLIService sqlService, HiveConf conf) {
    thriftCliService = sqlService;
    hiveConf = conf;
  }

  /*
   * A JDBC Client is associated with a User and SessionHandle and one Worker thread is
   * required per connection. This information is required to cleanup the session
   * if the client disconnects abruptly. Save these information as soon as a session
   * is opened.
  */
  public static void addSessionID(TSessionHandle tSessionHandle, String user) {

    LOG.info("Adding session handle for user " + user + " to cache " + tSessionHandle);
    threadLocalSessionHandle.set(tSessionHandle);
    threadLocalUser.set(user);
  }

  @Override
  public void preServe() {
  }

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    return null;
  }

  protected static String getUser() {
    return threadLocalUser.get();
  }

  protected static TSessionHandle getTSessionHandle() {
    return threadLocalSessionHandle.get();
  }

  protected static SessionHandle getSessionHandle() {
    return new SessionHandle(threadLocalSessionHandle.get());
  }

  protected static boolean isSessionValid() {
    return getTSessionHandle() != null &&
        thriftCliService.getCLIService().getSessionManager().isSessionValid(getSessionHandle());
  }

  /*
   * Close the session similar to how a JDBC Client would close it.
   */
  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {

    assert thriftCliService != null;

    try {
      LOG.info("Cleaning up resources for SessionHandle " + getTSessionHandle() + " on behalf of " + getUser());
      /*
       * Client closed connection or did not establish a session, nothing to do.
       * TSessionHandle or User could be null if there was a disconnection before a session was created.
      */
      if (!isSessionValid() || getTSessionHandle() == null || getUser() == null) {
        return;
      }
      if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
        closeSessionDoAs();
      } else {
        cleanupSession();
      }

    } catch (Throwable e) {
      // Just log the exceptions and don't throw anything that disrupts Thrift Server
      LOG.error("Error closing a disconnected session...", e);

    } finally {

      // Reset the thread local variables, let them not be misused in anyway.
      threadLocalSessionHandle.remove();
      threadLocalUser.remove();
    }
  }

  private void cleanupSession() throws HiveSQLException {
    thriftCliService.CloseSessionHandle(getTSessionHandle());
  }

  private void closeSessionDoAs() throws IOException, InterruptedException {
    UserGroupInformation.createProxyUser(getUser(), UserGroupInformation.getLoginUser()).
        doAs(new PrivilegedExceptionAction<Boolean>() {
          public Boolean run() throws HiveSQLException {
            cleanupSession();
            return true; // We don't care about any return values for now
          }
        });
  }

  @Override
  public void processContext(ServerContext serverContext, TTransport tTransport, TTransport tTransport2) {
  }
}
