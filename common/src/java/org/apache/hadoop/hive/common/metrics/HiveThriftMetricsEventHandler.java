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

package org.apache.hadoop.hive.common.metrics;

import java.io.IOException;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

/**
 * Event handler for thrift to maintain metrics about connections/requests etc.
 */
public class HiveThriftMetricsEventHandler implements TServerEventHandler {
  private static final Log LOG = LogFactory.getLog(HiveThriftMetricsEventHandler.class);

  public static final String METRICS_TOTAL_CONNECTIONS = "TotalConnections";
  public static final String METRICS_ACTIVE_CONNECTIONS = "ActiveConnections";
  public static final String METRICS_TOTAL_REQUESTS = "TotalRequests";
  protected final String serviceName;

  public HiveThriftMetricsEventHandler(final String serviceName) throws IOException {
    this.serviceName = serviceName;

    Metrics.set(getMetricsName(METRICS_ACTIVE_CONNECTIONS), 0L);
    Metrics.set(getMetricsName(METRICS_TOTAL_CONNECTIONS), 0L);
    Metrics.set(getMetricsName(METRICS_TOTAL_REQUESTS), 0L);
  }

  @Override
  public void preServe() {
  }

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    try {
      Metrics.incrementCounter(getMetricsName(METRICS_ACTIVE_CONNECTIONS));
      Metrics.incrementCounter(getMetricsName(METRICS_TOTAL_CONNECTIONS));
    } catch (IOException e) {
      LOG.error("Couldn't increment counter for active connections", e);
    }
    return null;
  }

  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
    try {
      Metrics.incrementCounter(getMetricsName(METRICS_ACTIVE_CONNECTIONS), -1L);
    } catch (IOException e) {
      LOG.error("Couldn't increment counter for active connections", e);
    }
  }

  @Override
  public void processContext(ServerContext serverContext, TTransport input, TTransport output) {
    try {
      Metrics.incrementCounter(getMetricsName(METRICS_TOTAL_REQUESTS));
    } catch (IOException e) {
      LOG.error("Couldn't increment counter for active connections", e);
    }
  }

  private String getMetricsName(final String metricsKey) {
    return serviceName + "." + metricsKey;
  }
}