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

import org.apache.hadoop.hive.common.metrics.Metrics;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class ThriftCLIMetricsProcessor<I extends TCLIService.Iface> extends TCLIService.Processor<I> {
  private static final Logger LOG = LoggerFactory.getLogger(ThriftCLIMetricsProcessor.class);
  public static String METRICS_ACTIVE_REQUESTS = "HS.ActiveRequests";

  public ThriftCLIMetricsProcessor(I iface) {
    super(iface);
  }

  public static void initMetrics() {
    try {
      Metrics.set(METRICS_ACTIVE_REQUESTS, 0L);
    } catch (IOException e) {
      LOG.error("Couldn't initialize " + METRICS_ACTIVE_REQUESTS + " metrics", e);
    }
  }

  protected ThriftCLIMetricsProcessor(I iface, Map<String, org.apache.thrift.ProcessFunction<I, ? extends org.apache.thrift.TBase>> processMap) {
    super(iface, processMap);
  }

  @Override
  public boolean process(TProtocol in, TProtocol out) throws TException {
    boolean metricsIncremented = false;
    try {
      try {
        if(Metrics.incrementCounter(METRICS_ACTIVE_REQUESTS) != null) {
          metricsIncremented = true;
        } else{
          LOG.error("Couldn't increment counter for " + METRICS_ACTIVE_REQUESTS);
        }
      } catch (IOException e) {
        LOG.error("Error incrementing counter " + METRICS_ACTIVE_REQUESTS, e);
      }
      return super.process(in, out);
    } finally {
      if(metricsIncremented) {
        try {
          Metrics.incrementCounter(METRICS_ACTIVE_REQUESTS, -1L);
        } catch (IOException e) {
          LOG.error("Error decrementing counter " + METRICS_ACTIVE_REQUESTS, e);
        }
      }
    }
  }
}
