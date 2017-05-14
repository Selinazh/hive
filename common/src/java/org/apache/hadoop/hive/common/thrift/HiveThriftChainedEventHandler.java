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

package org.apache.hadoop.hive.common.thrift;

import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TTransport;

import java.util.ArrayList;
import java.util.List;

public class HiveThriftChainedEventHandler implements TServerEventHandler {

  protected List<TServerEventHandler> eventHandlers = new ArrayList<TServerEventHandler>();

  public HiveThriftChainedEventHandler() {
  }

  public void addEventHandler(TServerEventHandler eventHandler) {
    eventHandlers.add(eventHandler);
  }

  @Override
  public void preServe() {
    for (TServerEventHandler eventHandler : eventHandlers) {
      eventHandler.preServe();
    }
  }

  @Override
  public ServerContext createContext(TProtocol input, TProtocol output) {
    ServerContextList serverContexts = new ServerContextList();
    for (TServerEventHandler eventHandler : eventHandlers) {
      serverContexts.add(eventHandler.createContext(input, output));
    }
    return serverContexts;
  }

  @Override
  public void deleteContext(ServerContext serverContext, TProtocol input, TProtocol output) {
    ServerContextList serverContexts = (ServerContextList) serverContext;
    for (int i = 0; i < eventHandlers.size(); i++) {
      eventHandlers.get(i).deleteContext(serverContexts.get(i), input, output);
    }
  }

  @Override
  public void processContext(ServerContext serverContext, TTransport input, TTransport output) {
    ServerContextList serverContexts = (ServerContextList) serverContext;
    for (int i = 0; i < eventHandlers.size(); i++) {
      eventHandlers.get(i).processContext(serverContexts.get(i), input, output);
    }
  }

  public static class ServerContextList extends ArrayList<ServerContext> implements ServerContext {
  }
}
