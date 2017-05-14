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

package org.apache.hive.hcatalog.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mock MetaStoreEventListener. Stand-in for NotificationListener.
 * Fixes up the JMS Topic Names for tables added to a MetaStore.
 * Follows convention used by NotificationListener:
 *  HCAT_MSGBUS_TOPIC_NAME -> TOPIC_PREFIX_FOR_SITE.$DB_NAME.$TBL_NAME
 */
public class MockMetastoreEventListener extends MetaStoreEventListener {

  private static Logger LOG = LoggerFactory.getLogger(MockMetastoreEventListener.class);
  private long hcatServerId;

  public MockMetastoreEventListener(Configuration config) {
    super(config);
    hcatServerId = Thread.currentThread().getId();
    LOG.info("MockMetaStoreEventListener::MockMetaStoreEventListener(): HCat-server-id:" + hcatServerId);
  }

  @Override
  public void onCreateTable(CreateTableEvent tableEvent) throws MetaException {
    try {
      HiveMetaStore.HMSHandler handler = tableEvent.getHandler();
      HiveConf conf = handler.getHiveConf();
      Table hiveTable = handler.get_table(tableEvent.getTable().getDbName(), tableEvent.getTable().getTableName());
      String topicName = getTopicName(conf, hiveTable);
      hiveTable.getParameters().put(
          HCatConstants.HCAT_MSGBUS_TOPIC_NAME,
          topicName);
      LOG.info("MockMetastoreEventListener (hcat-server#" + hcatServerId + ") " +
          "Setting HCAT_MSGBUS_TOPIC_NAME for " + hiveTable.getDbName() + "." + hiveTable.getTableName() +
          " to " + topicName);
      handler.alter_table(hiveTable.getDbName(), hiveTable.getTableName(), hiveTable);
    }
    catch(Throwable t) {
      t.printStackTrace();
      throw new MetaException(t.getMessage());
    }
  }

  private String getTopicName(Configuration conf, Table table) {
    return getTopicPrefix(conf) + "."
         + table.getDbName().toLowerCase() + "."
         + table.getTableName().toLowerCase();
  }

  private String getTopicPrefix(Configuration conf) {
    return "MockHCatServer." + hcatServerId;
  }
}
