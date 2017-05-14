package org.apache.hive.service.cli.admin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.thrift.TResourceStatus;
import org.apache.hive.service.cli.thrift.TResourceStatusList;

import java.sql.SQLException;

public class UnlimitedResourceManager implements ResourceManager {

  public static final Log LOG = LogFactory.getLog(UnlimitedResourceManager.class.getName());

  @Override
  public void init(HiveConf hiveConf) {

  }

  @Override
  public void incrementConnections() {
  }

  @Override
  public void decrementConnections() {
  }

  @Override
  public void hasMaxConnectionsBeenReached(String user) throws SQLException {
  }

  @Override
  public boolean acquireResource(String method, String user, String host) {
    return true;
  }

  @Override
  public void releaseResource(String method, String user, String host) {
  }

  @Override
  public TResourceStatusList getResourceConsumption() {
    TResourceStatusList resourceStatusList = new TResourceStatusList();
    TResourceStatus resourceStatus = new TResourceStatus();
    resourceStatus.setResourceName("-1");
    resourceStatus.setResourceType("-1");
    resourceStatus.setUsedCapacity("-1");
    resourceStatus.setMaxCapacity("-1");
    return resourceStatusList;
  }
}
