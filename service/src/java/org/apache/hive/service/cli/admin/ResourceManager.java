package org.apache.hive.service.cli.admin;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.cli.thrift.TResourceStatusList;

import java.sql.SQLException;

public interface ResourceManager {

  public void init(HiveConf hiveConf);

  public void incrementConnections();

  public void decrementConnections();

  public void hasMaxConnectionsBeenReached(String user) throws SQLException;

  public boolean acquireResource(String method, String user, String host) throws SQLException;

  public void releaseResource(String method, String user, String host);

  public TResourceStatusList getResourceConsumption();
}
