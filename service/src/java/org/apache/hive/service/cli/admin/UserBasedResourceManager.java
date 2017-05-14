package org.apache.hive.service.cli.admin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.thrift.TResourceStatus;
import org.apache.hive.service.cli.thrift.TResourceStatusList;
import org.apache.hive.service.cli.thrift.ThriftCLIService;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * This class applies 3 kinds of resource management policies.
 *
 * MaxConnections:
 * For restricting the maximum number of connections to HiveServer2.
 *
 * MaxExecuteStatements:
 * For restricting the maximum number of execute statements that could
 * simultaneously happen on the same server.
 *
 * UserBasedRestrictions:
 * For restricting Users' limitations to certain API's.
 *
 * It allows some users (SuperUsers aka administrative users) to by pass all these restrictions.
 */
public class UserBasedResourceManager implements ResourceManager {

  public static final Log LOG = LogFactory.getLog(UserBasedResourceManager.class.getName());

  private final Map<ResourceIdentity, Semaphore> consumedResources;
  private static Map<String, Integer> maxCapacity;

  private AtomicInteger noConnections = new AtomicInteger(0);
  private static int maxConnections;

  private int maxTotalExecStatements;
  private Semaphore consumedExecStatements = null;

  private List<String> superUsersList = null;

  public UserBasedResourceManager() {
    consumedResources = new ConcurrentHashMap<ResourceIdentity, Semaphore>();
    maxCapacity = new ConcurrentHashMap<String, Integer>();
    superUsersList = new ArrayList<String>(1);
  }

  public void init(HiveConf hiveConf) {

    initializeUserBasedConfiguration(hiveConf);
    initializeMaxConnectionConfiguration(hiveConf);
    initializeExecutionBasedConfiguration(hiveConf);
    loadSuperUsers(hiveConf);
  }

  private void initializeMaxConnectionConfiguration(HiveConf hiveConf) {
    maxConnections = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_CONNECTIONS);
  }

  private void initializeExecutionBasedConfiguration(HiveConf hiveConf) {
    maxTotalExecStatements = hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_TOTALEXECSTATEMENTS);
    if (maxTotalExecStatements > 0) {
      consumedExecStatements = new Semaphore(maxTotalExecStatements);
    }
  }

  private void initializeUserBasedConfiguration(HiveConf hiveConf) {
    addLimitIfEnabled(ThriftCLIService.ThriftCliFunctions.OpenSession,
        hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_OPENSESSIONS));
    addLimitIfEnabled(ThriftCLIService.ThriftCliFunctions.ExecuteStatement,
        hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_EXECSTATEMENTS));
    addLimitIfEnabled(ThriftCLIService.ThriftCliFunctions.FetchResults,
        hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_MAX_FETCHRESULTS));
  }

  private void addLimitIfEnabled(ThriftCLIService.ThriftCliFunctions opType, int maxLimit) {
    if (maxLimit > 0) {
      maxCapacity.put(opType.name(), maxLimit);
    }
  }

  /*
   * Tried to use Hadoop's ProxyUsers so that we could rely on the same configuration files.
   * But its a little heavy weight for every API call.
   */
  private void loadSuperUsers(HiveConf hiveConf) {
    String superusers = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_SUPERUSERS);
    if (superusers != null && superusers.length() > 0) {
      superUsersList.addAll(Arrays.asList(superusers.split(",")));
    }
    String currentUser = null;
    try {
      currentUser = UserGroupInformation.getLoginUser().getShortUserName();
    } catch (IOException e) {
      LOG.error("Could not obtain current user ", e);
    }

    if (currentUser != null && !superUsersList.contains(currentUser)) {
      LOG.info("Adding default user \"" + currentUser + "\" as superuser");
      superUsersList.add(currentUser);
    }
    LOG.info("Adding super users " + superUsersList);
  }

  @Override
  public void incrementConnections() {
    noConnections.incrementAndGet();
  }

  @Override
  public void decrementConnections() {
    noConnections.decrementAndGet();
  }

  @Override
  public void hasMaxConnectionsBeenReached(String user) throws HiveSQLException {
    if (!isSuperUser(user) && (maxConnections > 0 && noConnections.get() > maxConnections)) {
      throw new HiveSQLException("Server reached maximum number connections permissible" , "42000");
    }
  }

  private synchronized Semaphore createResource(ResourceIdentity resourceIdentity) {
    LOG.info("Creating resources for " + resourceIdentity + " with maxcapacity " +
        maxCapacity.get(resourceIdentity.getResourceType()));
    consumedResources.put(resourceIdentity, new Semaphore(maxCapacity.get(resourceIdentity.getResourceType())));
    return consumedResources.get(resourceIdentity);
  }

  private boolean isResourceTypeControlled(ResourceIdentity resourceIdentity) {
    return maxCapacity.containsKey(resourceIdentity.getResourceType());
  }

  private ResourceIdentity getResourceIdentity(String resourceName, String resourceType) {
    return new ResourceIdentity(resourceName, resourceType);
  }

  private boolean isSuperUser(String user) {
    if (user == null) {
      return false;
    }

    String names[] = user.split("[/@]");
    LOG.info("Checking for superuser privileges for " + user + " or "  + names[0]);
    return superUsersList.contains(user) || superUsersList.contains(names[0]);
  }

  @Override
  public boolean acquireResource(String method, String user, String host)
      throws SQLException {

    if (isSuperUser(user)) {
      LOG.info("Applying superuser privileges, bypassing");
      return true;
    }

    acquireExecutionBasedResources(method);

    ResourceIdentity resourceIdentity = getResourceIdentity(user, method);
    if (!isResourceTypeControlled(resourceIdentity)) {
      LOG.info("Resource not throttled, continuing...");
      return true;
    }

    return acquireUserBasedResources(user, resourceIdentity);
  }

  @Override
  public void releaseResource(String method, String user, String host) {

    if (isSuperUser(user)) {
      return;
    }

    releaseExecutionBasedResources(method);

    ResourceIdentity resourceIdentity = getResourceIdentity(user, method);
    if (!isResourceTypeControlled(resourceIdentity)) {
      return;
    }

    releaseUserBasedResources(resourceIdentity);
  }

  private boolean acquireUserBasedResources(String user, ResourceIdentity resourceIdentity) throws HiveSQLException {
    Semaphore semaphore;
    if (!consumedResources.containsKey(resourceIdentity)) {
      semaphore = createResource(resourceIdentity);
    } else {
      semaphore = consumedResources.get(resourceIdentity);
    }


    if (semaphore.tryAcquire()) {
      LOG.info("Acquiring resource for resource: " + resourceIdentity +
          ", still available:" + semaphore.availablePermits());
      return true;
    } else {
      // Release global consumption resources
      if (consumedExecStatements != null) {
        consumedExecStatements.release();
      }
      throw new HiveSQLException("User " + user +
          " has reached maximum connections for this operation ", "42000");
    }
  }

  private void releaseUserBasedResources(ResourceIdentity resourceIdentity) {
    if (!consumedResources.containsKey(resourceIdentity)) {
      throw new RuntimeException("Trying to release resource that does not exist " + resourceIdentity);
    }

    Semaphore semaphore = consumedResources.get(resourceIdentity);
    semaphore.release();
    LOG.info("Released resources for " + resourceIdentity + " available: " + semaphore.availablePermits());
  }

  private void acquireExecutionBasedResources(String method) throws HiveSQLException {

    if (maxTotalExecStatements > 0) {
      LOG.info("Trying to obtain global exec for " + method + "resource list " + consumedExecStatements.availablePermits());
      if (method.equals(ThriftCLIService.ThriftCliFunctions.ExecuteStatement.name())) {
        if (consumedExecStatements.tryAcquire()) {
          LOG.info("Acquiring global resource for executions, remaining:" +
              consumedExecStatements.availablePermits());
        } else {
          throw new HiveSQLException("Server reached maximum number of global executions possible", "42000");
        }
      }
    }
  }

  private void releaseExecutionBasedResources(String method) {
    if (maxTotalExecStatements > 0) {
      if (method.equals(ThriftCLIService.ThriftCliFunctions.ExecuteStatement.name())) {
        consumedExecStatements.release();
      }
    }
  }

  @Override
  public TResourceStatusList getResourceConsumption() {
    TResourceStatusList resourceStatusList = new TResourceStatusList();

    TResourceStatus globalConnectionStatus = new TResourceStatus();
    globalConnectionStatus.setMaxCapacity(Integer.toString(maxConnections));
    globalConnectionStatus.setUsedCapacity(Integer.toString(noConnections.get()));
    globalConnectionStatus.setResourceName("ALL");
    globalConnectionStatus.setResourceType("Connections");
    resourceStatusList.addToResourcesConsumed(globalConnectionStatus);

    if (maxTotalExecStatements > 0) {
      TResourceStatus globalExecStatus = new TResourceStatus();
      globalExecStatus.setResourceName("ALL");
      globalExecStatus.setResourceType("ExecStmts");
      globalExecStatus.setMaxCapacity(Integer.toString(maxTotalExecStatements));
      globalExecStatus.setUsedCapacity(Integer.toString(maxTotalExecStatements - consumedExecStatements.availablePermits()));
      resourceStatusList.addToResourcesConsumed(globalExecStatus);
    }

    for (ResourceIdentity resourceIdentity : consumedResources.keySet()) {

      int maxCap = maxCapacity.get(resourceIdentity.getResourceType());
      int usedCap = maxCap - consumedResources.get(resourceIdentity).availablePermits();
      if (usedCap > 0) {
        TResourceStatus resourceStatus = new TResourceStatus();
        resourceStatus.setResourceName(resourceIdentity.getResourceName());
        resourceStatus.setResourceType(resourceIdentity.getResourceType());

        resourceStatus.setUsedCapacity(Integer.toString(usedCap));
        resourceStatus.setMaxCapacity(Integer.toString(maxCap));
        resourceStatusList.addToResourcesConsumed(resourceStatus);
      }
    }
    return resourceStatusList;
  }

  /*
   * This can be an inner class as we don't use this outside the Resource Management context.
   */
  private static final class ResourceIdentity {
    private static final String DELIMITER = ":";

    private String resourceName;
    private String resourceType;

    private ResourceIdentity(String resourceName, String resourceType) {
      this.resourceName = resourceName;
      this.resourceType = resourceType;
    }

    private String getResourceType() {
      return resourceType;
    }

    private String getResourceName() {
      return resourceName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ResourceIdentity that = (ResourceIdentity) o;

      if (resourceName != null ? !resourceName.equals(that.resourceName) : that.resourceName != null)
        return false;

      if (resourceType != null ? !resourceType.equals(that.resourceType) : that.resourceType != null)
        return false;
      if (resourceType != null ? !resourceType.equals(that.resourceType) : that.resourceType != null)
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = resourceName != null ? resourceName.hashCode() : 0;
      result = 31 * result + (resourceType != null ? resourceType.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return resourceName + DELIMITER + DELIMITER + resourceType;
    }
  }
}
