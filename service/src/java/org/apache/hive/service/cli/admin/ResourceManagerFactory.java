package org.apache.hive.service.cli.admin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.ReflectionUtils;

public class ResourceManagerFactory {

  public static final Log LOG = LogFactory.getLog(ResourceManagerFactory.class.getName());

  private static ResourceManager defaultResourceManager = null;

  public static ResourceManager getDefaultResourceManager() {

    synchronized (ResourceManagerFactory.class) {
      if (defaultResourceManager == null) {

        HiveConf hiveConf = new HiveConf();
        String rateLimitingImpl = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_RESOURCE_MANAGER);
        LOG.info("Initializing Resource Manager : " + rateLimitingImpl);
        try {
          Class<? extends ResourceManager> implementerClass =
              (Class<? extends ResourceManager>) Class.forName(rateLimitingImpl, true, JavaUtils.getClassLoader());
          defaultResourceManager = ReflectionUtils.newInstance(implementerClass, hiveConf);
          defaultResourceManager.init(hiveConf);
        } catch (ClassNotFoundException e) {
          LOG.error("Unable to load class " + rateLimitingImpl + " Exception ", e);
          throw new RuntimeException(e);
        }
      }
      return defaultResourceManager;
    }
  }
}
