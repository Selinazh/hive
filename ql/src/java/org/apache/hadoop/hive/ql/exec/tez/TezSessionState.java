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
package org.apache.hadoop.hive.ql.exec.tez;


import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.login.LoginException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.PreWarmVertex;
import org.apache.tez.dag.api.SessionNotRunning;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.mapreduce.hadoop.DeprecatedKeys;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;

/**
 * Holds session state related to Tez
 */
public class TezSessionState {

  private static final Log LOG = LogFactory.getLog(TezSessionState.class.getName());
  private static final String TEZ_DIR = "_tez_session_dir";

  private HiveConf conf;
  private Path tezScratchDir;
  private LocalResource appJarLr;
  private TezClient session;
  private String sessionId;
  private final DagUtils utils;
  private String queueName;
  private boolean defaultQueue = false;
  private String user;

  private final Set<String> additionalFilesNotFromConf = new HashSet<String>();
  private final Set<LocalResource> localizedResources = new HashSet<LocalResource>();
  private boolean doAsEnabled;

  /**
   * Constructor. We do not automatically connect, because we only want to
   * load tez classes when the user has tez installed.
   */
  public TezSessionState(DagUtils utils) {
    this.utils = utils;
  }

  /**
   * Constructor. We do not automatically connect, because we only want to
   * load tez classes when the user has tez installed.
   */
  public TezSessionState(String sessionId) {
    this(DagUtils.getInstance());
    this.sessionId = sessionId;
  }

  /**
   * Returns whether a session has been established
   */
  public boolean isOpen() {
    return session != null;
  }

  /**
   * Get all open sessions. Only used to clean up at shutdown.
   * @return List<TezSessionState>
   */
  public static String makeSessionId() {
    return UUID.randomUUID().toString();
  }

  public void open(HiveConf conf)
      throws IOException, LoginException, URISyntaxException, TezException {
    open(conf, null);
  }

  /**
   * Creates a tez session. A session is tied to either a cli/hs2 session. You can
   * submit multiple DAGs against a session (as long as they are executed serially).
   * @throws IOException
   * @throws URISyntaxException
   * @throws LoginException
   * @throws TezException
   */
  public void open(HiveConf conf, String[] additionalFiles)
    throws IOException, LoginException, IllegalArgumentException, URISyntaxException, TezException {
    this.conf = conf;
    this.queueName = conf.get("tez.queue.name");
    this.doAsEnabled = conf.getBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS);

    UserGroupInformation ugi = Utils.getUGI();
    user = ugi.getShortUserName();
    LOG.info("User of session id " + sessionId + " is " + user);

    // create the tez tmp dir
    tezScratchDir = createTezDir(sessionId);

    additionalFilesNotFromConf.clear();
    if (additionalFiles != null) {
      for (String originalFile : additionalFiles) {
        additionalFilesNotFromConf.add(originalFile);
      }
    }

    refreshLocalResourcesFromConf(conf);

    // unless already installed on all the cluster nodes, we'll have to
    // localize hive-exec.jar as well.
    appJarLr = createJarLocalResource(utils.getExecJarPathLocal());

    // configuration for the application master
    Map<String, LocalResource> commonLocalResources = new HashMap<String, LocalResource>();
    commonLocalResources.put(utils.getBaseName(appJarLr), appJarLr);
    for (LocalResource lr : localizedResources) {
      commonLocalResources.put(utils.getBaseName(lr), lr);
    }

    // Create environment for AM.
    Map<String, String> amEnv = new HashMap<String, String>();
    MRHelpers.updateEnvBasedOnMRAMEnv(conf, amEnv);

    // and finally we're ready to create and start the session
    // generate basic tez config
    TezConfiguration tezConfig = new TezConfiguration(conf);

    // Check for overrides in HiveConf.
    for (Map.Entry<String, String> value : conf) {
      String name = value.getKey();
      if (name.startsWith(TezConfiguration.TEZ_PREFIX)) {
        tezConfig.set(name, value.getValue());
      }
    }

    setupTezParamsBasedOnMR(tezConfig);

    tezConfig.set(TezConfiguration.TEZ_AM_STAGING_DIR, tezScratchDir.toUri().toString());
    conf.stripHiddenConfigurations(tezConfig);

    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_PREWARM_ENABLED)) {
      int n = HiveConf.getIntVar(conf, ConfVars.HIVE_PREWARM_NUM_CONTAINERS);
      n = Math.max(tezConfig.getInt(
          TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS,
          TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS_DEFAULT), n);
      tezConfig.setInt(TezConfiguration.TEZ_AM_SESSION_MIN_HELD_CONTAINERS, n);
    }

    // Override enabling timeline service per YHIVE-89.
    tezConfig.setBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
        HiveConf.getBoolVar(conf, ConfVars.HIVE_YARN_TIMELINE_SERVICE_ENABLED));

    tuneTezSessionSettings(tezConfig);

    session = TezClient.create("HIVE-" + sessionId, tezConfig, true,
        commonLocalResources, null);

    LOG.info("Opening new Tez Session (id: " + sessionId
        + ", scratch dir: " + tezScratchDir + ")");

    TezJobMonitor.initShutdownHook();
    session.start();

    LOG.info("Starting Tez AM: applicationId=" + session.getAppMasterApplicationId()
        + " status=" + session.getAppMasterStatus());

    if (HiveConf.getBoolVar(conf, ConfVars.HIVE_PREWARM_ENABLED)) {
      int n = HiveConf.getIntVar(conf, ConfVars.HIVE_PREWARM_NUM_CONTAINERS);
      LOG.info("Prewarming " + n + " containers  (id: " + sessionId
          + ", scratch dir: " + tezScratchDir + ")");
      PreWarmVertex prewarmVertex = utils.createPreWarmVertex(tezConfig, n,
          commonLocalResources);
      try {
        session.preWarm(prewarmVertex);
      } catch (IOException ie) {
        if (ie.getMessage().contains("Interrupted while waiting")) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Hive Prewarm threw an exception ", ie);
          }
        } else {
          throw ie;
        }
      }
    }
    try {
      session.waitTillReady();
    } catch(InterruptedException ie) {
      //ignore
    }
  }

  private static TezConfiguration tuneTezSessionSettings(TezConfiguration conf) {

    if (conf.getBoolean(HiveConf.ConfVars.HIVE_AM_AUTO_TUNE_ENABLED.varname,
                                 ConfVars.HIVE_AM_AUTO_TUNE_ENABLED.defaultBoolVal)) {

      LOG.info("Attempting to auto-tune AM settings.");

      int amContainerSizeMB = conf.getInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 1536);

      if (amContainerSizeMB <= 1536) {
        LOG.warn(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB + "(" + amContainerSizeMB + ") " +
            "must exceed 1535 for auto-tuning. Disabling auto-tuning.");
        return conf;
      }

      // Set AM heap-size to `max(512, 0.2*amContainerSizeMB)` less than the chosen AM container size.
      int amMaxHeapSizeMB = amContainerSizeMB - Math.max(512, Math.round(0.2f*amContainerSizeMB));
      String amCmdOpts = conf.get(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS,
          TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS_DEFAULT);
      amCmdOpts += " -Xmx" + amMaxHeapSizeMB + "m";
      LOG.info("Setting " + TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS + " to " + amCmdOpts + ".");
      conf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, amCmdOpts);

      // Set concurrency based on available memory.
      // If N = nConcurrentTasksPerGB,
      //  1. Assume N/2 parallel tasks for the first 1 GB heap,
      //  2. N more parallel tasks for every additional 1 GB.
      // i.e.
      //  totalConcurrency = N * ((amMaxHeapSizeMB/1024) - 1/2)

      int nConcurrentTasksPerGB = conf.getInt(ConfVars.HIVE_AM_AUTO_TUNE_NUM_CONCURRENT_TASKS_PER_GB.varname,
                                              ConfVars.HIVE_AM_AUTO_TUNE_NUM_CONCURRENT_TASKS_PER_GB.defaultIntVal);
      int totalConcurrency = Math.round( nConcurrentTasksPerGB * (amMaxHeapSizeMB/1024f - 0.5f) );

      LOG.info("Setting " + TezConfiguration.TEZ_AM_VERTEX_MAX_TASK_CONCURRENCY
          + " to " + totalConcurrency + ".");
      conf.setInt(TezConfiguration.TEZ_AM_VERTEX_MAX_TASK_CONCURRENCY, totalConcurrency);
    }
    else {
      LOG.info("Auto-tuning disabled for AM settings.");
    }

    return conf;
  }

  /**
   * This takes settings from MR and applies them to the appropriate Tez configuration. This is
   * similar to what Pig on Tez does (refer MRToTezHelper.java).
   *
   * @param conf configuration with MR settings
   */
  private void setupTezParamsBasedOnMR(TezConfiguration conf) {

    String env = conf.get(MRJobConfig.MR_AM_ADMIN_USER_ENV);
    if (conf.get(MRJobConfig.MR_AM_ENV) != null) {
      env = (env == null) ? conf.get(MRJobConfig.MR_AM_ENV) : env + "," + conf.get(MRJobConfig.MR_AM_ENV);
    }
    if (env != null) {
      conf.setIfUnset(TezConfiguration.TEZ_AM_LAUNCH_ENV, env);
    }

    conf.setIfUnset(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS,
        org.apache.tez.mapreduce.hadoop.MRHelpers.getJavaOptsForMRAM(conf));

    String queueName = conf.get(JobContext.QUEUE_NAME, YarnConfiguration.DEFAULT_QUEUE_NAME);
    conf.setIfUnset(TezConfiguration.TEZ_QUEUE_NAME, queueName);

    int amMemMB = conf.getInt(MRJobConfig.MR_AM_VMEM_MB, MRJobConfig.DEFAULT_MR_AM_VMEM_MB);
    conf.setIfUnset(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, "" + amMemMB);

    int amCores = conf.getInt(MRJobConfig.MR_AM_CPU_VCORES, MRJobConfig.DEFAULT_MR_AM_CPU_VCORES);
    conf.setIfUnset(TezConfiguration.TEZ_AM_RESOURCE_CPU_VCORES, "" + amCores);

    conf.setIfUnset(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, ""
        + conf.getInt(MRJobConfig.MR_AM_MAX_ATTEMPTS, MRJobConfig.DEFAULT_MR_AM_MAX_ATTEMPTS));

    conf.setIfUnset(TezConfiguration.TEZ_AM_VIEW_ACLS,
        conf.get(MRJobConfig.JOB_ACL_VIEW_JOB, MRJobConfig.DEFAULT_JOB_ACL_VIEW_JOB));

    conf.setIfUnset(TezConfiguration.TEZ_AM_MODIFY_ACLS,
        conf.get(MRJobConfig.JOB_ACL_MODIFY_JOB, MRJobConfig.DEFAULT_JOB_ACL_MODIFY_JOB));

    // Refer to org.apache.tez.mapreduce.hadoop.MRHelpers.processDirectConversion.
    ArrayList<Map<String, String>> maps = new ArrayList<Map<String, String>>(2);
    maps.add(DeprecatedKeys.getMRToTezRuntimeParamMap());
    maps.add(DeprecatedKeys.getMRToDAGParamMap());

    boolean preferTez = true;

    for (Map<String, String> map : maps) {
      for (Map.Entry<String, String> dep : map.entrySet()) {
        if (conf.get(dep.getKey()) != null) {
          // TODO Deprecation reason does not seem to reflect in the config ?
          // The ordering is important in case of keys which are also deprecated.
          // Unset will unset the deprecated keys and all it's variants.
          final String mrValue = conf.get(dep.getKey());
          final String tezValue = conf.get(dep.getValue());
          conf.unset(dep.getKey());
          if (tezValue == null) {
            conf.set(dep.getValue(), mrValue, "TRANSLATED_TO_TEZ");
          } else if (!preferTez) {
            conf.set(dep.getValue(), mrValue, "TRANSLATED_TO_TEZ_AND_MR_OVERRIDE");
          }
          LOG.info("Config: mr(unset):" + dep.getKey() + ", mr initial value="
              + mrValue
              + ", tez(original):" + dep.getValue() + "=" + tezValue
              + ", tez(final):" + dep.getValue() + "=" + conf.get(dep.getValue()));
        }
      }
    }
  }

  public void refreshLocalResourcesFromConf(HiveConf conf)
    throws IOException, LoginException, IllegalArgumentException, URISyntaxException, TezException {

    String dir = tezScratchDir.toString();

    localizedResources.clear();

    // these are local resources set through add file, jar, etc
    List<LocalResource> lrs = utils.localizeTempFilesFromConf(dir, conf);
    if (lrs != null) {
      localizedResources.addAll(lrs);
    }

    // these are local resources that are set through the mr "tmpjars" property
    List<LocalResource> handlerLr = utils.localizeTempFiles(dir, conf,
      additionalFilesNotFromConf.toArray(new String[additionalFilesNotFromConf.size()]));

    if (handlerLr != null) {
      localizedResources.addAll(handlerLr);
    }
  }

  public boolean hasResources(String[] localAmResources) {
    if (localAmResources == null || localAmResources.length == 0) return true;
    if (additionalFilesNotFromConf.isEmpty()) return false;
    for (String s : localAmResources) {
      if (!additionalFilesNotFromConf.contains(s)) return false;
    }
    return true;
  }

  /**
   * Close a tez session. Will cleanup any tez/am related resources. After closing a session
   * no further DAGs can be executed against it.
   * @param keepTmpDir whether or not to remove the scratch dir at the same time.
   * @throws IOException
   * @throws TezException
   */
  public void close(boolean keepTmpDir) throws TezException, IOException {
    if (!isOpen()) {
      return;
    }

    LOG.info("Closing Tez Session");
    try {
      session.stop();
    } catch (SessionNotRunning nr) {
      // ignore
    }

    if (!keepTmpDir) {
      cleanupScratchDir();
    }
    session = null;
    tezScratchDir = null;
    conf = null;
    appJarLr = null;
    additionalFilesNotFromConf.clear();
    localizedResources.clear();
  }

  public void cleanupScratchDir () throws IOException {
    FileSystem fs = tezScratchDir.getFileSystem(conf);
    fs.delete(tezScratchDir, true);
    tezScratchDir = null;
  }

  public String getSessionId() {
    return sessionId;
  }

  public TezClient getSession() {
    return session;
  }

  public String getAppURL() {
    if (getSession() != null && getSession().getAppMasterApplicationId() != null) {
      return getSession().getAppMasterApplicationId().toString();
    }
    return null;
  }

  public Path getTezScratchDir() {
    return tezScratchDir;
  }

  public LocalResource getAppJarLr() {
    return appJarLr;
  }

  /**
   * createTezDir creates a temporary directory in the scratchDir folder to
   * be used with Tez. Assumes scratchDir exists.
   */
  private Path createTezDir(String sessionId)
    throws IOException {

    // tez needs its own scratch dir (per session)
    Path tezDir = new Path(SessionState.get().getHdfsScratchDirURIString(), TEZ_DIR);
    tezDir = new Path(tezDir, sessionId);
    FileSystem fs = tezDir.getFileSystem(conf);
    FsPermission fsPermission = new FsPermission(HiveConf.getVar(conf, HiveConf.ConfVars.SCRATCHDIRPERMISSION));
    fs.mkdirs(tezDir, fsPermission);
    // Make sure the path is normalized (we expect validation to pass since we just created it).
    tezDir = DagUtils.validateTargetDir(tezDir, conf).getPath();
    // don't keep the directory around on non-clean exit
    fs.deleteOnExit(tezDir);

    return tezDir;
  }

  /**
   * Returns a local resource representing a jar.
   * This resource will be used to execute the plan on the cluster.
   * @param localJarPath Local path to the jar to be localized.
   * @return LocalResource corresponding to the localized hive exec resource.
   * @throws IOException when any file system related call fails.
   * @throws LoginException when we are unable to determine the user.
   * @throws URISyntaxException when current jar location cannot be determined.
   */
  private LocalResource createJarLocalResource(String localJarPath)
      throws IOException, LoginException, IllegalArgumentException,
      FileNotFoundException {
    FileStatus destDirStatus = utils.getHiveJarDirectory(conf);
    assert destDirStatus != null;
    Path destDirPath = destDirStatus.getPath();

    Path localFile = new Path(localJarPath);
    String sha = getSha(localFile);

    String destFileName = localFile.getName();

    // Now, try to find the file based on SHA and name. Currently we require exact name match.
    // We could also allow cutting off versions and other stuff provided that SHA matches...
    destFileName = FilenameUtils.removeExtension(destFileName) + "-" + sha
        + FilenameUtils.EXTENSION_SEPARATOR + FilenameUtils.getExtension(destFileName);

    if (LOG.isDebugEnabled()) {
      LOG.debug("The destination file name for [" + localJarPath + "] is " + destFileName);
    }

    // TODO: if this method is ever called on more than one jar, getting the dir and the
    //       list need to be refactored out to be done only once.
    Path destFile = new Path(destDirPath.toString() + "/" + destFileName);
    return utils.localizeResource(localFile, destFile, LocalResourceType.FILE, conf);
  }


  private String getSha(Path localFile) throws IOException, IllegalArgumentException {
    InputStream is = null;
    try {
      FileSystem localFs = FileSystem.getLocal(conf);
      is = localFs.open(localFile);
      return DigestUtils.sha256Hex(is);
    } finally {
      if (is != null) {
        is.close();
      }
    }
  }
  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public String getQueueName() {
    return queueName;
  }

  public void setDefault() {
    defaultQueue  = true;
  }

  public boolean isDefault() {
    return defaultQueue;
  }

  public HiveConf getConf() {
    return conf;
  }

  public List<LocalResource> getLocalizedResources() {
    return new ArrayList<LocalResource>(localizedResources);
  }

  public String getUser() {
    return user;
  }

  public boolean getDoAsEnabled() {
    return doAsEnabled;
  }
}
