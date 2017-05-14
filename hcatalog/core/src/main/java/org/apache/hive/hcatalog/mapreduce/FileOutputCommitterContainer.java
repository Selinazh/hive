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

package org.apache.hive.hcatalog.mapreduce;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hive.hcatalog.common.ErrorType;
import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchemaUtils;
import org.apache.hive.hcatalog.har.HarOutputCommitterPostProcessor;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Part of the FileOutput*Container classes
 * See {@link FileOutputFormatContainer} for more information
 */
class FileOutputCommitterContainer extends OutputCommitterContainer {

  private static final String TEMP_DIR_NAME = "_temporary";
  private static final String LOGS_DIR_NAME = "_logs";

  static final String DYNTEMP_DIR_NAME = "_DYN";
  static final String SCRATCH_DIR_NAME = "_SCRATCH";
  private static final String APPEND_SUFFIX = "_a_";
  private static final int APPEND_COUNTER_WARN_THRESHOLD = 1000;
  private final int maxAppendAttempts;

  private static final Logger LOG = LoggerFactory.getLogger(FileOutputCommitterContainer.class);
  private final boolean dynamicPartitioningUsed;
  private boolean partitionsDiscovered;
  private boolean skipRegisterPartitions;
  private final boolean customDynamicLocationUsed;

  private Map<String, Map<String, String>> partitionsDiscoveredByPath;
  private Map<String, JobContext> contextDiscoveredByPath;
  private final HiveStorageHandler cachedStorageHandler;

  HarOutputCommitterPostProcessor harProcessor = new HarOutputCommitterPostProcessor();

  private String ptnRootLocation = null;

  private OutputJobInfo jobInfo = null;

  /**
   * @param context current JobContext
   * @param baseCommitter OutputCommitter to contain
   * @throws IOException
   */
  public FileOutputCommitterContainer(JobContext context,
                    org.apache.hadoop.mapred.OutputCommitter baseCommitter) throws IOException {
    super(context, baseCommitter);
    jobInfo = HCatOutputFormat.getJobInfo(context.getConfiguration());
    dynamicPartitioningUsed = jobInfo.isDynamicPartitioningUsed();

    this.partitionsDiscovered = !dynamicPartitioningUsed;
    cachedStorageHandler = HCatUtil.getStorageHandler(context.getConfiguration(), jobInfo.getTableInfo().getStorerInfo());
    Table table = new Table(jobInfo.getTableInfo().getTable());
    if (dynamicPartitioningUsed && Boolean.valueOf((String)table.getProperty("EXTERNAL"))
        && jobInfo.getCustomDynamicPath() != null
        && jobInfo.getCustomDynamicPath().length() > 0) {
      customDynamicLocationUsed = true;
    } else {
      customDynamicLocationUsed = false;
    }

    this.maxAppendAttempts = context.getConfiguration().getInt(HCatConstants.HCAT_APPEND_LIMIT, APPEND_COUNTER_WARN_THRESHOLD);

    skipRegisterPartitions = HCatUtil.getHiveConf(context.getConfiguration())
                                                 .getBoolean("hive.hcat.mapreduce.skip.partition.registration", false);

    if (skipRegisterPartitions) {
      LOG.warn("HCatalog partition-registration will be skipped!");
    }
    else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Partitions will be registered in HCatalog. Table-schema will be reconciled, if necessary.");
      }
    }
  }

  @Override
  public void abortTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      FileOutputFormatContainer.setWorkOutputPath(context);
      getBaseOutputCommitter().abortTask(HCatMapRedUtil.createTaskAttemptContext(context));
    } else {
      try {
        TaskCommitContextRegistry.getInstance().abortTask(context);
      }
      finally {
        TaskCommitContextRegistry.getInstance().discardCleanupFor(context);
      }
    }
  }

  @Override
  public void commitTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
         //See HCATALOG-499
      FileOutputFormatContainer.setWorkOutputPath(context);
      getBaseOutputCommitter().commitTask(HCatMapRedUtil.createTaskAttemptContext(context));
    } else {
      try {
        TaskCommitContextRegistry.getInstance().commitTask(context);
      }
      finally {
        TaskCommitContextRegistry.getInstance().discardCleanupFor(context);
      }
    }
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      FileOutputFormatContainer.setWorkOutputPath(context);
      return getBaseOutputCommitter().needsTaskCommit(HCatMapRedUtil.createTaskAttemptContext(context));
    } else {
      // called explicitly through FileRecordWriterContainer.close() if dynamic - return false by default
      return true;
    }
  }

  @Override
  public void setupJob(JobContext context) throws IOException {
    if (getBaseOutputCommitter() != null && !dynamicPartitioningUsed) {
      getBaseOutputCommitter().setupJob(HCatMapRedUtil.createJobContext(context));
    }
    // in dynamic usecase, called through FileRecordWriterContainer
  }

  @Override
  public void setupTask(TaskAttemptContext context) throws IOException {
    if (!dynamicPartitioningUsed) {
      getBaseOutputCommitter().setupTask(HCatMapRedUtil.createTaskAttemptContext(context));
    }
  }

  @Override
  public void abortJob(JobContext jobContext, State state) throws IOException {
    try {
      if (dynamicPartitioningUsed) {
        discoverPartitions(jobContext);
      }
      org.apache.hadoop.mapred.JobContext mapRedJobContext = HCatMapRedUtil
          .createJobContext(jobContext);
      if (getBaseOutputCommitter() != null && !dynamicPartitioningUsed) {
        getBaseOutputCommitter().abortJob(mapRedJobContext, state);
      } else if (dynamicPartitioningUsed) {
        for (JobContext currContext : contextDiscoveredByPath.values()) {
          try {
            new JobConf(currContext.getConfiguration())
                .getOutputCommitter().abortJob(currContext,
                    state);
          } catch (Exception e) {
            throw new IOException(e);
          }
        }
      }
      Path src;
      OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(jobContext.getConfiguration());
      Path tblPath = new Path(jobInfo.getTableInfo().getTableLocation());
      if (dynamicPartitioningUsed) {
        if (!customDynamicLocationUsed) {
          src = new Path(getPartitionRootLocation(jobInfo.getLocation(), jobInfo.getTableInfo().getTable()
              .getPartitionKeysSize()));
        } else {
          src = new Path(getCustomPartitionRootLocation(jobInfo, jobContext.getConfiguration()));
        }
      } else {
        src = new Path(jobInfo.getLocation());
      }
      FileSystem fs = src.getFileSystem(jobContext.getConfiguration());
      // Note fs.delete will fail on Windows. The reason is in OutputCommitter,
      // Hadoop is still writing to _logs/history. On Linux, OS don't care file is still
      // open and remove the directory anyway, but on Windows, OS refuse to remove a
      // directory containing open files. So on Windows, we will leave output directory
      // behind when job fail. User needs to remove the output directory manually
      LOG.info("Job failed. Try cleaning up temporary directory [{}].", src);
      if (!src.equals(tblPath)){
        fs.delete(src, true);
      }
    } finally {
      cancelDelegationTokens(jobContext);
    }
  }

  public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
  static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
    "mapreduce.fileoutputcommitter.marksuccessfuljobs";

  private static boolean getOutputDirMarking(Configuration conf) {
    return conf.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
      false);
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    if (dynamicPartitioningUsed) {
      discoverPartitions(jobContext);
      // Commit each partition so it gets moved out of the job work
      // dir
      for (JobContext context : contextDiscoveredByPath.values()) {
        new JobConf(context.getConfiguration())
            .getOutputCommitter().commitJob(context);
      }
    }
    if (getBaseOutputCommitter() != null && !dynamicPartitioningUsed) {
      getBaseOutputCommitter().commitJob(
          HCatMapRedUtil.createJobContext(jobContext));
    }
    registerPartitions(jobContext);
    // create _SUCCESS FILE if so requested.
    OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(jobContext.getConfiguration());
    if (getOutputDirMarking(jobContext.getConfiguration())) {
      Path outputPath = new Path(jobInfo.getLocation());
      FileSystem fileSys = outputPath.getFileSystem(jobContext
          .getConfiguration());
      // create a file in the folder to mark it
      if (fileSys.exists(outputPath)) {
        Path filePath = new Path(outputPath,
            SUCCEEDED_FILE_NAME);
        if (!fileSys.exists(filePath)) { // may have been
                         // created by
                         // baseCommitter.commitJob()
          fileSys.create(filePath).close();
        }
      }
    }

    // Commit has succeeded (since no exceptions have been thrown.)
    // Safe to cancel delegation tokens now.
    cancelDelegationTokens(jobContext);
  }

  @Override
  public void cleanupJob(JobContext context) throws IOException {
    throw new IOException("The method cleanupJob is deprecated and should not be called.");
  }

  private String getCustomPartitionRootLocation(OutputJobInfo jobInfo, Configuration conf) {
    if (ptnRootLocation == null) {
      // we only need to calculate it once, it'll be the same for other partitions in this job.
      String parentPath = jobInfo.getTableInfo().getTableLocation();
      if (jobInfo.getCustomDynamicRoot() != null
          && jobInfo.getCustomDynamicRoot().length() > 0) {
        parentPath = new Path(parentPath, jobInfo.getCustomDynamicRoot()).toString();
      }
      Path ptnRoot = new Path(parentPath, DYNTEMP_DIR_NAME +
          conf.get(HCatConstants.HCAT_DYNAMIC_PTN_JOBID));
      ptnRootLocation = ptnRoot.toString();
    }
    return ptnRootLocation;
  }

  private String getPartitionRootLocation(String ptnLocn, int numPtnKeys) {
    if (customDynamicLocationUsed) {
      return null;
    }

    if (ptnRootLocation == null) {
      // we only need to calculate it once, it'll be the same for other partitions in this job.
      Path ptnRoot = new Path(ptnLocn);
      for (int i = 0; i < numPtnKeys; i++) {
        ptnRoot = ptnRoot.getParent();
      }
      ptnRootLocation = ptnRoot.toString();
    }

    return ptnRootLocation;
  }

  /**
   * Generate partition metadata object to be used to add to metadata.
   * This method is called for static partitions only.
   * @param context The job context.
   * @param jobInfo The OutputJobInfo.
   * @param partLocnRoot The table-equivalent location root of the partition
   *                       (temporary dir if dynamic partition, table dir if static)
   * @param partKVs The keyvalue pairs that form the partition
   * @param outputSchema The output schema for the partition
   * @param params The parameters to store inside the partition
   * @param table The Table metadata object under which this Partition will reside
   * @param fs FileSystem object to operate on the underlying filesystem
   * @param grpName Group name that owns the table dir
   * @param perms FsPermission that's the default permission of the table dir.
   * @param acls a list of extended ACLs associated with the table.
   * @return Constructed Partition metadata object
   * @throws java.io.IOException
   */
  private Partition constructPartition(
    OutputJobInfo jobInfo,
    String partLocnRoot, Map<String, String> partKVs,
    HCatSchema outputSchema, Map<String, String> params,
    Table table, FileSystem fs,
    String grpName, FsPermission perms, List<AclEntry> acls) throws IOException {

    Partition partition = new Partition();
    partition.setDbName(table.getDbName());
    partition.setTableName(table.getTableName());
    partition.setSd(new StorageDescriptor(table.getTTable().getSd()));

    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    for (HCatFieldSchema fieldSchema : outputSchema.getFields()) {
      fields.add(HCatSchemaUtils.getFieldSchema(fieldSchema));
    }

    partition.getSd().setCols(fields);

    partition.setValues(FileOutputFormatContainer.getPartitionValueList(table, partKVs));

    partition.setParameters(params);

    // Sets permissions and group name on partition dirs and files.

    Path partPath = null;

    if (Boolean.valueOf((String)table.getProperty("EXTERNAL"))
        && jobInfo.getLocation() != null && jobInfo.getLocation().length() > 0) {
      // NOTE: This condition is true when initializing HCatStorer with two parameters:
      // HCatStorer(<partition-spec>, <location>).
      // see TestHCatStorerWrapper.testStoreExternalTableWithExternalDir.
      //
      // Now, we need to de-scratchify this location - i.e., get rid of any
      // _SCRATCH[\d].?[\d]+ from the location.
      String jobLocation = jobInfo.getLocation();
      String finalLocn = jobLocation.replaceAll(Path.SEPARATOR + SCRATCH_DIR_NAME + "\\d\\.?\\d+(E-\\d+)?", "");
      partPath = new Path(finalLocn);
    } else {
      partPath = new Path(partLocnRoot);

      boolean checkGroupPerms = true;
      String newGroup = null;
      FsPermission newPerms = null;

      int i = 0;
      for (FieldSchema partKey : table.getPartitionKeys()) {
        if (i++ != 0) {
          if (! fs.exists(partPath)) {
            fs.mkdirs(partPath);

            if (checkGroupPerms) {
              FileStatus fstat = fs.getFileStatus(partPath);
              newGroup = compareGroups(fs, fstat, grpName);
              newPerms = compareDirPermissions(fstat, perms);
              checkGroupPerms = false;
            }

            if (newGroup != null || newPerms != null || acls != null) {
              applyGroupAndPerms(fs, partPath, newGroup, newPerms, acls, false);
              newGroup = null;
            }
          }
        }

        partPath = constructPartialPartPath(partPath, partKey.getName().toLowerCase(), partKVs);
      }
    }

    // Apply the group and permissions to the leaf partition and files.
    if (! fs.exists(partPath)) {
      fs.mkdirs(partPath);
      // NOTE: applyGroupAndPerms is called by registerPartitions on the leaf node.
    }

    partition.getSd().setLocation(partPath.toString());

    return partition;
  }

  /**
   * Generate a list of partition metadata objects to be used to add to metadata.
   * This method is called only for dynamic partitions, but accounts for default
   * partition locations and custom partition locations (patterns).
   * @param context The job context.
   * @param jobInfo The OutputJobInfo.
   * @param destRoot The table-equivalent location root of the partition
   * @param discoveredPartitions a map of the discovered partitions
   * @param outputSchema The output schema for the partition
   * @param params The parameters to store inside the partition
   * @param table The Table metadata object under which this Partition will reside
   * @param fs FileSystem object to operate on the underlying filesystem
   * @param grpName Group name that owns the table dir
   * @param perms FsPermission that's the default permission of the table dir.
   * @param acls a list of extended ACLs associated with the table.
   * @return a list of Partition objects associated with the discovered partitions.
   * @throws java.io.IOException
   */
  private List<Partition> constructPartitions(JobContext context, OutputJobInfo jobInfo, String destRoot,
      Map<String, Map<String, String>> discoveredPartitions, HCatSchema outputSchema, Map<String, String> params,
      Table table, FileSystem fs, String grpName, FsPermission perms, List<AclEntry> acls) throws IOException {

    ArrayList<Partition> partitions = new ArrayList<Partition>();
    LinkedHashMap<Path, Boolean> paths = new LinkedHashMap<Path, Boolean>();

    List<FieldSchema> fields = new ArrayList<FieldSchema>();
    for (HCatFieldSchema field : outputSchema.getFields()) {
      fields.add(HCatSchemaUtils.getFieldSchema(field));
    }

    Path tableRoot = new Path(table.getTTable().getSd().getLocation());

    if (customDynamicLocationUsed) {
      // NOTE: There is no guarantee that the group/perms will be equivalent to that of the
      // table in the case of custom partition locations. Hence, we will just add the leaf
      // directories to paths.
      for (Entry<String,Map<String,String>> entry : discoveredPartitions.entrySet()) {
        String dest = getFinalDynamicPartitionDestination(table, entry.getValue(), jobInfo);
        Path path = new Path(dest);
        Path harPath = new Path(entry.getKey());

        paths.put(path.getParent(), Boolean.TRUE);

        Partition partition = new Partition();
        partition.setDbName(table.getDbName());
        partition.setTableName(table.getTableName());
        partition.setSd(new StorageDescriptor(table.getTTable().getSd()));

        partition.getSd().setCols(fields);
        partition.setValues(FileOutputFormatContainer.getPartitionValueList(table, entry.getValue()));
        partition.setParameters(params);

        if (harProcessor.isEnabled()) {
          harProcessor.exec(context, partition, harPath);
          partition.getSd().setLocation(harProcessor.getProcessedLocation(new Path(dest)));
        } else {
          partition.getSd().setLocation(dest);
        }

        partitions.add(partition);
      }
    } else {
      // Generate managed partition locations.
      // NOTE: We do a best-effort attempt to ensure that non-custom dynamic partitions
      // have group/perms that are equivalent to the table. Hence, we will add all of the
      // parent directories of the leaf directories to paths.
      int leaf = table.getPartitionKeys().size() - 1;

      for (Entry<String,Map<String,String>> entry : discoveredPartitions.entrySet()) {
        Path path = new Path(destRoot);
        Path harPath = new Path(ptnRootLocation);

        // Add all partition directories except for the leaf directories.
        // Leaf directories will be renamed from _DYN to their destination.
        int index = 0;
        for (FieldSchema part : table.getPartitionKeys()) {
          path = constructPartialPartPath(path, part.getName().toLowerCase(), entry.getValue());
          harPath = constructPartialPartPath(harPath, part.getName().toLowerCase(), entry.getValue());
          if (index++ < leaf) {
            paths.put(path, Boolean.FALSE);
          }
        }

        Partition partition = new Partition();
        partition.setDbName(table.getDbName());
        partition.setTableName(table.getTableName());
        partition.setSd(new StorageDescriptor(table.getTTable().getSd()));

        partition.getSd().setCols(fields);
        partition.setValues(FileOutputFormatContainer.getPartitionValueList(table, entry.getValue()));
        partition.setParameters(params);

        String dest = getFinalDynamicPartitionDestination(table, entry.getValue(), jobInfo);

        if (harProcessor.isEnabled()) {
          harProcessor.exec(context, partition, harPath);
          partition.getSd().setLocation(harProcessor.getProcessedLocation(new Path(dest)));
        } else {
          partition.getSd().setLocation(dest);
        }

        partitions.add(partition);
      }
    }

    // Set up the parent directories of all leaf directories.
    String newGroup = null;
    FsPermission newPerms = null;

    for (Map.Entry<Path, Boolean> entry : paths.entrySet()) {
      Path path = entry.getKey();

      if (! fs.exists(path)) {
        fs.mkdirs(path);

        // NOTE: applyGroupAndPerms is called on all directories regardless of whether
        // the table is MANAGED or EXTERNAL or if customDynamicLocationUsed is true, which
        // could mean the data is stored outside of the table directory.
        Path parent = path.getParent();
        boolean isTopDirectory = tableRoot.equals(parent);

        // If the path is directly under the table root or a previously created directory
        // the group will be correct, so just verify the perms.
        if (isTopDirectory || (paths.containsKey(parent) && paths.get(parent))) {
          FileStatus fstat = fs.getFileStatus(path);
          newGroup = null;
          newPerms = compareDirPermissions(fstat, perms);
        } else {
          FileStatus fstat = fs.getFileStatus(path);
          newGroup = compareGroups(fs, fstat, grpName);
          newPerms = compareDirPermissions(fstat, perms);
        }

        if (newGroup != null || newPerms != null || acls != null) {
          applyGroupAndPerms(fs, path, newGroup, newPerms, acls, false);
        }

        entry.setValue(Boolean.TRUE);
      }
    }

    return partitions;
  }

  /**
   * Attempt to set the group, permissions, and ACLs of the directory to the specified values.
   * It is possible that the directory cannot be changed to the specified group if the user is
   * not a member of that group. Any of group, permission, or acls that is set to <code>null</code>
   * will be ignored.
   * @param fs the FileSystem object associated with the directory.
   * @param dir the directory to operate on.
   * @param group the group to change to.
   * @param permission the permissions to change to.
   * @param acls the extended ACLs to apply.
   * @param recursive indicate whether to call the method on the contents of the directory.
   */
  private void applyGroupAndPerms(FileSystem fs, Path dir, String group,
      FsPermission permission, List<AclEntry> acls, boolean recursive) throws IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("applyGroupAndPerms: " + dir +
          " group: " + group +
          " perms: " + permission +
          " acls: " + acls +
          " recursive: " + recursive);
    }

    // Apply group.
    if (group != null) {
      try {
        fs.setOwner(dir, null, group);
      } catch (AccessControlException e) {
        LOG.warn("Unable to change group: " + dir, e);
      }
    }

    // Apply permissions.
    if (permission != null) {
      fs.setPermission(dir, permission);
    }

    // Apply ACLs.
    if ((acls != null) && (acls.size() > 0)) {
      fs.setAcl(dir, acls);
    }

    if (recursive) {
      List<AclEntry> fileAcls = getFileAclEntries(acls);

      for (FileStatus fileStatus : fs.listStatus(dir)) {
        if (fileStatus.isDir()) {
          applyGroupAndPerms(fs, fileStatus.getPath(), group, permission, acls, true);
        } else {
          applyGroupAndPerms(fs, fileStatus.getPath(), group, permission, fileAcls, false);
        }
      }
    }
  }

  private String getFinalDynamicPartitionDestination(Table table, Map<String, String> partKVs,
      OutputJobInfo jobInfo) {
    Path partPath = new Path(table.getTTable().getSd().getLocation());
    if (!customDynamicLocationUsed) {
      // file:///tmp/hcat_junit_warehouse/employee/_DYN0.7770480401313761/emp_country=IN/emp_state=KA  ->
      // file:///tmp/hcat_junit_warehouse/employee/emp_country=IN/emp_state=KA
      for (FieldSchema partKey : table.getPartitionKeys()) {
        partPath = constructPartialPartPath(partPath, partKey.getName().toLowerCase(), partKVs);
      }

      return partPath.toString();
    } else {
      // if custom root specified, update the parent path
      if (jobInfo.getCustomDynamicRoot() != null
          && jobInfo.getCustomDynamicRoot().length() > 0) {
        partPath = new Path(partPath, jobInfo.getCustomDynamicRoot());
      }
      return new Path(partPath, HCatFileUtil.resolveCustomPath(jobInfo, partKVs, false)).toString();
    }
  }

  private Map<String, String> getStorerParameterMap(StorerInfo storer) {
    Map<String, String> params = new HashMap<String, String>();

    //Copy table level hcat.* keys to the partition
    for (Entry<Object, Object> entry : storer.getProperties().entrySet()) {
      params.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return params;
  }

  private Path constructPartialPartPath(Path partialPath, String partKey, Map<String, String> partKVs) {

    StringBuilder sb = new StringBuilder(FileUtils.escapePathName(partKey));
    sb.append("=");
    sb.append(FileUtils.escapePathName(partKVs.get(partKey)));
    return new Path(partialPath, sb.toString());
  }

  /**
   * Update table schema, adding new columns as added for the partition.
   * @param client the client
   * @param table the table
   * @param partitionSchema the schema of the partition
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   * @throws org.apache.hadoop.hive.metastore.api.InvalidOperationException the invalid operation exception
   * @throws org.apache.hadoop.hive.metastore.api.MetaException the meta exception
   * @throws org.apache.thrift.TException the t exception
   */
  private void updateTableSchema(IMetaStoreClient client, Table table,
                   HCatSchema partitionSchema) throws IOException, InvalidOperationException, MetaException, TException {


    List<FieldSchema> newColumns = HCatUtil.validatePartitionSchema(table, partitionSchema);

    if (newColumns.size() != 0) {
      if (skipRegisterPartitions) {
        LOG.warn("Skipping partition-registration. " +
                 "Partitions detected with the following new columns: ( " + newColumns + " ). " +
                 "Please update the table-schema before these partitions are registered.");
      }
      else {
        LOG.info("Updating table schema with new columns: " + newColumns);
        List<FieldSchema> tableColumns = new ArrayList<FieldSchema>(table.getTTable().getSd().getCols());
        tableColumns.addAll(newColumns);

        //Update table schema to add the newly added columns
        table.getTTable().getSd().setCols(tableColumns);
        client.alter_table(table.getDbName(), table.getTableName(), table.getTTable());
      }
    }
  }

  /**
   * Move all of the files from the temp directory to the final location
   * @param fs the output file system
   * @param file the file to move
   * @param srcDir the source directory
   * @param destDir the target directory
   * @param dryRun - a flag that simply tests if this move would succeed or not based
   *                 on whether other files exist where we're trying to copy
   * @throws java.io.IOException
   */
  private void moveTaskOutputs(FileSystem fs, Path file, Path srcDir,
                 Path destDir, final boolean dryRun, boolean immutable
      ) throws IOException {
    if(LOG.isDebugEnabled()) {
      LOG.debug("moveTaskOutputs "
          + file + " from: " + srcDir + " to: " + destDir
          + " dry: " + dryRun + " immutable: " + immutable);
    }

    if (dynamicPartitioningUsed) {
      immutable = true; // Making sure we treat dynamic partitioning jobs as if they were immutable.
    }

    if (file.getName().equals(TEMP_DIR_NAME) || file.getName().equals(LOGS_DIR_NAME) || file.getName().equals(SUCCEEDED_FILE_NAME)) {
      return;
    }

    final Path finalOutputPath = getFinalPath(fs, file, srcDir, destDir, immutable);
    FileStatus fileStatus = fs.getFileStatus(file);

    if (!fileStatus.isDir()) {
      if (dryRun){
        if (immutable){
          // Dryrun checks are meaningless for mutable table - we should always succeed
          // unless there is a runtime IOException.
          if(LOG.isDebugEnabled()) {
            LOG.debug("Testing if moving file: [" + file + "] to ["
                + finalOutputPath + "] would cause a problem");
          }
          if (fs.exists(finalOutputPath)) {
            throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Data already exists in "
                + finalOutputPath + ", duplicate publish not possible.");
          }
        }
      } else {
        if(LOG.isDebugEnabled()) {
          LOG.debug("Moving file: [ " + file + "] to [" + finalOutputPath + "]");
        }
        // Make sure the parent directory exists.  It is not an error
        // to recreate an existing directory
        fs.mkdirs(finalOutputPath.getParent());
        if (!fs.rename(file, finalOutputPath)) {
          if (!fs.delete(finalOutputPath, true)) {
            throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Failed to delete existing path " + finalOutputPath);
          }
          if (!fs.rename(file, finalOutputPath)) {
            throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Failed to move output to " + finalOutputPath);
          }
        }
      }
    } else {

      FileStatus[] children = fs.listStatus(file);
      FileStatus firstChild = null;
      if (children != null) {
        int index=0;
        while (index < children.length) {
          if ( !children[index].getPath().getName().equals(TEMP_DIR_NAME)
              && !children[index].getPath().getName().equals(LOGS_DIR_NAME)
              && !children[index].getPath().getName().equals(SUCCEEDED_FILE_NAME)) {
            firstChild = children[index];
            break;
          }
          index++;
        }
      }
      if(firstChild!=null && firstChild.isDir()) {
        // If the first child is directory, then rest would be directory too according to HCatalog dir structure
        // recurse in that case
        for (FileStatus child : children) {
          moveTaskOutputs(fs, child.getPath(), srcDir, destDir, dryRun, immutable);
        }
      } else {

        if (!dryRun) {
          if (dynamicPartitioningUsed) {

            // Optimization: if the first child is file, we have reached the leaf directory, move the parent directory itself
            // instead of moving each file under the directory. See HCATALOG-538
            // Note for future Append implementation : This optimization is another reason dynamic
            // partitioning is currently incompatible with append on mutable tables.

            final Path parentDir = finalOutputPath.getParent();

            // NOTE: The following is very ugly code which masks broken behavior and works around bad
            // behavior in the FileSystem implementation.
            //
            // Creating the following placeholder file has the side effect of creating all the parent
            // directories required to move the leaf directories. This code masked the bug addressed by this
            // patch.
            //
            // The placeholder file also addresses bad behavior in the FileSystem implementation.
            // Calling rename(tbl/_DYN/a/b/c, tbl/a/b) will move the contents of the source directory into
            // the destination directory if the destination directory is empty. A work around is to call
            // rename(tbl/_DYN/a/b/c, tbl/a/b/c) to achieve the expected behavior, however there is no
            // guarantee that tbl/a/b/c will not be created between calls for the existence of that directory
            // and rename, which would result in undefined behavior. If the directory is created and is empty,
            // then the contents would be moved as expected and the concurrent process might also try to move
            // data there; Otherwise tbl/a/b/c/c will be created.
            Path placeholder = new Path(parentDir, "_placeholder_" + String.valueOf(Math.random()));
            if (fs.mkdirs(parentDir)) {
              fs.create(placeholder).close();
            }

            if (LOG.isDebugEnabled()) {
              LOG.debug("Moving directory: " + file + " to " + parentDir);
            }

            // If custom dynamic location provided, need to rename to final output path
            Path dstPath = ! customDynamicLocationUsed ? parentDir : finalOutputPath;
            if (!fs.rename(file, dstPath)) {
              final String msg = "Failed to move file: " + file + " to " + dstPath;
              LOG.error(msg);
              throw new HCatException(ErrorType.ERROR_MOVE_FAILED, msg);
            }

            fs.delete(placeholder, false);
          } else {

            // In case of no partition we have to move each file
            for (FileStatus child : children) {
              moveTaskOutputs(fs, child.getPath(), srcDir, destDir, dryRun, immutable);
            }

          }

        } else {
          if(immutable && fs.exists(finalOutputPath) && !MetaStoreUtils.isDirEmpty(fs, finalOutputPath)) {

            throw new HCatException(ErrorType.ERROR_DUPLICATE_PARTITION, "Data already exists in " + finalOutputPath
                + ", duplicate publish not possible.");
          }

        }
      }
    }
  }

  /**
   * Find the final name of a given output file, given the output directory
   * and the work directory. If immutable, attempt to create file of name
   * _aN till we find an item that does not exist.
   * @param file the file to move
   * @param src the source directory
   * @param dest the target directory
   * @return the final path for the specific output file
   * @throws java.io.IOException
   */
  private Path getFinalPath(FileSystem fs, Path file, Path src,
                Path dest, final boolean immutable) throws IOException {
    URI taskOutputUri = file.toUri();
    URI relativePath = src.toUri().relativize(taskOutputUri);
    if (taskOutputUri == relativePath) {
      throw new HCatException(ErrorType.ERROR_MOVE_FAILED, "Can not get the relative path: base = " +
        src + " child = " + file);
    }
    if (relativePath.getPath().length() > 0) {

      Path itemDest = new Path(dest, relativePath.getPath());
      if (!immutable){
        String name = relativePath.getPath();
        String filetype;
        int index = name.lastIndexOf('.');
        if (index >= 0) {
          filetype = name.substring(index);
          name = name.substring(0, index);
        } else {
          filetype = "";
        }

        // Attempt to find maxAppendAttempts possible alternatives to a filename by
        // appending _a_N and seeing if that destination also clashes. If we're
        // still clashing after that, give up.
        int counter = 1;
        for (; fs.exists(itemDest) && counter < maxAppendAttempts; counter++) {
          itemDest = new Path(dest, name + (APPEND_SUFFIX + counter) + filetype);
        }

        if (counter == maxAppendAttempts){
          throw new HCatException(ErrorType.ERROR_MOVE_FAILED,
              "Could not find a unique destination path for move: file = "
                  + file + " , src = " + src + ", dest = " + dest);
        } else if (counter > APPEND_COUNTER_WARN_THRESHOLD) {
          LOG.warn("Append job used filename clash counter [" + counter
              +"] which is greater than warning limit [" + APPEND_COUNTER_WARN_THRESHOLD
              +"]. Please compact this table so that performance is not impacted."
              + " Please see HIVE-9381 for details.");
        }

      }

      if (LOG.isDebugEnabled()){
        LOG.debug("FinalPath(file:"+file+":"+src+"->"+dest+"="+itemDest);
      }

      return itemDest;
    } else {

      return dest;
    }
  }

  /**
   * Run to discover dynamic partitions available
   */
  private void discoverPartitions(JobContext context) throws IOException {
    if (!partitionsDiscovered) {
      //      LOG.info("discover ptns called");
      OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(context.getConfiguration());

      harProcessor.setEnabled(jobInfo.getHarRequested());

      List<Integer> dynamicPartCols = jobInfo.getPosOfDynPartCols();
      int maxDynamicPartitions = jobInfo.getMaxDynamicPartitions();

      Path loadPath = new Path(jobInfo.getLocation());
      FileSystem fs = loadPath.getFileSystem(context.getConfiguration());

      // construct a path pattern (e.g., /*/*) to find all dynamically generated paths
      String dynPathSpec = loadPath.toUri().getPath();
      dynPathSpec = dynPathSpec.replaceAll("__HIVE_DEFAULT_PARTITION__", "*");

      //      LOG.info("Searching for "+dynPathSpec);
      Path pathPattern = new Path(dynPathSpec);
      FileStatus[] status = fs.globStatus(pathPattern, FileUtils.HIDDEN_FILES_PATH_FILTER);

      partitionsDiscoveredByPath = new LinkedHashMap<String, Map<String, String>>();
      contextDiscoveredByPath = new LinkedHashMap<String, JobContext>();


      if (status.length == 0) {
        //        LOG.warn("No partition found genereated by dynamic partitioning in ["
        //            +loadPath+"] with depth["+jobInfo.getTable().getPartitionKeysSize()
        //            +"], dynSpec["+dynPathSpec+"]");
      } else {
        if ((maxDynamicPartitions != -1) && (status.length > maxDynamicPartitions)) {
          this.partitionsDiscovered = true;
          throw new HCatException(ErrorType.ERROR_TOO_MANY_DYNAMIC_PTNS,
            "Number of dynamic partitions being created "
              + "exceeds configured max allowable partitions["
              + maxDynamicPartitions
              + "], increase parameter ["
              + HiveConf.ConfVars.DYNAMICPARTITIONMAXPARTS.varname
              + "] if needed.");
        }

        for (FileStatus st : status) {
          LinkedHashMap<String, String> fullPartSpec = new LinkedHashMap<String, String>();
          if (!customDynamicLocationUsed) {
            Warehouse.makeSpecFromName(fullPartSpec, st.getPath());
          } else {
            HCatFileUtil.getPartKeyValuesForCustomLocation(fullPartSpec, jobInfo,
                st.getPath().toString());
          }
          partitionsDiscoveredByPath.put(st.getPath().toString(), fullPartSpec);
          JobConf jobConf = (JobConf)context.getConfiguration();
          JobContext currContext = HCatMapRedUtil.createJobContext(
            jobConf,
            context.getJobID(),
            InternalUtil.createReporter(HCatMapRedUtil.createTaskAttemptContext(jobConf,
              ShimLoader.getHadoopShims().getHCatShim().createTaskAttemptID())));
          HCatOutputFormat.configureOutputStorageHandler(currContext, jobInfo, fullPartSpec);
          contextDiscoveredByPath.put(st.getPath().toString(), currContext);
        }
      }

      //      for (Entry<String,Map<String,String>> spec : partitionsDiscoveredByPath.entrySet()){
      //        LOG.info("Partition "+ spec.getKey());
      //        for (Entry<String,String> e : spec.getValue().entrySet()){
      //          LOG.info(e.getKey() + "=>" +e.getValue());
      //        }
      //      }

      this.partitionsDiscovered = true;
    }
  }

  private void registerPartitions(JobContext context) throws IOException{
    if (dynamicPartitioningUsed){
      discoverPartitions(context);
    }
    OutputJobInfo jobInfo = HCatOutputFormat.getJobInfo(context.getConfiguration());
    Configuration conf = context.getConfiguration();
    Table table = new Table(jobInfo.getTableInfo().getTable());
    Path tblPath = new Path(table.getTTable().getSd().getLocation());
    FileSystem fs = tblPath.getFileSystem(conf);

    if( table.getPartitionKeys().size() == 0 ) {
      //Move data from temp directory the actual table directory
      //No metastore operation required.
      Path src = new Path(jobInfo.getLocation());
      moveTaskOutputs(fs, src, src, tblPath, false, HCatUtil.isImmutable(table, conf));
      if (!src.equals(tblPath)){
        fs.delete(src, true);
      }

      // TODO: We should probably call applyGroupAndPerms here as well, however this becomes
      // complicated if tables are mutable and group/permissions have changed between inserts.

      return;
    }

    IMetaStoreClient client = null;
    HCatTableInfo tableInfo = jobInfo.getTableInfo();
    List<Partition> partitionsAdded = new ArrayList<Partition>();
    try {
      HiveConf hiveConf = HCatUtil.getHiveConf(conf);
      client = HCatUtil.getHiveMetastoreClient(hiveConf);
      StorerInfo storer = InternalUtil.extractStorerInfo(table.getTTable().getSd(),table.getParameters());

      FileStatus tblStat = fs.getFileStatus(tblPath);
      String tblGroup = tblStat.getGroup();
      FsPermission tblPerms = tblStat.getPermission();
      ArrayList<AclEntry> tblAcls = null;

      try {
        AclStatus stat = fs.getAclStatus(tblPath);
        if (hasExtendedAcls(stat)) {
          tblAcls = getDefaultAclEntries(stat, tblPerms);
        }
      } catch (AclException e) {
        LOG.debug("Skipping ACLs", e);
      } catch (UnsupportedOperationException e) {
        LOG.debug("Skipping ACLs", e);
      }

      List<Partition> partitionsToAdd = new ArrayList<Partition>();
      if (! dynamicPartitioningUsed){
        partitionsToAdd.add(constructPartition(jobInfo, tblPath.toString(),
              jobInfo.getPartitionValues(), jobInfo.getOutputSchema(), getStorerParameterMap(storer),
              table, fs, tblGroup, tblPerms, tblAcls));
      } else {
        // NOTE: Must initialize ptnRootLocation before ptnRootLocation is referenced below.
        // Call it the same way as abortJob would.
        getPartitionRootLocation(jobInfo.getLocation(), jobInfo.getTableInfo().getTable().getPartitionKeysSize());

        partitionsToAdd = constructPartitions(context, jobInfo, tblPath.toString(),
              partitionsDiscoveredByPath, jobInfo.getOutputSchema(), getStorerParameterMap(storer),
              table, fs, tblGroup, tblPerms, tblAcls);
      }

      ArrayList<Map<String,String>> ptnInfos = new ArrayList<Map<String,String>>();
      for(Partition ptn : partitionsToAdd){
        ptnInfos.add(InternalUtil.createPtnKeyValueMap(new Table(tableInfo.getTable()), ptn));
      }

      /**
       * Dynamic partitioning & Append incompatibility note:
       *
       * Currently, we do not support mixing dynamic partitioning and append in the
       * same job. One reason is that we need exhaustive testing of corner cases
       * for that, and a second reason is the behaviour of add_partitions. To support
       * dynamic partitioning with append, we'd have to have a add_partitions_if_not_exist
       * call, rather than an add_partitions call. Thus far, we've tried to keep the
       * implementation of append jobtype-agnostic, but here, in code, we assume that
       * a table is considered immutable if dynamic partitioning is enabled on the job.
       *
       * This does not mean that we can check before the job begins that this is going
       * to be a dynamic partition job on an immutable table and thus fail the job, since
       * it is quite possible to have a dynamic partitioning job run on an unpopulated
       * immutable table. It simply means that at the end of the job, as far as copying
       * in data is concerned, we will pretend that the table is immutable irrespective
       * of what table.isImmutable() tells us.
       */

      //Publish the new partition(s)
      if (harProcessor.isEnabled() && dynamicPartitioningUsed){
        if (!partitionsToAdd.isEmpty()) {
          if (!customDynamicLocationUsed) {
            Path src = new Path(ptnRootLocation);
            // check here for each dir we're copying out, to see if it
            // already exists, error out if so.
            // Also, treat dyn-writes as writes to immutable tables.
            moveTaskOutputs(fs, src, src, tblPath, true, true); // dryRun = true, immutable = true
            moveTaskOutputs(fs, src, src, tblPath, false, true);
            if (!src.equals(tblPath)){
              fs.delete(src, true);
            }
          } else {
            moveCustomLocationTaskOutputs(fs, table, hiveConf);
          }
          try {
            updateTableSchema(client, table, jobInfo.getOutputSchema());
            LOG.info("HAR is being used. The table {} has new partitions {}.", table.getTableName(), ptnInfos);
            registerPartitionsImpl(client, partitionsToAdd);
            partitionsAdded = partitionsToAdd;
          } catch (Exception e){
            // There was an error adding partitions : rollback fs copy and rethrow
            for (Partition p : partitionsToAdd){
              Path ptnPath = new Path(harProcessor.getParentFSPath(new Path(p.getSd().getLocation())));
              if (fs.exists(ptnPath)){
                fs.delete(ptnPath,true);
              }
            }
            throw e;
          }
        }
      } else {

        // no harProcessor, regular operation
        updateTableSchema(client, table, jobInfo.getOutputSchema());
        LOG.info("HAR not is not being used. The table {} has new partitions {}.", table.getTableName(), ptnInfos);
        if (partitionsToAdd.size() > 0) {
          if (!dynamicPartitioningUsed ) {

            // regular single-partition write into a partitioned table.
            //Move data from temp directory the actual table directory
            if (partitionsToAdd.size() > 1){
              throw new HCatException(ErrorType.ERROR_PUBLISHING_PARTITION,
                  "More than one partition to publish in non-dynamic partitioning job");
            }
            Partition p = partitionsToAdd.get(0);
            Path src = new Path(jobInfo.getLocation());
            Path dest = new Path(p.getSd().getLocation());
            moveTaskOutputs(fs, src, src, dest, true, HCatUtil.isImmutable(table, conf));
            moveTaskOutputs(fs,src,src,dest,false, HCatUtil.isImmutable(table, conf));
            if (!src.equals(dest)){
              if (src.toString().matches(".*" + Path.SEPARATOR + SCRATCH_DIR_NAME + "\\d\\.?\\d+.*")){
                // src is scratch directory, need to trim the part key value pairs from path
                String diff = StringUtils.difference(src.toString(), dest.toString());
                fs.delete(new Path(StringUtils.substringBefore(src.toString(), diff)), true);
              } else {
                fs.delete(src, true);
              }
            }

            // Now, we check if the partition already exists. If not, we go ahead.
            // If so, we error out if immutable, and if mutable, check that the partition's IF
            // matches our current job's IF (table's IF) to check for compatibility. If compatible, we
            // ignore and do not add. If incompatible, we error out again.

            boolean publishRequired = false;
            try {
              Partition existingP = client.getPartition(p.getDbName(),p.getTableName(),p.getValues());
              if (existingP != null){
                if (HCatUtil.isImmutable(table, conf)) {
                  throw new HCatException(ErrorType.ERROR_DUPLICATE_PARTITION,
                      "Attempted duplicate partition publish on to immutable table");
                } else {
                  if (! existingP.getSd().getInputFormat().equals(table.getInputFormatClass().getName())){
                    throw new HCatException(ErrorType.ERROR_PUBLISHING_PARTITION,
                        "Attempted partition append, where old partition format was "
                            + existingP.getSd().getInputFormat()
                            + " and table format was "
                            + table.getInputFormatClass().getName());
                  }
                }
              } else {
                publishRequired = true;
              }
            } catch (NoSuchObjectException e){
              // All good, no such partition exists, move on.
              publishRequired = true;
            }
            if (publishRequired){
              registerPartitionsImpl(client, partitionsToAdd);
              partitionsAdded = partitionsToAdd;
            }

          } else {
            // Dynamic partitioning usecase
            if (!customDynamicLocationUsed) {
              Path src = new Path(ptnRootLocation);
              moveTaskOutputs(fs, src, src, tblPath, true, true); // dryRun = true, immutable = true
              moveTaskOutputs(fs, src, src, tblPath, false, true);
              if (!src.equals(tblPath)){
                fs.delete(src, true);
              }
            } else {
              moveCustomLocationTaskOutputs(fs, table, hiveConf);
            }
            registerPartitionsImpl(client, partitionsToAdd);
            partitionsAdded = partitionsToAdd;
          }
        }

        // Examine the first partition to determine whether group/permissions need to be
        // updated. Test whether group can be changed.
        if (partitionsAdded.size() > 0) {
          Path path = new Path(partitionsAdded.get(0).getSd().getLocation());

          // Check for group differences.
          FileStatus fstat = fs.getFileStatus(path);
          String newGroup = compareGroups(fs, fstat, tblGroup);

          // Check for permissions differences.
          FsPermission newPerms = compareDirPermissions(fstat, tblPerms);

          // Check permissions for one of the files.
          // In some cases the directory permissions are correct, but the file permissions are not.
          if (newPerms == null) {
            RemoteIterator<FileStatus> iter = fs.listStatusIterator(path);
            fstat = iter.hasNext() ? iter.next() : null;

            if (fstat != null) {
              newPerms = compareFilePermissions(fstat, tblPerms);
            }
          }

          // Set groups/permissions to mimic the table.
          if (newGroup != null || newPerms != null || tblAcls != null) {
            for (Partition part : partitionsAdded) {
              path = new Path(part.getSd().getLocation());
              LOG.info("Group and/or permissions differ from table for partition: " + path);
              applyGroupAndPerms(fs, path, newGroup, newPerms, tblAcls, true);
            }
          }
        }
      }
    } catch (Exception e) {
      if (partitionsAdded.size() > 0) {
        try {
          // baseCommitter.cleanupJob failed, try to clean up the
          // metastore
          cleanUpPartitions(fs, client, partitionsAdded);
        } catch (Exception te) {
          // Keep cause as the original exception
          throw new HCatException(
              ErrorType.ERROR_PUBLISHING_PARTITION, e);
        }
      }
      if (e instanceof HCatException) {
        throw (HCatException) e;
      } else {
        throw new HCatException(ErrorType.ERROR_PUBLISHING_PARTITION, e);
      }
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
  }

  private void registerPartitionsImpl(IMetaStoreClient hiveMetaStoreClient,
                                        List<Partition> partitions) throws TException {
    if (!skipRegisterPartitions) {
      hiveMetaStoreClient.add_partitions(partitions);
    }
    else {
      LOG.info("Skipping partition registration.");
    }
  }

  private void cleanUpPartitions(FileSystem fs, IMetaStoreClient hiveMetaStoreClient,
                                 List<Partition> partitions) throws TException, IOException {
    if (!skipRegisterPartitions) {
      LOG.info("Attempting cleanup of partitions from metastore.");
      for (Partition p : partitions) {
        hiveMetaStoreClient.dropPartition(p.getDbName(), p.getTableName(), p.getValues(), true);
      }
    }
    else {
      LOG.info("Partition-registration was skipped. Cleaning up outputs from HDFS.");
      for (Partition p : partitions) {
        fs.delete(new Path(p.getSd().getLocation()), true);
      }
    }
  }

  private void moveCustomLocationTaskOutputs(FileSystem fs, Table table, Configuration conf)
    throws IOException {
    // in case of custom dynamic partitions, we can't just move the sub-tree of partition root
    // directory since the partitions location contain regex pattern. We need to first find the
    // final destination of each partition and move its output.
    for (Entry<String, Map<String, String>> entry : partitionsDiscoveredByPath.entrySet()) {
      Path src = new Path(entry.getKey());
      Path destPath = new Path(getFinalDynamicPartitionDestination(table, entry.getValue(), jobInfo));
      moveTaskOutputs(fs, src, src, destPath, true, true); // dryRun = true, immutable = true
      moveTaskOutputs(fs, src, src, destPath, false, true);
    }
    // delete the parent temp directory of all custom dynamic partitions
    Path parentPath = new Path(getCustomPartitionRootLocation(jobInfo, conf));
    if (fs.exists(parentPath)) {
      fs.delete(parentPath, true);
    }
  }

  private void cancelDelegationTokens(JobContext context) throws IOException{
    LOG.info("Cancelling delegation token for the job.");
    IMetaStoreClient client = null;
    try {
      HiveConf hiveConf = HCatUtil
          .getHiveConf(context.getConfiguration());
      client = HCatUtil.getHiveMetastoreClient(hiveConf);
      // cancel the deleg. tokens that were acquired for this job now that
      // we are done - we should cancel if the tokens were acquired by
      // HCatOutputFormat and not if they were supplied by Oozie.
      // In the latter case the HCAT_KEY_TOKEN_SIGNATURE property in
      // the conf will not be set
      String tokenStrForm = client.getTokenStrForm();
      String hCatKeyTokenSignature = context.getConfiguration().get(HCatConstants.HCAT_KEY_TOKEN_SIGNATURE);
      if (tokenStrForm != null
          && hCatKeyTokenSignature != null) {
        LOG.info("FileOutputCommitterContainer::cancelDelegationTokens(): " +
            "Cancelling token fetched for HCAT_KEY_TOKEN_SIGNATURE == (" + hCatKeyTokenSignature + ").");
        client.cancelDelegationToken(tokenStrForm);
      }
      else {
        LOG.info("FileOutputCommitterContainer::cancelDelegationTokens(): " +
            "Could not find tokenStrForm, or HCAT_KEY_TOKEN_SIGNATURE. Skipping token cancellation.");
      }
    } catch (MetaException e) {
      LOG.warn("MetaException while cancelling delegation token.", e);
    } catch (TException e) {
      LOG.warn("TException while cancelling delegation token.", e);
    } finally {
      HCatUtil.closeHiveClientQuietly(client);
    }
  }

  /**
   * Returns a list of AclEntry including DEFAULT ACLs and ACCESS ACLs as derived from the DEFAULTs.
   * @param stat the AclStatus of the parent directory.
   * @param perms the FsPermission of the parent directory.
   * @return a list of AclEntry objects.
   */
  private ArrayList<AclEntry> getDefaultAclEntries(AclStatus stat, FsPermission perms) {
    ArrayList<AclEntry> defaults = new ArrayList<AclEntry>();

    boolean[] hasDefaults = { false, false, false, false };

    for (AclEntry e : stat.getEntries()) {
      if (e.getScope() == AclEntryScope.DEFAULT) {
        AclEntry acl = new AclEntry.Builder().setName(e.getName()).setScope(AclEntryScope.ACCESS)
          .setType(e.getType()).setPermission(e.getPermission()).build();

        defaults.add(acl);
        defaults.add(e);

        if (e.getName() ==  null) {
          if (e.getType() == AclEntryType.USER) {
            hasDefaults[0] = true;
          }
          if (e.getType() == AclEntryType.GROUP) {
            hasDefaults[1] = true;
          }
          if (e.getType() == AclEntryType.OTHER) {
            hasDefaults[2] = true;
          }
          if (e.getType() == AclEntryType.MASK) {
            hasDefaults[3] = true;
          }
        }
      }
    }

    if (! hasDefaults[0]) {
      AclEntry acl = new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.USER).setPermission(perms.getUserAction()).build();
      defaults.add(acl);

      hasDefaults[0] = true;
    }

    if (! hasDefaults[1]) {
      for (AclEntry e : stat.getEntries()) {
        if ((e.getScope() == AclEntryScope.ACCESS) && (e.getType() == AclEntryType.GROUP) && (e.getName() == null)) {
          AclEntry acl = new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
            .setType(AclEntryType.GROUP).setPermission(e.getPermission()).build();
          defaults.add(acl);

          hasDefaults[1] = true;
        }
      }
    }

    if (! hasDefaults[2]) {
      AclEntry acl = new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.OTHER).setPermission(perms.getOtherAction()).build();
      defaults.add(acl);

      hasDefaults[2] = true;
    }

    if (! hasDefaults[3]) {
      AclEntry acl = new AclEntry.Builder().setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.MASK).setPermission(perms.getGroupAction()).build();
      defaults.add(acl);

      hasDefaults[3] = true;
    }

    return defaults;
  }

  /**
   * Returns a list of AclEntry excluding the default ACLs.
   * Hadoop will throw an exception if DEFAULT ACLs are applied to files.
   * @param acls the list of ACLs for the parent directory.
   * @return a list of AclEntry excluding the default ACLs.
   */
  private List<AclEntry> getFileAclEntries(List<AclEntry> acls) {
    ArrayList<AclEntry> fileAcls = null;

    if (acls != null) {
      fileAcls = new ArrayList<AclEntry>();

      for (AclEntry entry : acls) {
        if (entry.getScope() != AclEntryScope.DEFAULT) {
          fileAcls.add(entry);
        }
      }
    }

    return fileAcls;
  }

  private boolean hasExtendedAcls(AclStatus status) {
    if (status != null) {
      return status.getEntries().size() > 0;
    }   

    return false;
  }

  private String compareGroups(FileSystem fs, FileStatus fstat, String group) throws IOException {
    if (! group.equals(fstat.getGroup())) {
      // Attempt to change the group on one file to determine if the user is part of the group.
      try {
        fs.setOwner(fstat.getPath(), null, group);
        return group;
      } catch (AccessControlException e) {
        LOG.warn("Unable to change group to: " + group, e);
      }
    }

    return null;
  }

  private FsPermission comparePermissions(FileStatus fstat, FsPermission perms, short mask) {
    short permsMasked = (short) (perms.toShort() & mask);
    short perm = (short) (fstat.getPermission().toShort() & mask);
    if (permsMasked != perm) {
      return perms;
    }

    return null;
  }

  private FsPermission compareDirPermissions(FileStatus fstat, FsPermission perms) {
    short mask = 00777;

    return comparePermissions(fstat, perms, mask);
  }

  private FsPermission compareFilePermissions(FileStatus fstat, FsPermission perms) {
    short mask = 00666;

    return comparePermissions(fstat, perms, mask);
  }
}