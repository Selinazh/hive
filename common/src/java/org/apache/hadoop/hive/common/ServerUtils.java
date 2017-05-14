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

package org.apache.hadoop.hive.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.IOException;
import java.security.AccessControlException;

/**
 * ServerUtils (specific to HiveServer version 1)
 */
public class ServerUtils {

  public static final Log LOG = LogFactory.getLog(ServerUtils.class);

  public static void cleanUpScratchDir(HiveConf hiveConf) {
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_START_CLEANUP_SCRATCHDIR)) {
      String hiveScratchDir = hiveConf.get(HiveConf.ConfVars.SCRATCHDIR.varname);
      try {
        Path jobScratchDir = new Path(hiveScratchDir);
        LOG.info("Cleaning scratchDir : " + hiveScratchDir);
        FileSystem fileSystem = jobScratchDir.getFileSystem(hiveConf);
        fileSystem.delete(jobScratchDir, true);
      }
      // Even if the cleanup throws some exception it will continue.
      catch (Throwable e) {
        LOG.warn("Unable to delete scratchDir : " + hiveScratchDir, e);
      }
    }
  }

  /*
   * Scratch directory should have sticky bit set so that one cannot delete other user's
   * temporary data. If the scratch directory exists, then it should have sticky bit set.
   * If it does not exist, it will be created with Sticky bit. If it exists and does not
   * have sticky bit, that will be set and failure to do so will result in AccessControException.
   */
  public static void checkAndSetScratchDirectory(HiveConf hiveConf) throws IOException {

    String hiveScratchDir = hiveConf.get(HiveConf.ConfVars.SCRATCHDIR.varname);
    Path jobScratchDir = new Path(hiveScratchDir);
    FsPermission fsPermission = new FsPermission((short) 01777);

    FileSystem fileSystem = jobScratchDir.getFileSystem(hiveConf);
    if (fileSystem.exists(jobScratchDir)) {
      if (!fileSystem.getFileStatus(jobScratchDir).getPermission().getStickyBit()) {
        LOG.info("Scratch directory exists without sticky bit, setting it.");
        fileSystem.setPermission(jobScratchDir, fsPermission);
      }
    } else {
      LOG.info("Scratch directory does not exist, creating it with sticky bit.");
      fileSystem.mkdirs(jobScratchDir, fsPermission);
    }
  }
}