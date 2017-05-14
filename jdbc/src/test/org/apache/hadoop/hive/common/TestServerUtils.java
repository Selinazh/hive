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

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.service.server.HiveServer2;
import org.junit.Assert;

import java.io.IOException;
import java.security.AccessControlException;
import java.sql.SQLException;
import java.util.Properties;

public class TestServerUtils extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestServerUtils.class.getName());

  private FileSystem fs = null;
  private Path scratchPath = null;
  private HiveConf hiveConf = null;
  private HiveServer2 hiveServer2;

  @Override
  public void setUp() throws IOException {
    hiveConf = new HiveConf();
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);

    String scratchDir = hiveConf.getVar(HiveConf.ConfVars.SCRATCHDIR);
    LOG.info("Creating scratch directory before HiveServer2 starts");
    scratchPath = new Path(scratchDir);
    fs = scratchPath.getFileSystem(hiveConf);
    fs.mkdirs(scratchPath);

    LOG.info("Initializing HiveServer2");
    hiveServer2 = new HiveServer2();
    hiveServer2.init(hiveConf);
  }

  @Override
  public void tearDown() throws IOException {
    fs.delete(scratchPath, true);
  }

  public void testStickyBitSet() throws IOException {
    fs.setPermission(scratchPath, new FsPermission((short) 01777));
    try {
      ServerUtils.checkAndSetScratchDirectory(hiveConf);
    } catch (IOException e) {
      e.printStackTrace();
      Assert.fail("Sticky bit set, should not throw an exception");
    }
  }

  public void testScratchDirectoryDoesNotExist() {
    boolean exception_thrown;
    try {
      fs.delete(scratchPath, true);
      exception_thrown = false;
      ServerUtils.checkAndSetScratchDirectory(hiveConf);
    } catch (IOException e) {
      e.printStackTrace();
      exception_thrown = true;
    }
  }

  public void testHiveServer2StartWithStickyBit() throws IOException, InterruptedException {
    fs.setPermission(scratchPath, new FsPermission((short) 01777));
    hiveServer2.start();
    Thread.sleep(5000);
    // Check if connection goes through fine
    try {
      HiveConnection connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail("Connection failed, server should have failed to start" + e);
    } finally {
      hiveServer2.stop();
    }
  }

  public void testHiveServer2StartWithoutStickyBit() throws IOException, InterruptedException {
    fs.setPermission(scratchPath, new FsPermission((short) 0777));
    hiveServer2.start();
    Thread.sleep(5000);
    try {
      HiveConnection connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail("Connection failed, server should have started");
    }
  }

  public void testHiveServer2StartWithoutScratchDirectory() throws IOException, InterruptedException {
    fs.delete(scratchPath, true);
    hiveServer2.start();
    Thread.sleep(5000);
    try {
      HiveConnection connection = new HiveConnection("jdbc:hive2://localhost:10000/default", new Properties());
      connection.close();
    } catch (SQLException e) {
      e.printStackTrace();
      Assert.fail("Connection failed, server should have started");
    }
  }
}