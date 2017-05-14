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

package org.apache.hive.service.cli.thrift;

import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.auth.HiveAuthFactory.AuthTypes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;

/**
 *
 * TestThriftBinaryCLIService.
 * This tests ThriftCLIService started in binary mode.
 *
 */

public class TestThriftBinaryCLIService extends ThriftCLIServiceTest {

  private static String transportMode = "binary";

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set up the base class
    ThriftCLIServiceTest.setUpBeforeClass();

    assertNotNull(port);
    assertNotNull(hiveServer2);
    assertNotNull(hiveConf);

    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_THRIFT_BIND_HOST, host);
    hiveConf.setIntVar(ConfVars.HIVE_SERVER2_THRIFT_PORT, port);
    hiveConf.setVar(ConfVars.HIVE_SERVER2_AUTHENTICATION, AuthTypes.NONE.toString());
    hiveConf.setVar(ConfVars.HIVE_SERVER2_TRANSPORT_MODE, transportMode);

    //Create scratch directory
    FileSystem fs = FileSystem.get(hiveConf);
    Path scratchDirPath = new Path(hiveConf.get(HiveConf.ConfVars.SCRATCHDIR.varname));
    boolean fileExists = fs.exists(scratchDirPath);
    try { 
      if (!fileExists) {
        System.err.println("Creating scratchDir : " + scratchDirPath.toString());
        fileExists = fs.mkdirs(scratchDirPath, new FsPermission((short) 01777));
      }
      else {
        System.err.println("Setting permission on scratchDir : " + scratchDirPath.toString());
        fs.setPermission(scratchDirPath, new FsPermission((short) 01777));
      }
    }catch (IOException ioe) {
      System.err.println("Error in setting scratchDir for TestThriftBinaryCLIService " + ioe);
    }

    startHiveServer2WithConf(hiveConf);

    client = getServiceClientInternal();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ThriftCLIServiceTest.tearDownAfterClass();

    String hiveScratchDir = hiveConf.get(HiveConf.ConfVars.SCRATCHDIR.varname);
    try {
      System.err.println("Cleaning scratchDir : " + hiveScratchDir);
      Path jobScratchDir = new Path(hiveScratchDir);
      FileSystem fileSystem = jobScratchDir.getFileSystem(hiveConf);
      fileSystem.delete(jobScratchDir, true);
    } 
    //Even if the cleanup throws some exception it will continue.
    catch (Throwable e) {
      System.err.println("Unable to delete scratchDir : " + hiveScratchDir + " " + e);
    }
  }

  /**
   * @throws java.lang.Exception
   */
  @Override
  @Before
  public void setUp() throws Exception {

  }

  /**
   * @throws java.lang.Exception
   */
  @Override
  @After
  public void tearDown() throws Exception {

  }
}
