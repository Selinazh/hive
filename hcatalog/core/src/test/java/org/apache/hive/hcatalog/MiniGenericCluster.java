package org.apache.hive.hcatalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.pig.ExecType;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;

import java.io.IOException;
import java.util.Properties;

abstract public class MiniGenericCluster {
  protected MiniDFSCluster m_dfs = null;
  protected FileSystem m_fileSys = null;
  protected Configuration m_conf = null;

  protected static MiniGenericCluster INSTANCE = null;
  protected static boolean isSetup = false;

  public static String EXECTYPE_MR = "mr";

  /**
   * Returns the single instance of class MiniGenericCluster that represents
   * the resources for a mini dfs cluster and a mini mr (or tez) cluster. The
   * system property "test.exec.type" is used to decide whether a mr or tez mini
   * cluster will be returned.
   */
  public static MiniGenericCluster buildCluster() {
    if (INSTANCE == null) {
      String execType = System.getProperty("test.exec.type");
      if (execType == null) {
        // Default to MR
        System.setProperty("test.exec.type", EXECTYPE_MR);
        return buildCluster(EXECTYPE_MR);
      }

      return buildCluster(execType);
    }
    return INSTANCE;
  }

  public static MiniGenericCluster buildCluster(String execType) {
    if (INSTANCE == null) {
      if (execType.equalsIgnoreCase(EXECTYPE_MR)) {
        INSTANCE = new MiniCluster();
      }
      // TODO: Add support for TezMiniCluster.
      else {
        throw new RuntimeException("Unknown test.exec.type: " + execType);
      }
    }
    if (!isSetup) {
      INSTANCE.setupMiniDfsAndMrClusters();
      isSetup = true;
    }
    return INSTANCE;
  }

  abstract public ExecType getExecType();

  abstract protected void setupMiniDfsAndMrClusters();

  public void shutDown(){
    INSTANCE.shutdownMiniDfsAndMrClusters();
  }

  @Override
  protected void finalize() {
    shutdownMiniDfsAndMrClusters();
  }

  protected void shutdownMiniDfsAndMrClusters() {
    isSetup = false;
    shutdownMiniDfsClusters();
    shutdownMiniMrClusters();
    m_conf = null;
  }

  protected void shutdownMiniDfsClusters() {
    try {
      if (m_fileSys != null) { m_fileSys.close(); }
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (m_dfs != null) { m_dfs.shutdown(); }
    m_fileSys = null;
    m_dfs = null;
  }

  abstract protected void shutdownMiniMrClusters();

  public Properties getProperties() {
    errorIfNotSetup();
    return ConfigurationUtil.toProperties(m_conf);
  }

  public Configuration getConfiguration() {
    return new Configuration(m_conf);
  }

  public void setProperty(String name, String value) {
    errorIfNotSetup();
    m_conf.set(name, value);
  }

  public FileSystem getFileSystem() {
    errorIfNotSetup();
    return m_fileSys;
  }

  /**
   * Throw RunTimeException if isSetup is false
   */
  private void errorIfNotSetup(){
    if(isSetup)
      return;
    String msg = "function called on MiniCluster that has been shutdown";
    throw new RuntimeException(msg);
  }
}
