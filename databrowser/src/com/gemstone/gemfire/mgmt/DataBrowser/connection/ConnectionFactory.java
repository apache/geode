/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.connection;

import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.internal.DummyMemberDiscoveryImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.internal.GemFireClientConnectionImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.internal.JMXDiscoveryImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.prefs.DataBrowserPreferences;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;
/**
 * This class provides factory method to prepare a connection to a GemFire
 * system.
 *
 * @author Hrishi/Manish
 **/
public class ConnectionFactory {
  private static final String DATABROWSER_PROPS                 = "/com/gemstone/gemfire/mgmt/DataBrowser/resources/databrowser.properties";
  private static final String CLIENT_AUTHRIZATION               = "security-client-auth-init";

  public static GemFireConnection createGemFireConnection(DSConfiguration config)
      throws ConnectionFailureException {

    GFMemberDiscovery disc = null;
    try {
      disc = new JMXDiscoveryImpl(config.getHost(), config.getPort(), "MySystem", config.getUserName(), config.getPassword());
    } catch (ConnectionFailureException e) {
      throw e;
    }
    config.setVersion(disc.getGemFireSystemVersion());
    
    DistributedSystem system = null;
    try {
      Properties props = getStandardProperties();
      addSecurityProperties(props, config.getSecAttributes());
      system = DistributedSystem.connect(props);
    } catch (Exception e) {
      throw new ConnectionFailureException(
          "Failed to create local distributed system. Reason : "+e.getMessage(), e);
    }
    
    try {
      GemFireClientConnectionImpl connection = new GemFireClientConnectionImpl(system, disc);
      return connection;
    } catch (Exception e) {
      throw new ConnectionFailureException(
          "Failed to create local cache. Reason : "+e.getMessage(), e);
    }
  }

  //Used for DUnit testing...
  public static GemFireConnection createGemFireConnection(
      ClientConfiguration config) throws ConnectionFailureException {

    Properties props = getStandardProperties();

    try {
      DistributedSystem system = DistributedSystem.connect(props);

      GFMemberDiscovery disc = new DummyMemberDiscoveryImpl(config);
      GemFireClientConnectionImpl connection = new GemFireClientConnectionImpl(
          system, disc);
      return connection;
    }
    catch (Exception e) {
      LogUtil.error("Error while making connection", e);
      throw new ConnectionFailureException(
          "Failed to connect to the distributed system. Reason : "+e.getMessage(), e);
    }
  }


  public static Properties getStandardProperties() {
    String logDir = LogUtil.getLogDir();
    //TODO localize the file name
    String logFile = logDir + "/" + "query-client_"+LogUtil.getProcessId()+".log";

    String logLevel  = DataBrowserPreferences.getLoggingLevel();
    logLevel         = logLevel.equals("OFF") ? "NONE" : logLevel;//See #740
    int logFileSize  = DataBrowserPreferences.getLogFileSize();
    int logFileCount = DataBrowserPreferences.getLogFileCount();
    int diskSpace    = logFileSize * logFileCount;
    
    Properties props = new Properties();
    props.put(DistributedSystemConfig.NAME_NAME, "Data-Browser");
    props.put(DistributedSystemConfig.LOG_LEVEL_NAME, logLevel);
    props.put(DistributedSystemConfig.LOG_FILE_NAME, logFile);
    props.put(DistributedSystemConfig.LOG_FILE_SIZE_LIMIT_NAME, String.valueOf(logFileSize));
    props.put(DistributedSystemConfig.LOG_DISK_SPACE_LIMIT_NAME, String.valueOf(diskSpace));
    
    //Setting cache-xml-file specification to empty string, to prevent DB reading any other cache.xml 
    //see #829
    props.put("cache-xml-file", "");
    
    props.put(DistributedSystemConfig.MCAST_PORT_NAME, "0");
    props.put(DistributedSystemConfig.LOCATORS_NAME, "");
    
    // setting default  databrowser properties. It will not allow DB read out any other gemfire properties kept
    // in  current dir/ home dir/ classpath
    // see #829
    System.setProperty("gemfirePropertyFile", DATABROWSER_PROPS);
    return props;
  }

  public static void addSecurityProperties(Properties props, SecurityAttributes secAttrs ) {
    if(secAttrs == null || props == null)
      return;
    String authImpl = secAttrs.getAuthImplString();
    if(authImpl != null)
      props.put(CLIENT_AUTHRIZATION, authImpl);

    Map<String, String> securityProperties = secAttrs.getSecurityProperties();
    props.putAll(securityProperties);
  }
}
