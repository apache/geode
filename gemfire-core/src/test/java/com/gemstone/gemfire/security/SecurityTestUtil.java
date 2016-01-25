package com.gemstone.gemfire.security;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;
import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLContextSpi;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.NoAvailableServersException;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.client.ServerRefusedConnectionException;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.ProxyCache;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.PureLogWriter;
import com.gemstone.gemfire.internal.util.Callable;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;

/**
 * Contains utility methods for setting up servers/clients for authentication
 * and authorization tests.
 * 
 * @author sumedh
 * @since 5.5
 */
public class SecurityTestUtil extends DistributedTestCase {

  public SecurityTestUtil(String name) {
    super(name);
  }

  private static Locator locator = null;

  private static Cache cache = null;

  private static Properties currentJavaProps = null;

  private static String locatorString = null;

  private static Integer mcastPort = null;

  public static final int NO_EXCEPTION = 0;

  public static final int AUTHREQ_EXCEPTION = 1;

  public static final int AUTHFAIL_EXCEPTION = 2;

  public static final int CONNREFUSED_EXCEPTION = 3;

  public static final int NOTAUTHZ_EXCEPTION = 4;

  public static final int OTHER_EXCEPTION = 5;
  
  public static final int NO_AVAILABLE_SERVERS = 6;

  // Indicates that AuthReqException may not necessarily be thrown
  public static final int NOFORCE_AUTHREQ_EXCEPTION = 16;

  protected static final String regionName = "AuthRegion";

  protected static final String[] keys = { "key1", "key2", "key3", "key4",
      "key5", "key6", "key7", "key8" };

  protected static final String[] values = { "value1", "value2", "value3",
      "value4", "value5", "value6", "value7", "value8" };

  protected static final String[] nvalues = { "nvalue1", "nvalue2", "nvalue3",
      "nvalue4", "nvalue5", "nvalue6", "nvalue7", "nvalue8" };

  static String[] expectedExceptions = null;

  private static Pool pool = null;

  private static boolean multiUserAuthMode = false;

  private static final int numberOfUsers = 1;

  static ProxyCache[] proxyCaches = new ProxyCache[numberOfUsers];

  private static Region regionRef = null;

  public static void addExpectedExceptions(String[] expectedExceptions,
      LogWriter logger) {
    if (expectedExceptions != null) {
      for (int index = 0; index < expectedExceptions.length; index++) {
        logger.info("<ExpectedException action=add>"
            + expectedExceptions[index] + "</ExpectedException>");
      }
    }
  }

  public static void removeExpectedExceptions(String[] expectedExceptions,
      LogWriter logger) {
    if (expectedExceptions != null) {
      for (int index = 0; index < expectedExceptions.length; index++) {
        logger.info("<ExpectedException action=remove>"
            + expectedExceptions[index] + "</ExpectedException>");
      }
    }
  }

  public static void setJavaProps(Properties javaProps) {

    removeJavaProperties(currentJavaProps);
    addJavaProperties(javaProps);
    currentJavaProps = javaProps;
  }

  public DistributedSystem createSystem(Properties sysProps, Properties javaProps) {

    closeCache();
    clearStaticSSLContext();
    setJavaProps(javaProps);

    DistributedSystem dsys = getSystem(sysProps);
    assertNotNull(dsys);
    addExpectedExceptions(SecurityTestUtil.expectedExceptions, system
        .getLogWriter());
    return dsys;
  }

  void openCache() {

    assertNotNull(system);
    assertTrue(system.isConnected());
    cache = CacheFactory.create(system);
    assertNotNull(cache);
  }

  private static void initClientDynamicRegionFactory(String poolName) {

    DynamicRegionFactory.get().open(
        new DynamicRegionFactory.Config(null, poolName, false,true));
  }

  public static void initDynamicRegionFactory() {

    DynamicRegionFactory.get().open(
        new DynamicRegionFactory.Config(null, null, false, true));
  }

  public static Integer getLocatorPort() {

    Integer locatorPort = new Integer(AvailablePort
        .getRandomAvailablePort(AvailablePort.SOCKET));
    String addr = DistributedTestCase.getIPLiteral();
    if (locatorString == null) {
      locatorString = addr + "[" + locatorPort + ']';
    }
    else {
      locatorString += "," + addr + "[" + locatorPort + ']';
    }
    return locatorPort;
  }

  /**
   * Note that this clears the string after returning for convenience in reusing
   * for other tests. Hence it should normally be invoked only once for a test.
   */
  public static String getLocatorString() {

    String locString = locatorString;
    locatorString = null;
    return locString;
  }

  public static Properties concatProperties(Properties[] propsList) {

    Properties props = new Properties();
    for (int index = 0; index < propsList.length; ++index) {
      if (propsList[index] != null) {
        props.putAll(propsList[index]);
      }
    }
    return props;
  }

  public static void registerExpectedExceptions(String[] expectedExceptions) {
    SecurityTestUtil.expectedExceptions = expectedExceptions;
  }

  private static void addJavaProperties(Properties javaProps) {

    if (javaProps != null) {
      Iterator iter = javaProps.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry entry = (Map.Entry)iter.next();
        System.setProperty((String)entry.getKey(), (String)entry.getValue());
      }
    }
  }

  private static void removeJavaProperties(Properties javaProps) {

    if (javaProps != null) {
      Properties props = System.getProperties();
      Iterator iter = javaProps.keySet().iterator();
      while (iter.hasNext()) {
        props.remove(iter.next());
      }
      System.setProperties(props);
    }
  }

  public static Integer createCacheServer(Properties authProps,
      Object javaProps, Integer dsPort, String locatorString,
      Integer serverPort, Integer expectedResult) {

    return createCacheServer(authProps, javaProps, dsPort, locatorString,
        serverPort, Boolean.FALSE, expectedResult);
  }

  public static Integer createCacheServer(Properties authProps,
      Object javaProps, Integer locatorPort, String locatorString,
      Integer serverPort, Boolean setupDynamicRegionFactory,
      Integer expectedResult) {

    if (authProps == null) {
      authProps = new Properties();
    }
    authProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    if (locatorString != null && locatorString.length() > 0) {
      authProps.setProperty(DistributionConfig.LOCATORS_NAME, locatorString);
      if (locatorPort != null) {
        authProps.setProperty(DistributionConfig.START_LOCATOR_NAME,
            DistributedTestCase.getIPLiteral() + "[" + locatorPort.toString() + ']');
      }
    } else {
      authProps.setProperty("locators", "localhost["+getDUnitLocatorPort()+"]");
    }
    authProps.setProperty(DistributionConfig.SECURITY_LOG_LEVEL_NAME, "finest");
    getLogWriter().info("Set the server properties to: " + authProps);
    getLogWriter().info("Set the java properties to: " + javaProps);

    SecurityTestUtil tmpInstance = new SecurityTestUtil("temp");
    try {
      tmpInstance.createSystem(authProps, (Properties)javaProps);
      if (expectedResult.intValue() != NO_EXCEPTION) {
        fail("Expected a security exception when starting peer");
      }
    }
    catch (AuthenticationRequiredException ex) {
      if (expectedResult.intValue() == AUTHREQ_EXCEPTION) {
        getLogWriter().info("Got expected exception when starting peer: " + ex);
        return new Integer(0);
      }
      else {
        fail("Got unexpected exception when starting peer", ex);
      }
    }
    catch (AuthenticationFailedException ex) {
      if (expectedResult.intValue() == AUTHFAIL_EXCEPTION) {
        getLogWriter().info("Got expected exception when starting peer: " + ex);
        return new Integer(0);
      }
      else {
        fail("Got unexpected exception when starting peer", ex);
      }
    }
    catch (Exception ex) {
      fail("Got unexpected exception when starting peer", ex);
    }

    if (setupDynamicRegionFactory.booleanValue()) {
      initDynamicRegionFactory();
    }
    tmpInstance.openCache();
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    cache.createRegion(regionName, attrs);
    int port;
    if (serverPort == null || serverPort.intValue() <= 0) {
      port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    }
    else {
      port = serverPort.intValue();
    }
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    try {
      server1.start();
    }
    catch (Exception ex) {
      fail("Got unexpected exception when starting CacheServer", ex);
    }
    return new Integer(server1.getPort());
  }

  public static void createCacheClient(String authInitModule,
      Properties authProps, Properties javaProps, Integer[] ports,
      Object numConnections, Integer expectedResult) {
    createCacheClient(authInitModule, authProps, javaProps, ports,
        numConnections, "false", expectedResult);
  }

  public static void createCacheClient(String authInitModule,
      Properties authProps, Properties javaProps, Integer[] ports,
      Object numConnections, String multiUserMode, Integer expectedResult) {
    createCacheClient(authInitModule, authProps, javaProps, ports,
        (Integer)numConnections, Boolean.FALSE, multiUserMode, expectedResult);
  }

  public static void createCacheClient(String authInitModule,
      Properties authProps, Properties javaProps, Integer[] ports,
      Integer numConnections, Boolean setupDynamicRegionFactory,
      Integer expectedResult) {
    createCacheClient(authInitModule, authProps, javaProps, ports,
        numConnections, setupDynamicRegionFactory, "false", expectedResult);
  }

  public static void createCacheClient(String authInitModule,
      Properties authProps, Properties javaProps, Integer[] ports,
      Integer numConnections, Boolean setupDynamicRegionFactory,
      String multiUserMode, Integer expectedResult) {
    createCacheClient(authInitModule, authProps, javaProps, ports,
        numConnections, setupDynamicRegionFactory, multiUserMode, Boolean.TRUE,
        expectedResult);
  }

  public static void createCacheClient(String authInitModule,
      Properties authProps, Properties javaProps, Integer[] ports,
      Integer numConnections, Boolean setupDynamicRegionFactory,
      String multiUserMode, Boolean subscriptionEnabled,
      Integer expectedResult) {

    multiUserAuthMode = Boolean.valueOf(multiUserMode);
    if (authProps == null) {
      authProps = new Properties();
    }
    authProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    authProps.setProperty(DistributionConfig.LOCATORS_NAME, "");
    authProps.setProperty(DistributionConfig.SECURITY_LOG_LEVEL_NAME, "finest");
    // TODO (ashetkar) Add " && (!multiUserAuthMode)" below.
    if (authInitModule != null) {
      authProps.setProperty(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
          authInitModule);
    }

    SecurityTestUtil tmpInstance = new SecurityTestUtil("temp");
    tmpInstance.createSystem(authProps, javaProps);
    AttributesFactory factory = new AttributesFactory();
    int[] portsI = new int[ports.length];
    for(int z=0;z<ports.length;z++) {
      portsI[z] = ports[z].intValue();
    }
   
    try {
      PoolFactory poolFactory = PoolManager.createFactory();
      poolFactory.setRetryAttempts(200);
      if (multiUserAuthMode) {
        poolFactory.setMultiuserAuthentication(multiUserAuthMode);
        // [sumedh] Why is this false here only to be overridden in
        // ClientServerTestCase.configureConnectionPoolWithNameAndFactory below?
        // Actually setting it to false causes MultiuserAPIDUnitTest to fail.
        //poolFactory.setSubscriptionEnabled(false);
      }
      pool = ClientServerTestCase.configureConnectionPoolWithNameAndFactory(factory,
          DistributedTestCase.getIPLiteral(), portsI, subscriptionEnabled, 0,
          numConnections == null ? -1 : numConnections.intValue(), null, null,
          poolFactory);

      if (setupDynamicRegionFactory.booleanValue()) {
        initClientDynamicRegionFactory(pool.getName());
      }
      tmpInstance.openCache();
      try {
        getLogWriter().info("multi-user mode " + multiUserAuthMode);
        proxyCaches[0] = (ProxyCache)((PoolImpl) pool).createAuthenticatedCacheView(authProps);
        if (!multiUserAuthMode) {
          fail("Expected a UnsupportedOperationException but got none in single-user mode");
        }
      } catch (UnsupportedOperationException uoe) {
        if (!multiUserAuthMode) {
          getLogWriter().info("Got expected UnsupportedOperationException in single-user mode");
        }
        else {
          fail("Got unexpected exception in multi-user mode ", uoe);
        }
      }

      factory.setScope(Scope.LOCAL);
      if (multiUserAuthMode) {
        factory.setDataPolicy(DataPolicy.EMPTY);
      }
      RegionAttributes attrs = factory.create();
      cache.createRegion(regionName, attrs);

      if (expectedResult.intValue() != NO_EXCEPTION
          && expectedResult.intValue() != NOFORCE_AUTHREQ_EXCEPTION) {
        if (!multiUserAuthMode) {
          fail("Expected an exception when starting client");
        }
      }
    }
    catch (AuthenticationRequiredException ex) {
      if (expectedResult.intValue() == AUTHREQ_EXCEPTION
          || expectedResult.intValue() == NOFORCE_AUTHREQ_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when starting client: " + ex);
      }
      else {
        fail("Got unexpected exception when starting client", ex);
      }
    }
    catch (AuthenticationFailedException ex) {
      if (expectedResult.intValue() == AUTHFAIL_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when starting client: " + ex);
      }
      else {
        fail("Got unexpected exception when starting client", ex);
      }
    }
    catch (ServerRefusedConnectionException ex) {
      if (expectedResult.intValue() == CONNREFUSED_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when starting client: " + ex);
      }
      else {
        fail("Got unexpected exception when starting client", ex);
      }
    }
    catch (Exception ex) {
      fail("Got unexpected exception when starting client", ex);
    }
  }

  public static void createCacheClientForMultiUserMode(Integer numOfUsers,
      String authInitModule, Properties[] authProps, Properties javaProps,
      Integer[] ports, Integer numConnections,
      Boolean setupDynamicRegionFactory, Integer expectedResult) {
    createCacheClientForMultiUserMode(numOfUsers, authInitModule, authProps,
        javaProps, ports, numConnections, setupDynamicRegionFactory, null,
        expectedResult);
  }

  public static void createCacheClientForMultiUserMode(Integer numOfUsers,
      String authInitModule, Properties[] authProps, Properties javaProps,
      Integer[] ports, Integer numConnections,
      Boolean setupDynamicRegionFactory, String durableClientId,
      Integer expectedResult) {

    if (numOfUsers == null || numOfUsers < 1) {
      fail("Number of users cannot be less than one");
    }
    multiUserAuthMode = true;
    // Assert that number of users == length of arrays of the provided params
//    if (numOfUsers != authInitModules.length) {
//      fail("Number of authInitModules provided does not match with numOfUsers specified, "
//          + authInitModules.length);
//    }
    if (numOfUsers != authProps.length) {
      fail("Number of authProps provided does not match with numOfUsers specified, "
          + authProps.length);
    }
//    if (numOfUsers != javaProps.length) {
//      fail("Number of javaProps provided does not match with numOfUsers specified, "
//          + javaProps.length);
//    }
//    if (numOfUsers != expectedResult.length) {
//      fail("Number of expectedResult provided does not match with numOfUsers specified, "
//          + expectedResult.length);
//    }
    if (authProps[0] == null) {
      authProps[0] = new Properties();
    }
    authProps[0].setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    authProps[0].setProperty(DistributionConfig.LOCATORS_NAME, "");
    authProps[0].setProperty(DistributionConfig.SECURITY_LOG_LEVEL_NAME,
        "finest");
    Properties props = new Properties();
    if (authInitModule != null) {
      authProps[0].setProperty(
          DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME, authInitModule);
      props.setProperty(DistributionConfig.SECURITY_CLIENT_AUTH_INIT_NAME,
          authInitModule);
    }
    if (durableClientId != null) {
      props.setProperty(DistributionConfig.DURABLE_CLIENT_ID_NAME,
          durableClientId);
      props.setProperty(DistributionConfig.DURABLE_CLIENT_TIMEOUT_NAME, String
          .valueOf(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT));
    }

    SecurityTestUtil tmpInstance = new SecurityTestUtil("temp");
    tmpInstance.createSystem(props, javaProps);
    AttributesFactory factory = new AttributesFactory();
    int[] portsI = new int[ports.length];
    for(int z=0;z<ports.length;z++) {
      portsI[z] = ports[z].intValue();
    }
   
    try {
      tmpInstance.openCache();
      PoolFactory poolFactory = PoolManager.createFactory();
      poolFactory.setRetryAttempts(200);
      poolFactory.setMultiuserAuthentication(multiUserAuthMode);
      poolFactory.setSubscriptionEnabled(true);
      pool = ClientServerTestCase.configureConnectionPoolWithNameAndFactory(factory,
          DistributedTestCase.getIPLiteral(), portsI, true, 1,
          numConnections == null ? -1 : numConnections.intValue(), null, null,
          poolFactory);

      if (setupDynamicRegionFactory.booleanValue()) {
        initClientDynamicRegionFactory(pool.getName());
      }
      proxyCaches = new ProxyCache[numOfUsers];
      for (int i=0; i<numOfUsers; i++) {
        proxyCaches[i] = (ProxyCache)((PoolImpl) pool).createAuthenticatedCacheView(authProps[i]);
      }

      factory.setScope(Scope.LOCAL);
      factory.setDataPolicy(DataPolicy.EMPTY);
      RegionAttributes attrs = factory.create();
      cache.createRegion(regionName, attrs);

      if (expectedResult.intValue() != NO_EXCEPTION
          && expectedResult.intValue() != NOFORCE_AUTHREQ_EXCEPTION) {
        if (!multiUserAuthMode) {
          fail("Expected an exception when starting client");
        }
      }
    }
    catch (AuthenticationRequiredException ex) {
      if (expectedResult.intValue() == AUTHREQ_EXCEPTION
          || expectedResult.intValue() == NOFORCE_AUTHREQ_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when starting client: " + ex);
      }
      else {
        fail("Got unexpected exception when starting client", ex);
      }
    }
    catch (AuthenticationFailedException ex) {
      if (expectedResult.intValue() == AUTHFAIL_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when starting client: " + ex);
      }
      else {
        fail("Got unexpected exception when starting client", ex);
      }
    }
    catch (ServerRefusedConnectionException ex) {
      if (expectedResult.intValue() == CONNREFUSED_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when starting client: " + ex);
      }
      else {
        fail("Got unexpected exception when starting client", ex);
      }
    }
    catch (Exception ex) {
      fail("Got unexpected exception when starting client", ex);
    }
  }

  public static void createProxyCache(Integer[] userIndices, Properties[] props) {
    int j = 0;
    for (int i : userIndices) {
      SecurityTestUtil.proxyCaches[i] = (ProxyCache)((PoolImpl) SecurityTestUtil.pool)
          .createAuthenticatedCacheView(props[j]);
      j++;
    }
  }

  public static void stopCacheServers() {
    Iterator iter = getCache().getCacheServers().iterator();
    if (iter.hasNext()) {
      CacheServer server = (CacheServer)iter.next();
      server.stop();
      assertFalse(server.isRunning());
    }
  }

  public static void restartCacheServers() {
    Iterator iter = getCache().getCacheServers().iterator();
    if (iter.hasNext()) {
      CacheServer server = (CacheServer)iter.next();
      try {
        server.start();
      }
      catch (Exception ex) {
        fail("Unexpected exception when restarting cache servers", ex);
      }
      assertTrue(server.isRunning());
    }
  }

  public static void startLocator(String name, Integer port, Object extraProps,
      Object javaProps, String[] expectedExceptions) {

    File logFile = new File(name + "-locator" + port.intValue() + ".log");
    try {
      Properties authProps = new Properties();
      if (extraProps != null) {
        authProps.putAll((Properties)extraProps);
      }
      authProps.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
      authProps.setProperty(DistributionConfig.LOCATORS_NAME, 
                            DistributedTestCase.getIPLiteral() + "[" + port + "]");
      authProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
      clearStaticSSLContext();
      setJavaProps((Properties)javaProps);
      FileOutputStream logOut = new FileOutputStream(logFile);
      PrintStream logStream = new PrintStream(logOut);
      LogWriter logger = new PureLogWriter(InternalLogWriter.CONFIG_LEVEL,
          logStream);
      addExpectedExceptions(expectedExceptions, logger);
      logStream.flush();
      locator = Locator.startLocatorAndDS(port.intValue(), logFile, null,
          authProps);
    }
    catch (IOException ex) {
      fail("While starting locator on port " + port.intValue(), ex);
    }
  }

  public static void stopLocator(Integer port, String[] expectedExceptions) {

    try {
      locator.stop();
      removeExpectedExceptions(expectedExceptions, locator
          .getDistributedSystem().getLogWriter());
    }
    catch (Exception ex) {
      fail("While stopping locator on port " + port.intValue(), ex);
    }
  }

  public static Cache getCache() {
    return cache;
  }

  // Some useful region methods used by security tests

  public static void waitForCondition(Callable cond) {
    waitForCondition(cond, 100, 120);
  }

  public static void waitForCondition(final Callable cond, int sleepMillis,
      int numTries) {
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          return ((Boolean)cond.call()).booleanValue();
        }
        catch (Exception e) {
          fail("Unexpected exception", e);
        }
        return false; // NOTREACHED
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, sleepMillis * numTries, 200, true);
  }

  public static Object getLocalValue(Region region, Object key) {

    Region.Entry entry = region.getEntry(key);
    return (entry != null ? entry.getValue() : null);
  }

  public static void doProxyCacheClose() {
    for (int i=0; i<proxyCaches.length; i++) {
      proxyCaches[i].close();
    }
  }

  private static void doPutsP(Integer num, Integer expectedResult,
      boolean newVals) {
    doPutsP(num, Integer.valueOf(0), expectedResult, newVals);
  }

  private static void doPutsP(Integer num, Integer multiUserIndex,
      Integer expectedResult, boolean newVals) {

    assertTrue(num.intValue() <= keys.length);
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(regionName);
        regionRef = region;
      }
      else {
        region = getCache().getRegion(regionName);
      }
      assertNotNull(region);
    }
    catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing puts: " + ex);
      }
      else {
        fail("Got unexpected exception when doing puts", ex);
      }
    }
    for (int index = 0; index < num.intValue(); ++index) {
      try {
        if (newVals) {
          region.put(keys[index], nvalues[index]);
        }
        else {
          region.put(keys[index], values[index]);
        }
        if (expectedResult.intValue() != NO_EXCEPTION) {
          fail("Expected a NotAuthorizedException while doing puts");
        }
      }
      catch(NoAvailableServersException ex) {
        if(expectedResult.intValue() == SecurityTestUtil.NO_AVAILABLE_SERVERS) {
          getLogWriter().info(
              "Got expected NoAvailableServers when doing puts: "
              + ex.getCause());
          continue;
        }
        else {
          fail("Got unexpected exception when doing puts", ex);
        }
      }
      catch (ServerConnectivityException ex) {
        if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          getLogWriter().info(
              "Got expected NotAuthorizedException when doing puts: "
                  + ex.getCause());
          continue;
        }
        if ((expectedResult.intValue() == AUTHREQ_EXCEPTION)
            && (ex.getCause() instanceof AuthenticationRequiredException)) {
          getLogWriter().info(
              "Got expected AuthenticationRequiredException when doing puts: "
                  + ex.getCause());
          continue;
        }
        if ((expectedResult.intValue() == AUTHFAIL_EXCEPTION)
            && (ex.getCause() instanceof AuthenticationFailedException)) {
          getLogWriter().info(
              "Got expected AuthenticationFailedException when doing puts: "
                  + ex.getCause());
          continue;
        }
        else if (expectedResult.intValue() == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing puts: " + ex);
        }
        else {
          fail("Got unexpected exception when doing puts", ex);
        }
      }
      catch (Exception ex) {
        if (expectedResult.intValue() == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing puts: " + ex);
        }
        else {
          fail("Got unexpected exception when doing puts", ex);
        }
      }
    }
  }

  private static void doGetAllP(Integer multiUserIndex,
      Integer expectedResult, boolean useTX) {
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(regionName);
      }
      else {
        region = getCache().getRegion(regionName);
      }
      assertNotNull(region);
    }
    catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing getAll: " + ex);
      }
      else {
        fail("Got unexpected exception when doing getAll", ex);
      }
    }
    try {
      List keys = new ArrayList();
      keys.add("key1");
      keys.add("key2");
      if (useTX) {
        getCache().getCacheTransactionManager().begin();
      }
      Map entries = region.getAll(keys);
      // Also check getEntry()
      region.getEntry("key1");
      if (useTX) {
        getCache().getCacheTransactionManager().commit();
      }
      assertNotNull(entries);
      if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)) {
        assertEquals(0, entries.size());
      } else if ((expectedResult.intValue() == NO_EXCEPTION)) {
        assertEquals(2, entries.size());
        assertEquals("value1", entries.get("key1"));
        assertEquals("value2", entries.get("key2"));
      }
    } catch (NoAvailableServersException ex) {
      if (expectedResult.intValue() == SecurityTestUtil.NO_AVAILABLE_SERVERS) {
        getLogWriter().info(
            "Got expected NoAvailableServers when doing getAll: "
                + ex.getCause());
      } else {
        fail("Got unexpected exception when doing getAll", ex);
      }
    } catch (ServerConnectivityException ex) {
      if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)
          && (ex.getCause() instanceof NotAuthorizedException)) {
        getLogWriter().info(
            "Got expected NotAuthorizedException when doing getAll: "
                + ex.getCause());
      } else if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing getAll: " + ex);
      } else {
        fail("Got unexpected exception when doing getAll", ex);
      }
    } catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing getAll: " + ex);
      } else {
        fail("Got unexpected exception when doing getAll", ex);
      }
    }
  }

  private static void doGetsP(Integer num, Integer expectedResult,
      boolean newVals) {
    doGetsP(num, Integer.valueOf(0), expectedResult, newVals);
  }

  private static void doGetsP(Integer num, Integer multiUserIndex,
      Integer expectedResult, boolean newVals) {

    assertTrue(num.intValue() <= keys.length);
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(regionName);
      }
      else {
        region = getCache().getRegion(regionName);
      }
      assertNotNull(region);
    }
    catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing gets: " + ex);
      }
      else {
        fail("Got unexpected exception when doing gets", ex);
      }
    }
    for (int index = 0; index < num.intValue(); ++index) {
      Object value = null;
      try {
        try {
          region.localInvalidate(keys[index]);
        }
        catch (Exception ex) {
        }
        value = region.get(keys[index]);
        if (expectedResult.intValue() != NO_EXCEPTION) {
          fail("Expected a NotAuthorizedException while doing gets");
        }
      }
      catch(NoAvailableServersException ex) {
        if(expectedResult.intValue() == SecurityTestUtil.NO_AVAILABLE_SERVERS) {
          getLogWriter().info(
              "Got expected NoAvailableServers when doing gets: "
              + ex.getCause());
          continue;
        }
        else {
          fail("Got unexpected exception when doing gets", ex);
        }
      }
      catch (ServerConnectivityException ex) {
        if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          getLogWriter().info(
              "Got expected NotAuthorizedException when doing gets: "
                  + ex.getCause());
          continue;
        }
        else if (expectedResult.intValue() == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing gets: " + ex);
        }
        else {
          fail("Got unexpected exception when doing gets", ex);
        }
      }
      catch (Exception ex) {
        if (expectedResult.intValue() == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing gets: " + ex);
        }
        else {
          fail("Got unexpected exception when doing gets", ex);
        }
      }
      assertNotNull(value);
      if (newVals) {
        assertEquals(nvalues[index], value);
      }
      else {
        assertEquals(values[index], value);
      }
    }
  }

  private static void doLocalGetsP(int num, boolean checkNVals) {

    assertTrue(num <= keys.length);
    String[] vals = values;
    if (checkNVals) {
      vals = nvalues;
    }
    final Region region = getCache().getRegion(regionName);
    assertNotNull(region);
    for (int index = 0; index < num; ++index) {
      final String key = keys[index];
      final String expectedVal = vals[index];
      waitForCondition(new Callable() {
        public Object call() throws Exception {
          Object value = getLocalValue(region, key);
          return Boolean.valueOf(expectedVal.equals(value));
        }
      }, 1000, 30 / num);
    }
    for (int index = 0; index < num; ++index) {
      Region.Entry entry = region.getEntry(keys[index]);
      assertNotNull(entry);
      assertEquals(vals[index], entry.getValue());
    }
  }

  private static void doRegionDestroysP(Integer multiuserIndex,
      Integer expectedResult) {
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiuserIndex].getRegion(regionName);
      } else {
        region = getCache().getRegion(regionName);
      }
      assertNotNull(region);
    } catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when doing region destroy: " + ex);
      } else {
        fail("Got unexpected exception when doing region destroy", ex);
      }
    }

    try {
      region.destroyRegion();
      if (expectedResult.intValue() != NO_EXCEPTION) {
        fail("Expected a NotAuthorizedException while doing region destroy");
      }
      if (multiUserAuthMode) {
        region = proxyCaches[multiuserIndex].getRegion(regionName);
      } else {
        region = getCache().getRegion(regionName);
      }
      assertNull(region);
    } catch (NoAvailableServersException ex) {
      if (expectedResult.intValue() == SecurityTestUtil.NO_AVAILABLE_SERVERS) {
        getLogWriter().info(
            "Got expected NoAvailableServers when doing region destroy: "
                + ex.getCause());
      } else {
        fail("Got unexpected exception when doing region destroy", ex);
      }
    } catch (ServerConnectivityException ex) {
      if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)
          && (ex.getCause() instanceof NotAuthorizedException)) {
        getLogWriter().info(
            "Got expected NotAuthorizedException when doing region destroy: "
                + ex.getCause());
      } else if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when doing region destroy: " + ex);
      } else {
        fail("Got unexpected exception when doing region destroy", ex);
      }
    } catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when doing region destroy: " + ex);
      } else {
        fail("Got unexpected exception when doing region destroy", ex);
      }
    }
  }
  
  private static void doDestroysP(Integer num, Integer multiUserIndex,
      Integer expectedResult, boolean newVals) {

    assertTrue(num.intValue() <= keys.length);
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(regionName);
      }
      else {
        region = getCache().getRegion(regionName);
      }
      assertNotNull(region);
    }
    catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing destroys: " + ex);
      }
      else {
        fail("Got unexpected exception when doing destroys", ex);
      }
    }
    for (int index = 0; index < num.intValue(); ++index) {
      try {
        region.destroy(keys[index]);
        if (expectedResult.intValue() != NO_EXCEPTION) {
          fail("Expected a NotAuthorizedException while doing destroys");
        }
      }
      catch(NoAvailableServersException ex) {
        if(expectedResult.intValue() == SecurityTestUtil.NO_AVAILABLE_SERVERS) {
          getLogWriter().info(
              "Got expected NoAvailableServers when doing destroys: "
              + ex.getCause());
          continue;
        }
        else {
          fail("Got unexpected exception when doing destroys", ex);
        }
      }
      catch (ServerConnectivityException ex) {
        if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          getLogWriter().info(
              "Got expected NotAuthorizedException when doing destroys: "
                  + ex.getCause());
          continue;
        }
        else if (expectedResult.intValue() == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing destroys: " + ex);
        }
        else {
          fail("Got unexpected exception when doing destroys", ex);
        }
      }
      catch (Exception ex) {
        if (expectedResult.intValue() == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing destroys: " + ex);
        }
        else {
          fail("Got unexpected exception when doing destroys", ex);
        }
      }
    }
  }

  private static void doInvalidatesP(Integer num, Integer multiUserIndex,
      Integer expectedResult, boolean newVals) {

    assertTrue(num.intValue() <= keys.length);
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(regionName);
      }
      else {
        region = getCache().getRegion(regionName);
      }
      assertNotNull(region);
    }
    catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing invalidates: " + ex);
      }
      else {
        fail("Got unexpected exception when doing invalidates", ex);
      }
    }
    for (int index = 0; index < num.intValue(); ++index) {
      try {
        region.invalidate(keys[index]);
        if (expectedResult.intValue() != NO_EXCEPTION) {
          fail("Expected a NotAuthorizedException while doing invalidates");
        }
      }
      catch(NoAvailableServersException ex) {
        if(expectedResult.intValue() == SecurityTestUtil.NO_AVAILABLE_SERVERS) {
          getLogWriter().info(
              "Got expected NoAvailableServers when doing invalidates: "
              + ex.getCause());
          continue;
        }
        else {
          fail("Got unexpected exception when doing invalidates", ex);
        }
      }
      catch (ServerConnectivityException ex) {
        if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          getLogWriter().info(
              "Got expected NotAuthorizedException when doing invalidates: "
                  + ex.getCause());
          continue;
        }
        else if (expectedResult.intValue() == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing invalidates: " + ex);
        }
        else {
          fail("Got unexpected exception when doing invalidates", ex);
        }
      }
      catch (Exception ex) {
        if (expectedResult.intValue() == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing invalidates: " + ex);
        }
        else {
          fail("Got unexpected exception when doing invalidates", ex);
        }
      }
    }
  }

  private static void doContainsKeysP(Integer num, Integer multiUserIndex,
      Integer expectedResult, boolean newVals, boolean expectedValue) {

    assertTrue(num.intValue() <= keys.length);
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(regionName);
      }
      else {
        region = getCache().getRegion(regionName);
      }
      assertNotNull(region);
    }
    catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing containsKey: " + ex);
      }
      else {
        fail("Got unexpected exception when doing containsKey", ex);
      }
    }
    for (int index = 0; index < num.intValue(); ++index) {
      boolean result = false;
      try {
        result = region.containsKeyOnServer(keys[index]);
        if (expectedResult.intValue() != NO_EXCEPTION) {
          fail("Expected a NotAuthorizedException while doing containsKey");
        }
      }
      catch(NoAvailableServersException ex) {
        if(expectedResult.intValue() == SecurityTestUtil.NO_AVAILABLE_SERVERS) {
          getLogWriter().info(
              "Got expected NoAvailableServers when doing containsKey: "
              + ex.getCause());
          continue;
        }
        else {
          fail("Got unexpected exception when doing containsKey", ex);
        }
      }
      catch (ServerConnectivityException ex) {
        if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          getLogWriter().info(
              "Got expected NotAuthorizedException when doing containsKey: "
                  + ex.getCause());
          continue;
        }
        else if (expectedResult.intValue() == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing containsKey: " + ex);
        }
        else {
          fail("Got unexpected exception when doing containsKey", ex);
        }
      }
      catch (Exception ex) {
        if (expectedResult.intValue() == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing containsKey: " + ex);
        }
        else {
          fail("Got unexpected exception when doing containsKey", ex);
        }
      }
      assertEquals(expectedValue, result);
    }
  }

  private static void doQueriesP(Integer multiUserIndex,
      Integer expectedResult, Integer expectedValue) {
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(regionName);
      } else {
        region = getCache().getRegion(regionName);
      }
      assertNotNull(region);
    } catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing queries: " + ex);
      } else {
        fail("Got unexpected exception when doing queries", ex);
      }
    }
    String queryStr = "SELECT DISTINCT * FROM " + region.getFullPath();
    try {
      SelectResults queryResults = region.query(queryStr);
      Set resultSet = queryResults.asSet();
      assertEquals(expectedValue.intValue(), resultSet.size());
      if (expectedResult.intValue() != NO_EXCEPTION) {
        fail("Expected a NotAuthorizedException while doing queries");
      }
    } catch (NoAvailableServersException ex) {
      if (expectedResult.intValue() == SecurityTestUtil.NO_AVAILABLE_SERVERS) {
        getLogWriter().info(
            "Got expected NoAvailableServers when doing queries: "
                + ex.getCause());
      } else {
        fail("Got unexpected exception when doing queries", ex);
      }
    } catch (ServerConnectivityException ex) {
      if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)
          && (ex.getCause() instanceof NotAuthorizedException)) {
        getLogWriter().info(
            "Got expected NotAuthorizedException when doing queries: "
                + ex.getCause());
      } else if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing queries: " + ex);
      } else {
        fail("Got unexpected exception when doing queries", ex);
      }
    } catch (QueryInvocationTargetException qite) {
      if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)
          && (qite.getCause() instanceof NotAuthorizedException)) {
        getLogWriter().info(
            "Got expected NotAuthorizedException when doing queries: "
                + qite.getCause());
      } else if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing queries: " + qite);
      } else {
        fail("Got unexpected exception when doing queries", qite);
      }
    } catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing queries: " + ex);
      } else {
        fail("Got unexpected exception when doing queries", ex);
      }
    }
  }

  private static void doFunctionExecuteP(Integer multiUserIndex,
      Function function, Integer expectedResult, Object expectedValue,
      String method) {
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(regionName);
      } else {
        region = getCache().getRegion(regionName);
      }
      assertNotNull(region);
    } catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when executing function: " + ex);
      } else {
        fail("Got unexpected exception when executing function", ex);
      }
    }
    try {
      FunctionService.registerFunction(function);
      Execution execution = null;
      if ("region".equals(method)) {
        execution = FunctionService.onRegion(region);
      } else if ("server".equals(method)) {
        if (multiUserAuthMode) {
          execution = FunctionService.onServer(proxyCaches[multiUserIndex]);
        } else {
          execution = FunctionService.onServer(pool);
        }
      } else { // if ("servers".equals(method)) {
        if (multiUserAuthMode) {
          execution = FunctionService.onServers(proxyCaches[multiUserIndex]);
        } else {
          execution = FunctionService.onServers(pool);
        }
      }
      execution.execute(function.getId());
      if (expectedResult.intValue() != NO_EXCEPTION) {
        fail("Expected a NotAuthorizedException while executing function");
      }
    } catch (NoAvailableServersException ex) {
      if (expectedResult.intValue() == SecurityTestUtil.NO_AVAILABLE_SERVERS) {
        getLogWriter().info(
            "Got expected NoAvailableServers when executing function: "
                + ex.getCause());
      } else {
        fail("Got unexpected exception when executing function", ex);
      }
    } catch (ServerConnectivityException ex) {
      if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)
          && (ex.getCause() instanceof NotAuthorizedException)) {
        getLogWriter().info(
            "Got expected NotAuthorizedException when executing function: "
                + ex.getCause());
      } else if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when executing function: " + ex);
      } else {
        fail("Got unexpected exception when executing function", ex);
      }
    } catch (FunctionException ex) {
      if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)
          && ((ex.getCause() instanceof NotAuthorizedException) || ((ex
              .getCause() instanceof ServerOperationException) && (((ServerOperationException)ex
              .getCause()).getCause() instanceof NotAuthorizedException)))) {
        getLogWriter().info(
            "Got expected NotAuthorizedException when executing function: "
                + ex.getCause());
      } else if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when executing function: " + ex);
      } else {
        fail("Got unexpected exception when executing function", ex);
      }
    } catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when executing function: " + ex);
      } else {
        fail("Got unexpected exception when executing function", ex);
      }
    }
  }

  private static void doQueryExecuteP(Integer multiUserIndex,
      Integer expectedResult, Integer expectedValue) {
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(regionName);
      } else {
        region = getCache().getRegion(regionName);
      }
      assertNotNull(region);
    } catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when executing query: " + ex);
      } else {
        fail("Got unexpected exception when executing query", ex);
      }
    }
    try {
      String queryString = "SELECT DISTINCT * FROM " + region.getFullPath();
      Query query = null;
      if (multiUserAuthMode) {
        query = proxyCaches[multiUserIndex].getQueryService().newQuery(queryString);
      }
      else {
        region.getCache().getQueryService().newQuery(queryString);
      }
      SelectResults result = (SelectResults)query.execute();
      if (expectedResult.intValue() != NO_EXCEPTION) {
        fail("Expected a NotAuthorizedException while executing function");
      }
      assertEquals(expectedValue.intValue(), result.asList().size());
    } catch (NoAvailableServersException ex) {
      if (expectedResult.intValue() == SecurityTestUtil.NO_AVAILABLE_SERVERS) {
        getLogWriter().info(
            "Got expected NoAvailableServers when executing query: "
                + ex.getCause());
      } else {
        fail("Got unexpected exception when executing query", ex);
      }
    } catch (ServerConnectivityException ex) {
      if ((expectedResult.intValue() == NOTAUTHZ_EXCEPTION)
          && (ex.getCause() instanceof NotAuthorizedException)) {
        getLogWriter().info(
            "Got expected NotAuthorizedException when executing query: "
                + ex.getCause());
      } else if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when executing query: " + ex);
      } else {
        fail("Got unexpected exception when executing query", ex);
      }
    } catch (Exception ex) {
      if (expectedResult.intValue() == OTHER_EXCEPTION) {
        getLogWriter().info(
            "Got expected exception when executing query: " + ex);
      } else {
        fail("Got unexpected exception when executing query", ex);
      }
    }
  }

  public static void doPuts(Integer num) {

    doPutsP(num, new Integer(NO_EXCEPTION), false);
  }

  public static void doPuts(Integer num, Integer expectedResult) {

    doPutsP(num, expectedResult, false);
  }

  public static void doMultiUserPuts(Integer num, Integer numOfUsers,
      Integer[] expectedResults) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtil.doMultiUserPuts(): numOfUsers = " + numOfUsers
          + ", but expected results " + expectedResults.length);
    }
    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("PUT: MultiUser# " + i);
      doPutsP(num, Integer.valueOf(i), expectedResults[i], false);
    }
  }

  public static void doGets(Integer num) {

    doGetsP(num, new Integer(NO_EXCEPTION), false);
  }

  public static void doGets(Integer num, Integer expectedResult) {

    doGetsP(num, expectedResult, false);
  }

  public static void doMultiUserGetAll(Integer numOfUsers, Integer[] expectedResults) {
    doMultiUserGetAll(numOfUsers, expectedResults, false);
  }

  public static void doMultiUserGetAll(Integer numOfUsers,
      Integer[] expectedResults, boolean useTX) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtil.doMultiUserGetAll(): numOfUsers = " + numOfUsers
          + ", but expected results " + expectedResults.length);
    }
    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info(
          "GET_ALL" + (useTX ? " in TX" : "") + ": MultiUser# " + i);
      doGetAllP(Integer.valueOf(i), expectedResults[i], useTX);
    }
  }
  
  public static void doMultiUserGets(Integer num, Integer numOfUsers,
      Integer[] expectedResults) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtil.doMultiUserGets(): numOfUsers = " + numOfUsers
          + ", but expected results " + expectedResults.length);
    }
    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("GET: MultiUser# " + i);
      doGetsP(num, Integer.valueOf(i), expectedResults[i], false);
    }
  }

  public static void doMultiUserRegionDestroys(Integer numOfUsers,
      Integer[] expectedResults) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtil.doMultiUserRegionDestroys(): numOfUsers = " + numOfUsers
          + ", but expected results " + expectedResults.length);
    }
    for (int i = numOfUsers-1; i >= 0; i--) {
      getLogWriter().info("DESTROY: MultiUser# " + i);
      doRegionDestroysP(Integer.valueOf(i), expectedResults[i]);
    }
  }

  public static void doMultiUserDestroys(Integer num, Integer numOfUsers,
      Integer[] expectedResults) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtil.doMultiUserDestroys(): numOfUsers = " + numOfUsers
          + ", but expected results " + expectedResults.length);
    }
    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("DESTROY: MultiUser# " + i);
      doDestroysP(num, Integer.valueOf(i), expectedResults[i], false);
    }
  }

  public static void doMultiUserInvalidates(Integer num, Integer numOfUsers,
      Integer[] expectedResults) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtil.doMultiUserInvalidates(): numOfUsers = " + numOfUsers
          + ", but expected results " + expectedResults.length);
    }
    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("INVALIDATE: MultiUser# " + i);
      doInvalidatesP(num, Integer.valueOf(i), expectedResults[i], false);
    }
  }

  public static void doMultiUserContainsKeys(Integer num, Integer numOfUsers,
      Integer[] expectedResults, Boolean[] results) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtil.doMultiUserContainsKeys(): numOfUsers = " + numOfUsers
          + ", but #expected results " + expectedResults.length);
    }
    if (numOfUsers != results.length) {
      fail("SecurityTestUtil.doMultiUserContainsKeys(): numOfUsers = " + numOfUsers
          + ", but #expected output " + results.length);
    }
    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("CONTAINS_KEY: MultiUser# " + i);
      doContainsKeysP(num, Integer.valueOf(i), expectedResults[i], false, results[i]);
    }
  }

  public static void doMultiUserQueries(Integer numOfUsers,
      Integer[] expectedResults, Integer valueSize) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtil.doMultiUserQueries(): numOfUsers = " + numOfUsers
          + ", but #expected results " + expectedResults.length);
    }
    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("QUERY: MultiUser# " + i);
      doQueriesP(Integer.valueOf(i), expectedResults[i], valueSize);
    }
  }

  public static void doMultiUserFE(Integer numOfUsers, Function function,
      Integer[] expectedResults, Object[] results, Boolean isFailoverCase) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtil.doMultiUserFE(): numOfUsers = " + numOfUsers
          + ", but #expected results " + expectedResults.length);
    }
    if (numOfUsers != results.length) {
      fail("SecurityTestUtil.doMultiUserFE(): numOfUsers = " + numOfUsers
          + ", but #expected output " + results.length);
    }
    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("FunctionExecute:onRegion MultiUser# " + i);
      doFunctionExecuteP(Integer.valueOf(i), function, expectedResults[i], results[i], "region");
    }
    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("FunctionExecute:onServer MultiUser# " + i);
      doFunctionExecuteP(Integer.valueOf(i), function, expectedResults[i], results[i], "server");
    }
    if (!isFailoverCase) {
      for (int i = 0; i < numOfUsers; i++) {
        getLogWriter().info("FunctionExecute:onServers MultiUser# " + i);
        doFunctionExecuteP(Integer.valueOf(i), function, expectedResults[i],
            results[i], "servers");
      }
    }
  }

  public static void doMultiUserQueryExecute(Integer numOfUsers,
      Integer[] expectedResults, Integer result) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtil.doMultiUserFE(): numOfUsers = " + numOfUsers
          + ", but #expected results " + expectedResults.length);
    }
    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("QueryExecute: MultiUser# " + i);
      doQueryExecuteP(Integer.valueOf(i), expectedResults[i], result);
    }
  }

  public static void doLocalGets(Integer num) {

    doLocalGetsP(num.intValue(), false);
  }

  public static void doNPuts(Integer num) {

    doPutsP(num, new Integer(NO_EXCEPTION), true);
  }

  public static void doNPuts(Integer num, Integer expectedResult) {

    doPutsP(num, expectedResult, true);
  }

  public static void doNGets(Integer num) {

    doGetsP(num, new Integer(NO_EXCEPTION), true);
  }

  public static void doNGets(Integer num, Integer expectedResult) {

    doGetsP(num, expectedResult, true);
  }

  public static void doNLocalGets(Integer num) {

    doLocalGetsP(num.intValue(), true);
  }

  public static void doSimpleGet(String expectedResult) {
    if (regionRef != null) {
      try {
        regionRef.get("KEY");
        if (expectedResult != null && expectedResult.endsWith("Exception")) {
          fail("Expected " + expectedResult + " but found none in doSimpleGet()");
        }
      } catch (Exception e) {
        if (!e.getClass().getSimpleName().endsWith(expectedResult)) {
          fail("Expected " + expectedResult + " but found "
              + e.getClass().getSimpleName() + " in doSimpleGet()");
        } else {
          getLogWriter().fine(
              "Got expected " + e.getClass().getSimpleName()
                  + " in doSimpleGet()");
        }
      }
    }
  }

  public static void doSimplePut(String expectedResult) {
    if (regionRef != null) {
      try {
        regionRef.put("KEY", "VALUE");
        if (expectedResult != null && expectedResult.endsWith("Exception")) {
          fail("Expected " + expectedResult + " but found none in doSimplePut()");
        }
      } catch (Exception e) {
        if (!e.getClass().getSimpleName().endsWith(expectedResult)) {
          fail("Expected " + expectedResult + " but found "
              + e.getClass().getSimpleName() + " in doSimplePut()", e);
        } else {
          getLogWriter().fine(
              "Got expected " + e.getClass().getSimpleName()
                  + " in doSimplePut()");
        }
      }
    }
  }
  // Deal with javax SSL properties

  private static void makeNullStaticField(Class cls) {

    Field[] fields = cls.getDeclaredFields();
    for (int index = 0; index < fields.length; ++index) {
      Field field = fields[index];
      try {
        if (Modifier.isStatic(field.getModifiers())) {
          field.setAccessible(true);
          if (field.getClass().equals(boolean.class)) {
            field.setBoolean(null, false);
            assertFalse(field.getBoolean(null));
          }
          else if (cls.isInstance(field.get(null))) {
            field.set(null, null);
            assertNull(field.get(null));
          }
        }
      }
      catch (IllegalAccessException ex) {
        getLogWriter()
            .warning("Exception while clearing static SSL field.", ex);
      }
      catch (ClassCastException ex) {
        getLogWriter()
            .warning("Exception while clearing static SSL field.", ex);
      }
    }
  }

  private static void makeNullSSLFields(Object obj, Map fieldMap) {

    Iterator fieldIter = fieldMap.entrySet().iterator();
    while (fieldIter.hasNext()) {
      Map.Entry entry = (Map.Entry)fieldIter.next();
      Field field = (Field)entry.getKey();
      Object fieldObj = entry.getValue();
      try {
        field.setAccessible(true);
        makeNullStaticField(fieldObj.getClass());
        field.set(obj, null);
        assertNull(field.get(obj));
      }
      catch (IllegalAccessException ex) {
        getLogWriter().warning("Exception while clearing SSL fields.", ex);
      }
    }
  }

  private static HashMap getSSLFields(Object obj, Class[] classes) {

    HashMap resultFields = new HashMap();
    Field[] fields = obj.getClass().getDeclaredFields();
    for (int index = 0; index < fields.length; ++index) {
      Field field = fields[index];
      try {
        field.setAccessible(true);
        Object fieldObj = field.get(obj);
        boolean isInstance = false;
        for (int classIndex = 0; classIndex < classes.length; ++classIndex) {
          if ((isInstance = classes[classIndex].isInstance(fieldObj)) == true) {
            break;
          }
        }
        if (isInstance) {
          resultFields.put(field, fieldObj);
        }
      }
      catch (IllegalAccessException ex) {
        getLogWriter().warning("Exception while getting SSL fields.", ex);
      }
    }
    return resultFields;
  }

  // This is a hack using reflection to clear the static objects in JSSE since
  // otherwise changing the javax.* store related properties has no effect
  // during the course of running dunit suite unless the VMs are restarted.
  public static void clearStaticSSLContext() {

    ServerSocketFactory defaultServerFact = SSLServerSocketFactory.getDefault();
    // Get the class of this and use reflection to blank out any static
    // SSLContext objects inside
    Map contextMap = getSSLFields(defaultServerFact, new Class[] {
        SSLContext.class, SSLContextSpi.class });
    makeNullSSLFields(defaultServerFact, contextMap);
    Iterator contextObjsIter = contextMap.values().iterator();
    while (contextObjsIter.hasNext()) {
      Object contextObj = contextObjsIter.next();
      Map contextObjsMap = getSSLFields(contextObj, new Class[] {
          TrustManager.class, KeyManager.class, TrustManager[].class,
          KeyManager[].class });
      makeNullSSLFields(contextObj, contextObjsMap);
    }
    makeNullStaticField(SSLServerSocketFactory.class);

    // Do the same for normal SSL socket factory
    SocketFactory defaultFact = SSLSocketFactory.getDefault();
    contextMap = getSSLFields(defaultFact, new Class[] { SSLContext.class,
        SSLContextSpi.class });
    makeNullSSLFields(defaultFact, contextMap);
    contextObjsIter = contextMap.values().iterator();
    while (contextObjsIter.hasNext()) {
      Object contextObj = contextObjsIter.next();
      Map contextObjsMap = getSSLFields(contextObj, new Class[] {
          TrustManager.class, KeyManager.class, TrustManager[].class,
          KeyManager[].class });
      makeNullSSLFields(contextObj, contextObjsMap);
    }
    makeNullStaticField(SSLSocketFactory.class);
    makeNullStaticField(SSLContext.class);
  }

  private static LogWriter getLogger() {

    LogWriter logger = null;
    DistributedSystem dsys = system;
    if (dsys == null || !dsys.isConnected()) {
      while ((dsys = InternalDistributedSystem.getAnyInstance()) != null
          && !dsys.isConnected()) {
      }
    }
    if (dsys != null && dsys.isConnected()) {
      logger = dsys.getLogWriter();
    }
    return logger;
  }

  public static void closeCache() {

    LogWriter logger = getLogger();
    if (logger != null) {
      removeExpectedExceptions(SecurityTestUtil.expectedExceptions, logger);
    }
    if (cache != null && !cache.isClosed()) {
      DistributedSystem sys = cache.getDistributedSystem();
      cache.close();
      sys.disconnect();
      cache = null;
    }
    disconnectFromDS();
  }

  public static void closeCache(Boolean keepAlive) {
    LogWriter logger = getLogger();
    if (logger != null) {
      removeExpectedExceptions(SecurityTestUtil.expectedExceptions, logger);
    }
    if (cache != null && !cache.isClosed()) {
      DistributedSystem sys = cache.getDistributedSystem();
      cache.close(keepAlive);
      sys.disconnect();
      cache = null;
    }
    disconnectFromDS();
  }

}
