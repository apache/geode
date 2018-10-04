/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.security;

import static org.apache.geode.cache30.ClientServerTestCase.configureConnectionPoolWithNameAndFactory;
import static org.apache.geode.cache30.ClientServerTestCase.disconnectFromDS;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_ID;
import static org.apache.geode.distributed.ConfigurationProperties.DURABLE_CLIENT_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTHENTICATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_CLIENT_AUTH_INIT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_LEVEL;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.Assert.assertEquals;
import static org.apache.geode.test.dunit.Assert.assertFalse;
import static org.apache.geode.test.dunit.Assert.assertNotNull;
import static org.apache.geode.test.dunit.Assert.assertNull;
import static org.apache.geode.test.dunit.Assert.assertTrue;
import static org.apache.geode.test.dunit.Assert.fail;
import static org.apache.geode.test.dunit.DistributedTestUtils.getDUnitLocatorPort;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.NetworkUtils.getIPLiteral;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
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

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.DynamicRegionFactory;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolFactory;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.ServerRefusedConnectionException;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.cache.client.internal.ProxyCache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.SelectResults;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.Version;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.security.templates.UsernamePrincipal;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;

/**
 * Contains utility methods for setting up servers/clients for authentication and authorization
 * tests.
 *
 * @since GemFire 5.5
 *
 * @deprecated in favor of just writing a test without this class
 */
@Deprecated
public class SecurityTestUtils {

  private final JUnit4DistributedTestCase distributedTestCase = new JUnit4DistributedTestCase() {};

  protected static final int NO_EXCEPTION = 0;
  protected static final int AUTHREQ_EXCEPTION = 1;
  protected static final int AUTHFAIL_EXCEPTION = 2;
  protected static final int CONNREFUSED_EXCEPTION = 3;
  protected static final int NOTAUTHZ_EXCEPTION = 4;
  protected static final int OTHER_EXCEPTION = 5;
  protected static final int NO_AVAILABLE_SERVERS = 6;
  protected static final int SECURITY_EXCEPTION = 7;
  // Indicates that AuthReqException may not necessarily be thrown
  protected static final int NOFORCE_AUTHREQ_EXCEPTION = 16;

  protected static final String REGION_NAME = "AuthRegion";
  protected static final String[] KEYS =
      {"key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8"};
  protected static final String[] VALUES =
      {"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8"};
  protected static final String[] NVALUES =
      {"nvalue1", "nvalue2", "nvalue3", "nvalue4", "nvalue5", "nvalue6", "nvalue7", "nvalue8"};

  private static final int NUMBER_OF_USERS = 1;

  private static String[] ignoredExceptions = null;

  private static Locator locator = null;
  private static Cache cache = null;
  private static Properties currentJavaProps = null;

  private static Pool pool = null;
  private static boolean multiUserAuthMode = false;

  private static ProxyCache[] proxyCaches = new ProxyCache[NUMBER_OF_USERS];

  private static Region regionRef = null;

  /**
   * @deprecated Please use {@link org.apache.geode.test.dunit.IgnoredException} instead
   */
  private static void addIgnoredExceptions(final String[] expectedExceptions) { // TODO: delete
    if (expectedExceptions != null) {
      for (int index = 0; index < expectedExceptions.length; index++) {
        getLogWriter().info(
            "<ExpectedException action=add>" + expectedExceptions[index] + "</ExpectedException>");
      }
    }
  }

  /**
   * @deprecated Please use {@link org.apache.geode.test.dunit.IgnoredException} instead
   */
  private static void removeExpectedExceptions(final String[] expectedExceptions) { // TODO: delete
    if (expectedExceptions != null) {
      for (int index = 0; index < expectedExceptions.length; index++) {
        getLogWriter().info("<ExpectedException action=remove>" + expectedExceptions[index]
            + "</ExpectedException>");
      }
    }
  }

  protected static void setJavaProps(final Properties javaProps) {
    removeJavaProperties(currentJavaProps);
    addJavaProperties(javaProps);
    currentJavaProps = javaProps;
  }

  protected static ProxyCache getProxyCaches(final int index) {
    return proxyCaches[index];
  }

  protected static void initDynamicRegionFactory() {
    DynamicRegionFactory.get().open(new DynamicRegionFactory.Config(null, null, false, true));
  }

  protected static Properties concatProperties(final Properties[] propsList) {
    Properties props = new Properties();
    for (int index = 0; index < propsList.length; ++index) {
      if (propsList[index] != null) {
        props.putAll(propsList[index]);
      }
    }
    return props;
  }

  protected static void registerExpectedExceptions(final String[] expectedExceptions) {
    SecurityTestUtils.ignoredExceptions = expectedExceptions;
  }

  protected static int createCacheServer(String authenticatorFactoryMethodName) {
    Properties authProps = new Properties();
    authProps.setProperty(SECURITY_CLIENT_AUTHENTICATOR, authenticatorFactoryMethodName);
    return createCacheServer(authProps, null, 0, false, NO_EXCEPTION);
  }

  protected static int createCacheServer(final Properties authProps, final Properties javaProps,
      final int serverPort, final int expectedResult) {
    return createCacheServer(authProps, javaProps, serverPort, false, expectedResult);
  }

  protected static int createCacheServer(Properties authProps, final Properties javaProps,
      final int serverPort, final boolean setupDynamicRegionFactory, final int expectedResult) {
    if (authProps == null) {
      authProps = new Properties();
    }
    authProps.setProperty(MCAST_PORT, "0");
    authProps.setProperty(LOCATORS, "localhost[" + getDUnitLocatorPort() + "]");
    authProps.setProperty(SECURITY_LOG_LEVEL, "finest");

    getLogWriter().info("Set the server properties to: " + authProps);
    getLogWriter().info("Set the java properties to: " + javaProps);

    SecurityTestUtils tmpInstance = new SecurityTestUtils();
    try {
      tmpInstance.createSystem(authProps, javaProps);
    } catch (AuthenticationRequiredException ex) {
      if (expectedResult == AUTHREQ_EXCEPTION) {
        getLogWriter().info("Got expected exception when starting peer: " + ex);
        return 0;
      } else {
        fail("Got unexpected exception when starting peer", ex);
      }

    } catch (AuthenticationFailedException ex) {
      if (expectedResult == AUTHFAIL_EXCEPTION) {
        getLogWriter().info("Got expected exception when starting peer: " + ex);
        return 0;
      } else {
        fail("Got unexpected exception when starting peer", ex);
      }

    } catch (Exception ex) {
      fail("Got unexpected exception when starting peer", ex);
    }

    if (setupDynamicRegionFactory) {
      initDynamicRegionFactory();
    }

    tmpInstance.openCache();

    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);

    RegionAttributes attrs = factory.create();

    Region region = cache.createRegion(REGION_NAME, attrs);
    int port = serverPort <= 0 ? 0 : serverPort;

    CacheServer server1 = cache.addCacheServer();

    server1.setPort(port);
    server1.setNotifyBySubscription(true);
    try {
      server1.start();
    } catch (AuthenticationRequiredException ex) {
      if (expectedResult == AUTHREQ_EXCEPTION) {
        getLogWriter().info("Got expected exception when starting server: " + ex);
        return 0;
      } else {
        fail("Got unexpected exception when starting server", ex);
      }
    } catch (Exception ex) {
      fail("Got unexpected exception when starting server", ex);
    }

    return server1.getPort();
  }

  // 1
  protected static void createCacheClient(final String authInitModule, final Properties authProps,
      final Properties javaProps, final int[] ports, final int numConnections,
      final int expectedResult) {
    createCacheClient(authInitModule, authProps, javaProps, ports, numConnections, false,
        expectedResult);
  }

  // 2 a
  protected static void createCacheClient(final String authInitModule, final Properties authProps,
      final Properties javaProps, final int[] ports, final int numConnections,
      final boolean multiUserMode, final int expectedResult) {
    createCacheClient(authInitModule, authProps, javaProps, ports, numConnections, false,
        multiUserMode, expectedResult);
  }

  // 3
  protected static void createCacheClientWithDynamicRegion(final String authInitModule,
      final Properties authProps, final Properties javaProps, final int[] ports,
      final int numConnections, final boolean setupDynamicRegionFactory, final int expectedResult) {
    createCacheClient(authInitModule, authProps, javaProps, ports, numConnections,
        setupDynamicRegionFactory, false, expectedResult);
  }

  /** create a client cache using the dunit locator to find servers */
  protected static void createCacheClientWithDynamicRegion(final String authInitModule,
      final Properties authProps, final Properties javaProps, final int numConnections,
      final boolean setupDynamicRegionFactory, final int expectedResult) {
    createCacheClient(authInitModule, authProps, javaProps, new int[0], numConnections,
        setupDynamicRegionFactory, false, expectedResult);
  }

  // 4
  protected static void createCacheClient(final String authInitModule, final Properties authProps,
      final Properties javaProps, final int[] ports, final int numConnections,
      final boolean setupDynamicRegionFactory, final boolean multiUserMode,
      final int expectedResult) {
    createCacheClient(authInitModule, authProps, javaProps, ports, numConnections,
        setupDynamicRegionFactory, multiUserMode, true, expectedResult);
  }

  // 5
  protected static void createCacheClient(final String authInitModule, Properties authProps,
      final Properties javaProps, int[] ports, final int numConnections,
      final boolean setupDynamicRegionFactory, final boolean multiUserMode,
      final boolean subscriptionEnabled, final int expectedResult) {
    multiUserAuthMode = multiUserMode;

    if (authProps == null) {
      authProps = new Properties();
    }
    authProps.setProperty(MCAST_PORT, "0");
    authProps.setProperty(LOCATORS, "");
    authProps.setProperty(SECURITY_LOG_LEVEL, "finest");
    if (Version.CURRENT_ORDINAL >= 75) {
      authProps.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
          UsernamePrincipal.class.getName());
    }
    // TODO (ashetkar) Add " && (!multiUserAuthMode)" below.
    if (authInitModule != null) {
      authProps.setProperty(SECURITY_CLIENT_AUTH_INIT, authInitModule);
    }

    SecurityTestUtils tmpInstance = new SecurityTestUtils();
    tmpInstance.createSystem(authProps, javaProps);

    AttributesFactory factory = new AttributesFactory();

    int[] portsI = new int[ports.length];
    for (int z = 0; z < ports.length; z++) {
      portsI[z] = ports[z];
    }

    try {
      PoolFactory poolFactory = PoolManager.createFactory();
      poolFactory.setRetryAttempts(200);

      if (multiUserAuthMode) {
        poolFactory.setMultiuserAuthentication(multiUserAuthMode);
        // [sumedh] Why is this false here only to be overridden in
        // ClientServerTestCase.configureConnectionPoolWithNameAndFactory below?
        // Actually setting it to false causes MultiUserAPIDUnitTest to fail.
        // poolFactory.setSubscriptionEnabled(false);
      }

      pool = configureConnectionPoolWithNameAndFactory(factory, getIPLiteral(), portsI,
          subscriptionEnabled, 0, numConnections, null, null, poolFactory);

      if (setupDynamicRegionFactory) {
        initClientDynamicRegionFactory(pool.getName());
      }

      tmpInstance.openCache();
      try {
        getLogWriter().info("multi-user mode " + multiUserAuthMode);
        proxyCaches[0] = (ProxyCache) ((PoolImpl) pool).createAuthenticatedCacheView(authProps);
        if (!multiUserAuthMode) {
          fail("Expected a UnsupportedOperationException but got none in single-user mode");
        }

      } catch (UnsupportedOperationException uoe) {
        if (!multiUserAuthMode) {
          getLogWriter().info("Got expected UnsupportedOperationException in single-user mode");
        } else {
          fail("Got unexpected exception in multi-user mode ", uoe);
        }
      }

      factory.setScope(Scope.LOCAL);
      if (multiUserAuthMode) {
        factory.setDataPolicy(DataPolicy.EMPTY);
      }

      RegionAttributes attrs = factory.create();

      cache.createRegionFactory(attrs).create(REGION_NAME);

      // if (expectedResult != NO_EXCEPTION && expectedResult != NOFORCE_AUTHREQ_EXCEPTION) {
      // if (!multiUserAuthMode) {
      // fail("Expected an exception when starting client");
      // }
      // }

    } catch (AuthenticationRequiredException ex) {
      if (expectedResult == AUTHREQ_EXCEPTION || expectedResult == NOFORCE_AUTHREQ_EXCEPTION) {
        getLogWriter().info("Got expected exception when starting client: " + ex);
      } else {
        fail("Got unexpected exception when starting client", ex);
      }

    } catch (AuthenticationFailedException ex) {
      if (expectedResult == AUTHFAIL_EXCEPTION) {
        getLogWriter().info("Got expected exception when starting client: " + ex);
      } else {
        fail("Got unexpected exception when starting client", ex);
      }

    } catch (ServerRefusedConnectionException ex) {
      if (expectedResult == CONNREFUSED_EXCEPTION) {
        getLogWriter().info("Got expected exception when starting client: " + ex);
      } else {
        fail("Got unexpected exception when starting client", ex);
      }

    } catch (GemFireSecurityException ex) {
      if (expectedResult == SECURITY_EXCEPTION) {
        getLogWriter().info("Got expected exception when starting client: " + ex);
      } else {
        fail("Got unexpected exception when starting client", ex);
      }
    } catch (Exception ex) {
      fail("Got unexpected exception when starting client", ex);
    }
  }

  protected static void createCacheClientForMultiUserMode(final int numOfUsers,
      final String authInitModule, final Properties[] authProps, final Properties javaProps,
      final int[] ports, final int numConnections, final boolean setupDynamicRegionFactory,
      final int expectedResult) {
    createCacheClientForMultiUserMode(numOfUsers, authInitModule, authProps, javaProps, ports,
        numConnections, setupDynamicRegionFactory, null, expectedResult);
  }

  protected static void createCacheClientForMultiUserMode(final int numOfUsers,
      final String authInitModule, final Properties[] authProps, final Properties javaProps,
      final int[] ports, final int numConnections, final boolean setupDynamicRegionFactory,
      final String durableClientId, final int expectedResult) {
    if (numOfUsers < 1) {
      fail("Number of users cannot be less than one");
    }

    multiUserAuthMode = true;

    if (numOfUsers != authProps.length) {
      fail("Number of authProps provided does not match with numOfUsers specified, "
          + authProps.length);
    }

    if (authProps[0] == null) {
      authProps[0] = new Properties();
    }
    authProps[0].setProperty(MCAST_PORT, "0");
    authProps[0].setProperty(LOCATORS, "");
    authProps[0].setProperty(SECURITY_LOG_LEVEL, "finest");

    Properties props = new Properties();

    if (authInitModule != null) {
      authProps[0].setProperty(SECURITY_CLIENT_AUTH_INIT, authInitModule);
      props.setProperty(SECURITY_CLIENT_AUTH_INIT, authInitModule);
    }

    if (durableClientId != null) {
      props.setProperty(DURABLE_CLIENT_ID, durableClientId);
      props.setProperty(DURABLE_CLIENT_TIMEOUT,
          String.valueOf(DistributionConfig.DEFAULT_DURABLE_CLIENT_TIMEOUT));
    }

    if (Version.CURRENT.ordinal() >= 75) {
      props.put(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
          "org.apache.geode.security.templates.UsernamePrincipal");
    }

    SecurityTestUtils tmpInstance = new SecurityTestUtils();
    tmpInstance.createSystem(props, javaProps);

    AttributesFactory factory = new AttributesFactory();

    int[] portsI = new int[ports.length];
    for (int z = 0; z < ports.length; z++) {
      portsI[z] = ports[z];
    }

    try {
      tmpInstance.openCache();

      PoolFactory poolFactory = PoolManager.createFactory();
      poolFactory.setRetryAttempts(200);
      poolFactory.setMultiuserAuthentication(multiUserAuthMode);
      poolFactory.setSubscriptionEnabled(true);

      pool = configureConnectionPoolWithNameAndFactory(factory, getIPLiteral(), portsI, true, 1,
          numConnections, null, null, poolFactory);

      if (setupDynamicRegionFactory) {
        initClientDynamicRegionFactory(pool.getName());
      }

      proxyCaches = new ProxyCache[numOfUsers];
      for (int i = 0; i < numOfUsers; i++) {
        proxyCaches[i] = (ProxyCache) ((PoolImpl) pool).createAuthenticatedCacheView(authProps[i]);
      }

      factory.setScope(Scope.LOCAL);
      factory.setDataPolicy(DataPolicy.EMPTY);
      RegionAttributes attrs = factory.create();

      cache.createRegion(REGION_NAME, attrs);

      if (expectedResult != NO_EXCEPTION && expectedResult != NOFORCE_AUTHREQ_EXCEPTION) {
        if (!multiUserAuthMode) {
          fail("Expected an exception when starting client");
        }
      }

    } catch (AuthenticationRequiredException ex) {
      if (expectedResult == AUTHREQ_EXCEPTION || expectedResult == NOFORCE_AUTHREQ_EXCEPTION) {
        getLogWriter().info("Got expected exception when starting client: " + ex);
      } else {
        fail("Got unexpected exception when starting client", ex);
      }

    } catch (AuthenticationFailedException ex) {
      if (expectedResult == AUTHFAIL_EXCEPTION) {
        getLogWriter().info("Got expected exception when starting client: " + ex);
      } else {
        fail("Got unexpected exception when starting client", ex);
      }

    } catch (ServerRefusedConnectionException ex) {
      if (expectedResult == CONNREFUSED_EXCEPTION) {
        getLogWriter().info("Got expected exception when starting client: " + ex);
      } else {
        fail("Got unexpected exception when starting client", ex);
      }

    } catch (Exception ex) {
      fail("Got unexpected exception when starting client", ex);
    }
  }

  protected static void createProxyCache(final int[] userIndices, final Properties[] props) {
    int j = 0;
    for (int i : userIndices) {
      proxyCaches[i] = (ProxyCache) ((PoolImpl) pool).createAuthenticatedCacheView(props[j]);
      j++;
    }
  }

  protected static void startLocator(final String name, int port, final Properties extraProps,
      final Properties javaProps, final String[] expectedExceptions) {
    try {
      Properties authProps = new Properties();

      if (extraProps != null) {
        authProps.putAll(extraProps);
      }
      authProps.setProperty(MCAST_PORT, "0");
      authProps.setProperty(LOCATORS, getIPLiteral() + "[" + port + "]");
      authProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

      clearStaticSSLContext();

      setJavaProps(javaProps);

      File logFile = new File(name + "-locator" + port + ".log");
      FileOutputStream logOut = new FileOutputStream(logFile);
      PrintStream logStream = new PrintStream(logOut);
      addIgnoredExceptions(expectedExceptions);
      logStream.flush();

      locator = Locator.startLocatorAndDS(port, logFile, null, authProps);

    } catch (IOException ex) {
      fail("While starting locator on port " + port, ex);
    }
  }

  protected static void stopLocator(final int port, final String[] expectedExceptions) {
    try {
      locator.stop();
      removeExpectedExceptions(expectedExceptions);

    } catch (Exception ex) {
      fail("While stopping locator on port " + port, ex);
    }
  }

  protected static Cache getCache() {
    return cache;
  }

  protected static Object getLocalValue(final Region region, final Object key) {
    Region.Entry entry = region.getEntry(key);
    if (entry != null) {
      try { // Handle race conditions with concurrent destroy ops
        return entry.getValue();
      } catch (EntryDestroyedException e) {
        return null;
      }
    } else {
      return null;
    }
  }

  protected static void doProxyCacheClose() {
    for (int i = 0; i < proxyCaches.length; i++) {
      proxyCaches[i].close();
    }
  }

  protected static void doPutAllP() throws Exception {
    Region region = getCache().getRegion(REGION_NAME);
    assertNotNull(region);

    Map<String, Employee> map = new LinkedHashMap<>();
    map.put("1010L", new Employee(1010L, "John", "Doe"));

    region.putAll(map);
  }

  protected static void doPuts(final int num) {
    doPutsP(num, NO_EXCEPTION, false);
  }

  protected static void doPuts(final int num, final int expectedResult) {
    doPutsP(num, expectedResult, false);
  }

  protected static void verifySizeOnServer(final int size) {
    verifySizeOnServer(size, NO_EXCEPTION);
  }

  protected static void verifyIsEmptyOnServer(final boolean isEmpty) {
    verifyIsEmptyOnServer(isEmpty, NO_EXCEPTION);
  }

  protected static void doMultiUserPuts(final int num, final int numOfUsers,
      final int[] expectedResults) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtils.doMultiUserPuts(): numOfUsers = " + numOfUsers
          + ", but expected results " + expectedResults.length);
    }

    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("PUT: MultiUser# " + i);
      doPutsP(num, i, expectedResults[i], false);
    }
  }

  protected static void doGets(final int num) {
    doGetsP(num, NO_EXCEPTION, false);
  }

  protected static void doGets(final int num, final int expectedResult) {
    doGetsP(num, expectedResult, false);
  }

  protected static void doMultiUserGetAll(final int numOfUsers, final int[] expectedResults) {
    doMultiUserGetAll(numOfUsers, expectedResults, false);
  }

  protected static void doMultiUserGetAll(final int numOfUsers, final int[] expectedResults,
      final boolean useTX) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtils.doMultiUserGetAll(): numOfUsers = " + numOfUsers
          + ", but expected results " + expectedResults.length);
    }

    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("GET_ALL" + (useTX ? " in TX" : "") + ": MultiUser# " + i);
      doGetAllP(i, expectedResults[i], useTX);
    }
  }

  protected static void doMultiUserGets(final int num, final int numOfUsers,
      final int[] expectedResults) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtils.doMultiUserGets(): numOfUsers = " + numOfUsers
          + ", but expected results " + expectedResults.length);
    }

    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("GET: MultiUser# " + i);
      doGetsP(num, i, expectedResults[i], false);
    }
  }

  protected static void doMultiUserRegionDestroys(final int numOfUsers,
      final int[] expectedResults) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtils.doMultiUserRegionDestroys(): numOfUsers = " + numOfUsers
          + ", but expected results " + expectedResults.length);
    }

    for (int i = numOfUsers - 1; i >= 0; i--) {
      getLogWriter().info("DESTROY: MultiUser# " + i);
      doRegionDestroysP(i, expectedResults[i]);
    }
  }

  protected static void doMultiUserDestroys(final int num, final int numOfUsers,
      final int[] expectedResults) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtils.doMultiUserDestroys(): numOfUsers = " + numOfUsers
          + ", but expected results " + expectedResults.length);
    }

    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("DESTROY: MultiUser# " + i);
      doDestroysP(num, i, expectedResults[i]);
    }
  }

  protected static void doMultiUserInvalidates(final int num, final int numOfUsers,
      final int[] expectedResults) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtils.doMultiUserInvalidates(): numOfUsers = " + numOfUsers
          + ", but expected results " + expectedResults.length);
    }

    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("INVALIDATE: MultiUser# " + i);
      doInvalidatesP(num, i, expectedResults[i]);
    }
  }

  protected static void doMultiUserContainsKeys(final int num, final int numOfUsers,
      final int[] expectedResults, final boolean[] results) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtils.doMultiUserContainsKeys(): numOfUsers = " + numOfUsers
          + ", but #expected results " + expectedResults.length);
    }

    if (numOfUsers != results.length) {
      fail("SecurityTestUtils.doMultiUserContainsKeys(): numOfUsers = " + numOfUsers
          + ", but #expected output " + results.length);
    }

    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("CONTAINS_KEY: MultiUser# " + i);
      doContainsKeysP(num, i, expectedResults[i], results[i]);
    }
  }

  protected static void doMultiUserQueries(final int numOfUsers, final int[] expectedResults,
      final int valueSize) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtils.doMultiUserQueries(): numOfUsers = " + numOfUsers
          + ", but #expected results " + expectedResults.length);
    }

    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("QUERY: MultiUser# " + i);
      doQueriesP(i, expectedResults[i], valueSize);
    }
  }

  protected static void doMultiUserFE(final int numOfUsers, final Function function,
      final int[] expectedResults, final boolean isFailOverCase) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtils.doMultiUserFE(): numOfUsers = " + numOfUsers
          + ", but #expected results " + expectedResults.length);
    }

    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("FunctionExecute:onRegion MultiUser# " + i);
      doFunctionExecuteP(i, function, expectedResults[i], "region");
    }

    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("FunctionExecute:onServer MultiUser# " + i);
      doFunctionExecuteP(i, function, expectedResults[i], "server");
    }

    if (!isFailOverCase) {
      for (int i = 0; i < numOfUsers; i++) {
        getLogWriter().info("FunctionExecute:onServers MultiUser# " + i);
        doFunctionExecuteP(i, function, expectedResults[i], "servers");
      }
    }
  }

  protected static void doMultiUserQueryExecute(final int numOfUsers, final int[] expectedResults,
      final int result) {
    if (numOfUsers != expectedResults.length) {
      fail("SecurityTestUtils.doMultiUserFE(): numOfUsers = " + numOfUsers
          + ", but #expected results " + expectedResults.length);
    }

    for (int i = 0; i < numOfUsers; i++) {
      getLogWriter().info("QueryExecute: MultiUser# " + i);
      doQueryExecuteP(i, expectedResults[i], result);
    }
  }

  protected static void doLocalGets(final int num) {
    doLocalGetsP(num, false);
  }

  protected static void doNPuts(final int num) {
    doPutsP(num, NO_EXCEPTION, true);
  }

  protected static void doNPuts(final int num, final int expectedResult) {
    doPutsP(num, expectedResult, true);
  }

  protected static void doNGets(final int num) {
    doGetsP(num, NO_EXCEPTION, true);
  }

  protected static void doNGets(final int num, final int expectedResult) {
    doGetsP(num, expectedResult, true);
  }

  protected static void doNLocalGets(final int num) {
    doLocalGetsP(num, true);
  }

  protected static void doSimpleGet(final String expectedResult) {
    if (regionRef != null) {
      try {
        regionRef.get("KEY");
        if (expectedResult != null && expectedResult.endsWith("Exception")) {
          fail("Expected " + expectedResult + " but found none in doSimpleGet()");
        }

      } catch (Exception e) {
        if (!e.getClass().getSimpleName().endsWith(expectedResult)) {
          fail("Expected " + expectedResult + " but found " + e.getClass().getSimpleName()
              + " in doSimpleGet()");
        } else {
          getLogWriter().fine("Got expected " + e.getClass().getSimpleName() + " in doSimpleGet()");
        }
      }
    }
  }

  protected static void doSimplePut(final String expectedResult) {
    if (regionRef != null) {
      try {
        regionRef.put("KEY", "VALUE");
        if (expectedResult != null && expectedResult.endsWith("Exception")) {
          fail("Expected " + expectedResult + " but found none in doSimplePut()");
        }

      } catch (Exception e) {
        if (!e.getClass().getSimpleName().endsWith(expectedResult)) {
          fail("Expected " + expectedResult + " but found " + e.getClass().getSimpleName()
              + " in doSimplePut()", e);
        } else {
          getLogWriter().fine("Got expected " + e.getClass().getSimpleName() + " in doSimplePut()");
        }
      }
    }
  }

  /**
   * This is a hack using reflection to clear the static objects in JSSE since otherwise changing
   * the javax.* store related properties has no effect during the course of running dunit suite
   * unless the VMs are restarted.
   */
  protected static void clearStaticSSLContext() {
    ServerSocketFactory defaultServerFact = SSLServerSocketFactory.getDefault();

    // Get the class of this and use reflection to blank out any static SSLContext objects inside
    Map<Field, Object> contextMap =
        getSSLFields(defaultServerFact, new Class[] {SSLContext.class, SSLContextSpi.class});
    makeNullSSLFields(defaultServerFact, contextMap);

    for (Iterator contextObjsIter = contextMap.values().iterator(); contextObjsIter.hasNext();) {
      Object contextObj = contextObjsIter.next();
      Map<Field, Object> contextObjsMap = getSSLFields(contextObj, new Class[] {TrustManager.class,
          KeyManager.class, TrustManager[].class, KeyManager[].class});
      makeNullSSLFields(contextObj, contextObjsMap);
    }

    makeNullStaticField(SSLServerSocketFactory.class);

    // Do the same for normal SSL socket factory
    SocketFactory defaultFact = SSLSocketFactory.getDefault();
    contextMap = getSSLFields(defaultFact, new Class[] {SSLContext.class, SSLContextSpi.class});
    makeNullSSLFields(defaultFact, contextMap);

    for (Iterator contextObjsIter = contextMap.values().iterator(); contextObjsIter.hasNext();) {
      Object contextObj = contextObjsIter.next();
      Map<Field, Object> contextObjsMap = getSSLFields(contextObj, new Class[] {TrustManager.class,
          KeyManager.class, TrustManager[].class, KeyManager[].class});
      makeNullSSLFields(contextObj, contextObjsMap);
    }

    makeNullStaticField(SSLSocketFactory.class);
    makeNullStaticField(SSLContext.class);
  }

  protected static void closeCache() {
    removeExpectedExceptions(ignoredExceptions);

    if (cache != null && !cache.isClosed()) {
      DistributedSystem sys = cache.getDistributedSystem();
      cache.close();
      sys.disconnect();
      cache = null;
    }

    disconnectFromDS();
  }

  protected static void closeCache(final Boolean keepAlive) {
    removeExpectedExceptions(ignoredExceptions);

    if (cache != null && !cache.isClosed()) {
      DistributedSystem sys = cache.getDistributedSystem();
      cache.close(keepAlive);
      sys.disconnect();
      cache = null;
    }

    disconnectFromDS();
  }

  // ------------------------- private static methods -------------------------

  private static void initClientDynamicRegionFactory(final String poolName) {
    DynamicRegionFactory.get().open(new DynamicRegionFactory.Config(null, poolName, false, true));
  }

  private static void addJavaProperties(final Properties javaProps) {
    if (javaProps != null) {
      for (Iterator iter = javaProps.entrySet().iterator(); iter.hasNext();) {
        Map.Entry entry = (Map.Entry) iter.next();
        System.setProperty((String) entry.getKey(), (String) entry.getValue());
      }
    }
  }

  private static void removeJavaProperties(final Properties javaProps) {
    if (javaProps != null) {
      Properties props = System.getProperties();

      for (Iterator iter = javaProps.keySet().iterator(); iter.hasNext();) {
        props.remove(iter.next());
      }

      System.setProperties(props);
    }
  }

  private static void verifySizeOnServer(final int size, final int expectedResult) {
    Region region = getRegion(0, expectedResult);
    try {
      int sizeOnServer = region.sizeOnServer();
      if (expectedResult != NO_EXCEPTION) {
        fail("Expected a NotAuthorizedException while executing sizeOnServer");
      }
      assertEquals(size, sizeOnServer);
    } catch (NoSuchMethodError ex) {
      // expected with backward-compatibility tests
    } catch (Exception ex) {
      fail("Got unexpected exception when executing sizeOnServer", ex);
    }
  }

  private static void verifyIsEmptyOnServer(final boolean isEmpty, final int expectedResult) {
    Region region = getRegion(0, expectedResult);
    try {
      boolean isEmptyOnServer = region.isEmptyOnServer();
      if (expectedResult != NO_EXCEPTION) {
        fail("Expected a NotAuthorizedException while executing isEmptyOnServer");
      }
      assertEquals(isEmpty, isEmptyOnServer);
    } catch (NoSuchMethodError ex) {
      // expected with old version clients
    } catch (Exception ex) {
      fail("Got unexpected exception when executing isEmptyOnServer", ex);
    }
  }

  private static void doPutsP(final int num, final int expectedResult, final boolean newVals) {
    doPutsP(num, 0, expectedResult, newVals);
  }

  private static void doPutsP(final int num, final int multiUserIndex, final int expectedResult,
      final boolean newVals) {
    assertTrue(num <= KEYS.length);
    Region region = getRegion(multiUserIndex, expectedResult);

    for (int index = 0; index < num; ++index) {
      try {
        if (newVals) {
          region.put(KEYS[index], NVALUES[index]);
        } else {
          region.put(KEYS[index], VALUES[index]);
        }
        if (expectedResult != NO_EXCEPTION) {
          fail("Expected a NotAuthorizedException while doing puts");
        }

      } catch (NoAvailableServersException ex) {
        if (expectedResult == NO_AVAILABLE_SERVERS) {
          getLogWriter().info("Got expected NoAvailableServers when doing puts: " + ex.getCause());
          continue;
        } else {
          fail("Got unexpected exception when doing puts", ex);
        }

      } catch (ServerConnectivityException ex) {
        if ((expectedResult == NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          getLogWriter()
              .info("Got expected NotAuthorizedException when doing puts: " + ex.getCause());
          continue;
        }

        if ((expectedResult == AUTHREQ_EXCEPTION)
            && (ex.getCause() instanceof AuthenticationRequiredException)) {
          getLogWriter().info(
              "Got expected AuthenticationRequiredException when doing puts: " + ex.getCause());
          continue;
        }

        if ((expectedResult == AUTHFAIL_EXCEPTION)
            && (ex.getCause() instanceof AuthenticationFailedException)) {
          getLogWriter()
              .info("Got expected AuthenticationFailedException when doing puts: " + ex.getCause());
          continue;
        } else if (expectedResult == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing puts: " + ex);
        } else {
          fail("Got unexpected exception when doing puts", ex);
        }

      } catch (Exception ex) {
        if (expectedResult == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing puts: " + ex);
        } else {
          fail("Got unexpected exception when doing puts", ex);
        }
      }
    }
  }

  private static Region getRegion(final int multiUserIndex, final int expectedResult) {
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(REGION_NAME);
        regionRef = region;
      } else {
        region = getCache().getRegion(REGION_NAME);
      }
      assertNotNull(region);

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception during getRegion: " + ex);
      } else {
        fail("Got unexpected exception during getRegion", ex);
      }
    }
    return region;
  }

  private static Map<Field, Object> getSSLFields(final Object obj, final Class[] classes) {
    Map<Field, Object> resultFields = new HashMap<>();
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

      } catch (IllegalAccessException ex) {
        getLogWriter().warning("Exception while getting SSL fields.", ex);
      }
    }
    return resultFields;
  }

  private static void makeNullSSLFields(final Object obj, final Map<Field, Object> fieldMap) {
    for (Iterator<Map.Entry<Field, Object>> fieldIter = fieldMap.entrySet().iterator(); fieldIter
        .hasNext();) {
      Map.Entry<Field, Object> entry = fieldIter.next();
      Field field = entry.getKey();
      Object fieldObj = entry.getValue();

      try {
        field.setAccessible(true);
        makeNullStaticField(fieldObj.getClass());
        field.set(obj, null);
        assertNull(field.get(obj));

      } catch (IllegalAccessException ex) {
        getLogWriter().warning("Exception while clearing SSL fields.", ex);
      }
    }
  }

  /**
   * Deal with javax SSL properties
   */
  private static void makeNullStaticField(final Class sslClass) {
    Field[] fields = sslClass.getDeclaredFields();
    for (int index = 0; index < fields.length; ++index) {
      Field field = fields[index];

      try {
        if (Modifier.isStatic(field.getModifiers())) {
          field.setAccessible(true);
          if (field.getClass().equals(boolean.class)) {
            field.setBoolean(null, false);
            assertFalse(field.getBoolean(null));

          } else if (sslClass.isInstance(field.get(null))) {
            field.set(null, null);
            assertNull(field.get(null));
          }
        }

      } catch (IllegalAccessException ex) {
        getLogWriter().warning("Exception while clearing static SSL field.", ex);
      } catch (ClassCastException ex) {
        getLogWriter().warning("Exception while clearing static SSL field.", ex);
      }
    }
  }

  private static void doQueryExecuteP(final int multiUserIndex, final int expectedResult,
      final int expectedValue) {
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(REGION_NAME);
      } else {
        region = getCache().getRegion(REGION_NAME);
      }
      assertNotNull(region);

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when executing query: " + ex);
      } else {
        fail("Got unexpected exception when executing query", ex);
      }
    }

    try {
      String queryString = "SELECT DISTINCT * FROM " + region.getFullPath();
      Query query = null;

      if (multiUserAuthMode) {
        query = proxyCaches[multiUserIndex].getQueryService().newQuery(queryString);
      } else {
        region.getCache().getQueryService().newQuery(queryString);
      }

      SelectResults result = (SelectResults) query.execute();
      if (expectedResult != NO_EXCEPTION) {
        fail("Expected a NotAuthorizedException while executing function");
      }
      assertEquals(expectedValue, result.asList().size());

    } catch (NoAvailableServersException ex) {
      if (expectedResult == NO_AVAILABLE_SERVERS) {
        getLogWriter()
            .info("Got expected NoAvailableServers when executing query: " + ex.getCause());
      } else {
        fail("Got unexpected exception when executing query", ex);
      }

    } catch (ServerConnectivityException ex) {
      if ((expectedResult == NOTAUTHZ_EXCEPTION)
          && (ex.getCause() instanceof NotAuthorizedException)) {
        getLogWriter()
            .info("Got expected NotAuthorizedException when executing query: " + ex.getCause());
      } else if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when executing query: " + ex);
      } else {
        fail("Got unexpected exception when executing query", ex);
      }

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when executing query: " + ex);
      } else {
        fail("Got unexpected exception when executing query", ex);
      }
    }
  }

  private static void doFunctionExecuteP(final int multiUserIndex, final Function function,
      int expectedResult, final String method) {
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(REGION_NAME);
      } else {
        region = getCache().getRegion(REGION_NAME);
      }
      assertNotNull(region);

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when executing function: " + ex);
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
      if (expectedResult != NO_EXCEPTION) {
        fail("Expected a NotAuthorizedException while executing function");
      }

    } catch (NoAvailableServersException ex) {
      if (expectedResult == NO_AVAILABLE_SERVERS) {
        getLogWriter()
            .info("Got expected NoAvailableServers when executing function: " + ex.getCause());
      } else {
        fail("Got unexpected exception when executing function", ex);
      }

    } catch (ServerConnectivityException ex) {
      if ((expectedResult == NOTAUTHZ_EXCEPTION)
          && (ex.getCause() instanceof NotAuthorizedException)) {
        getLogWriter()
            .info("Got expected NotAuthorizedException when executing function: " + ex.getCause());
      } else if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when executing function: " + ex);
      } else {
        fail("Got unexpected exception when executing function", ex);
      }

    } catch (FunctionException ex) {
      // if NOTAUTHZ_EXCEPTION AND (cause is NotAuthorizedException OR (cause is
      // ServerOperationException AND cause.cause is NotAuthorizedException))
      if (expectedResult == NOTAUTHZ_EXCEPTION && (ex.getCause() instanceof NotAuthorizedException
          || (ex.getCause() instanceof ServerOperationException
              && ex.getCause().getCause() instanceof NotAuthorizedException))) {
        getLogWriter()
            .info("Got expected NotAuthorizedException when executing function: " + ex.getCause());
      } else if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when executing function: " + ex);
      } else {
        fail("Got unexpected exception when executing function", ex);
      }

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when executing function: " + ex);
      } else {
        fail("Got unexpected exception when executing function", ex);
      }
    }
  }

  private static void doQueriesP(final int multiUserIndex, final int expectedResult,
      final int expectedValue) {
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(REGION_NAME);
      } else {
        region = getCache().getRegion(REGION_NAME);
      }
      assertNotNull(region);

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing queries: " + ex);
      } else {
        fail("Got unexpected exception when doing queries", ex);
      }
    }

    String queryStr = "SELECT DISTINCT * FROM " + region.getFullPath();
    try {
      SelectResults queryResults = region.query(queryStr);
      Set resultSet = queryResults.asSet();
      assertEquals(expectedValue, resultSet.size());
      if (expectedResult != NO_EXCEPTION) {
        fail("Expected a NotAuthorizedException while doing queries");
      }

    } catch (NoAvailableServersException ex) {
      if (expectedResult == NO_AVAILABLE_SERVERS) {
        getLogWriter().info("Got expected NoAvailableServers when doing queries: " + ex.getCause());
      } else {
        fail("Got unexpected exception when doing queries", ex);
      }

    } catch (ServerConnectivityException ex) {
      if ((expectedResult == NOTAUTHZ_EXCEPTION)
          && (ex.getCause() instanceof NotAuthorizedException)) {
        getLogWriter()
            .info("Got expected NotAuthorizedException when doing queries: " + ex.getCause());
      } else if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing queries: " + ex);
      } else {
        fail("Got unexpected exception when doing queries", ex);
      }

    } catch (QueryInvocationTargetException qite) {
      if ((expectedResult == NOTAUTHZ_EXCEPTION)
          && (qite.getCause() instanceof NotAuthorizedException)) {
        getLogWriter()
            .info("Got expected NotAuthorizedException when doing queries: " + qite.getCause());
      } else if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing queries: " + qite);
      } else {
        fail("Got unexpected exception when doing queries", qite);
      }

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing queries: " + ex);
      } else {
        fail("Got unexpected exception when doing queries", ex);
      }
    }
  }

  private static void doContainsKeysP(final int num, final int multiUserIndex,
      final int expectedResult, final boolean expectedValue) {
    assertTrue(num <= KEYS.length);

    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(REGION_NAME);
      } else {
        region = getCache().getRegion(REGION_NAME);
      }
      assertNotNull(region);

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing containsKey: " + ex);
      } else {
        fail("Got unexpected exception when doing containsKey", ex);
      }
    }

    for (int index = 0; index < num; ++index) {
      boolean result = false;

      try {
        result = region.containsKeyOnServer(KEYS[index]);
        if (expectedResult != NO_EXCEPTION) {
          fail("Expected a NotAuthorizedException while doing containsKey");
        }

      } catch (NoAvailableServersException ex) {
        if (expectedResult == NO_AVAILABLE_SERVERS) {
          getLogWriter()
              .info("Got expected NoAvailableServers when doing containsKey: " + ex.getCause());
          continue;
        } else {
          fail("Got unexpected exception when doing containsKey", ex);
        }

      } catch (ServerConnectivityException ex) {
        if ((expectedResult == NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          getLogWriter()
              .info("Got expected NotAuthorizedException when doing containsKey: " + ex.getCause());
          continue;
        } else if (expectedResult == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing containsKey: " + ex);
        } else {
          fail("Got unexpected exception when doing containsKey", ex);
        }

      } catch (Exception ex) {
        if (expectedResult == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing containsKey: " + ex);
        } else {
          fail("Got unexpected exception when doing containsKey", ex);
        }
      }

      assertEquals(expectedValue, result);
    }
  }

  private static void doInvalidatesP(final int num, final int multiUserIndex,
      final int expectedResult) {
    assertTrue(num <= KEYS.length);

    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(REGION_NAME);
      } else {
        region = getCache().getRegion(REGION_NAME);
      }
      assertNotNull(region);

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing invalidates: " + ex);
      } else {
        fail("Got unexpected exception when doing invalidates", ex);
      }
    }

    for (int index = 0; index < num; ++index) {
      try {
        region.invalidate(KEYS[index]);
        if (expectedResult != NO_EXCEPTION) {
          fail("Expected a NotAuthorizedException while doing invalidates");
        }

      } catch (NoAvailableServersException ex) {
        if (expectedResult == NO_AVAILABLE_SERVERS) {
          getLogWriter()
              .info("Got expected NoAvailableServers when doing invalidates: " + ex.getCause());
          continue;
        } else {
          fail("Got unexpected exception when doing invalidates", ex);
        }

      } catch (ServerConnectivityException ex) {
        if ((expectedResult == NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          getLogWriter()
              .info("Got expected NotAuthorizedException when doing invalidates: " + ex.getCause());
          continue;
        } else if (expectedResult == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing invalidates: " + ex);
        } else {
          fail("Got unexpected exception when doing invalidates", ex);
        }

      } catch (Exception ex) {
        if (expectedResult == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing invalidates: " + ex);
        } else {
          fail("Got unexpected exception when doing invalidates", ex);
        }
      }
    }
  }

  private static void doDestroysP(final int num, final int multiUserIndex,
      final int expectedResult) {
    assertTrue(num <= KEYS.length);

    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(REGION_NAME);
      } else {
        region = getCache().getRegion(REGION_NAME);
      }
      assertNotNull(region);

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing destroys: " + ex);
      } else {
        fail("Got unexpected exception when doing destroys", ex);
      }
    }

    for (int index = 0; index < num; ++index) {
      try {
        region.destroy(KEYS[index]);
        if (expectedResult != NO_EXCEPTION) {
          fail("Expected a NotAuthorizedException while doing destroys");
        }

      } catch (NoAvailableServersException ex) {
        if (expectedResult == NO_AVAILABLE_SERVERS) {
          getLogWriter()
              .info("Got expected NoAvailableServers when doing destroys: " + ex.getCause());
          continue;
        } else {
          fail("Got unexpected exception when doing destroys", ex);
        }

      } catch (ServerConnectivityException ex) {
        if ((expectedResult == NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          getLogWriter()
              .info("Got expected NotAuthorizedException when doing destroys: " + ex.getCause());
          continue;
        } else if (expectedResult == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing destroys: " + ex);
        } else {
          fail("Got unexpected exception when doing destroys", ex);
        }

      } catch (Exception ex) {
        if (expectedResult == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing destroys: " + ex);
        } else {
          fail("Got unexpected exception when doing destroys", ex);
        }
      }
    }
  }

  private static void doRegionDestroysP(final int multiUserIndex, final int expectedResult) {
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(REGION_NAME);
      } else {
        region = getCache().getRegion(REGION_NAME);
      }
      assertNotNull(region);

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing region destroy: " + ex);
      } else {
        fail("Got unexpected exception when doing region destroy", ex);
      }
    }

    try {
      region.destroyRegion();
      if (expectedResult != NO_EXCEPTION) {
        fail("Expected a NotAuthorizedException while doing region destroy");
      }

      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(REGION_NAME);
      } else {
        region = getCache().getRegion(REGION_NAME);
      }
      assertNull(region);

    } catch (NoAvailableServersException ex) {
      if (expectedResult == NO_AVAILABLE_SERVERS) {
        getLogWriter()
            .info("Got expected NoAvailableServers when doing region destroy: " + ex.getCause());
      } else {
        fail("Got unexpected exception when doing region destroy", ex);
      }

    } catch (ServerConnectivityException ex) {
      if ((expectedResult == NOTAUTHZ_EXCEPTION)
          && (ex.getCause() instanceof NotAuthorizedException)) {
        getLogWriter().info(
            "Got expected NotAuthorizedException when doing region destroy: " + ex.getCause());
      } else if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing region destroy: " + ex);
      } else {
        fail("Got unexpected exception when doing region destroy", ex);
      }

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing region destroy: " + ex);
      } else {
        fail("Got unexpected exception when doing region destroy", ex);
      }
    }
  }

  private static void doLocalGetsP(final int num, final boolean checkNVals) {
    assertTrue(num <= KEYS.length);

    String[] vals = VALUES;
    if (checkNVals) {
      vals = NVALUES;
    }

    final Region region = getCache().getRegion(REGION_NAME);
    assertNotNull(region);

    for (int index = 0; index < num; ++index) {
      final String key = KEYS[index];
      final String expectedVal = vals[index];
      await()
          .until(() -> expectedVal.equals(getLocalValue(region, key)));
    }

    for (int index = 0; index < num; ++index) {
      Region.Entry entry = region.getEntry(KEYS[index]);
      assertNotNull(entry);
      assertEquals(vals[index], entry.getValue());
    }
  }

  private static void doGetAllP(final int multiUserIndex, final int expectedResult,
      final boolean useTX) {
    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(REGION_NAME);
      } else {
        region = getCache().getRegion(REGION_NAME);
      }
      assertNotNull(region);

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing getAll: " + ex);
      } else {
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

      if ((expectedResult == NOTAUTHZ_EXCEPTION)) {
        assertEquals(0, entries.size());
      } else if ((expectedResult == NO_EXCEPTION)) {
        assertEquals(2, entries.size());
        assertEquals("value1", entries.get("key1"));
        assertEquals("value2", entries.get("key2"));
      }

    } catch (NoAvailableServersException ex) {
      if (expectedResult == NO_AVAILABLE_SERVERS) {
        getLogWriter().info("Got expected NoAvailableServers when doing getAll: " + ex.getCause());
      } else {
        fail("Got unexpected exception when doing getAll", ex);
      }

    } catch (ServerConnectivityException ex) {
      if ((expectedResult == NOTAUTHZ_EXCEPTION)
          && (ex.getCause() instanceof NotAuthorizedException)) {
        getLogWriter()
            .info("Got expected NotAuthorizedException when doing getAll: " + ex.getCause());
      } else if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing getAll: " + ex);
      } else {
        fail("Got unexpected exception when doing getAll", ex);
      }

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing getAll: " + ex);
      } else {
        fail("Got unexpected exception when doing getAll", ex);
      }
    }
  }

  private static void doGetsP(final int num, final int expectedResult, final boolean newVals) {
    doGetsP(num, 0, expectedResult, newVals);
  }

  private static void doGetsP(final int num, final int multiUserIndex, final int expectedResult,
      final boolean newVals) {
    assertTrue(num <= KEYS.length);

    Region region = null;
    try {
      if (multiUserAuthMode) {
        region = proxyCaches[multiUserIndex].getRegion(REGION_NAME);
      } else {
        region = getCache().getRegion(REGION_NAME);
      }
      assertNotNull(region);

    } catch (Exception ex) {
      if (expectedResult == OTHER_EXCEPTION) {
        getLogWriter().info("Got expected exception when doing gets: " + ex);
      } else {
        fail("Got unexpected exception when doing gets", ex);
      }
    }

    for (int index = 0; index < num; ++index) {
      Object value = null;
      try {

        try {
          region.localInvalidate(KEYS[index]);
        } catch (Exception ex) {
        }

        value = region.get(KEYS[index]);
        if (expectedResult != NO_EXCEPTION) {
          fail("Expected a NotAuthorizedException while doing gets");
        }

      } catch (NoAvailableServersException ex) {
        if (expectedResult == NO_AVAILABLE_SERVERS) {
          getLogWriter().info("Got expected NoAvailableServers when doing gets: " + ex.getCause());
          continue;
        } else {
          fail("Got unexpected exception when doing gets", ex);
        }

      } catch (ServerConnectivityException ex) {
        if ((expectedResult == NOTAUTHZ_EXCEPTION)
            && (ex.getCause() instanceof NotAuthorizedException)) {
          getLogWriter()
              .info("Got expected NotAuthorizedException when doing gets: " + ex.getCause());
          continue;
        } else if (expectedResult == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing gets: " + ex);
        } else {
          fail("Got unexpected exception when doing gets", ex);
        }

      } catch (Exception ex) {
        if (expectedResult == OTHER_EXCEPTION) {
          getLogWriter().info("Got expected exception when doing gets: " + ex);
        } else {
          fail("Got unexpected exception when doing gets", ex);
        }
      }

      assertNotNull(value);

      if (newVals) {
        assertEquals(NVALUES[index], value);
      } else {
        assertEquals(VALUES[index], value);
      }
    }
  }

  // ----------------------------- member methods -----------------------------

  public DistributedSystem createSystem(final Properties sysProps, final Properties javaProps) {
    closeCache();
    clearStaticSSLContext();
    setJavaProps(javaProps);

    DistributedSystem dsys = distributedTestCase.getSystem(sysProps);
    assertNotNull(dsys);
    addIgnoredExceptions(ignoredExceptions);
    return dsys;
  }

  private void openCache() {
    assertNotNull(distributedTestCase.basicGetSystem());
    assertTrue(distributedTestCase.basicGetSystem().isConnected());
    cache = CacheFactory.create(distributedTestCase.basicGetSystem());
    assertNotNull(cache);
  }

  // ------------------------------- inner classes ----------------------------

  public static class Employee implements PdxSerializable {

    private Long Id;
    private String fname;
    private String lname;

    public Employee() {}

    public Employee(Long id, String fn, String ln) {
      this.Id = id;
      this.fname = fn;
      this.lname = ln;
    }

    /**
     * For test purpose, to make sure the object is not deserialized
     */
    @Override
    public void fromData(PdxReader in) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void toData(PdxWriter out) {
      out.writeLong("Id", Id);
      out.writeString("fname", fname);
      out.writeString("lname", lname);
    }
  }

}
