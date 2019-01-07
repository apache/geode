/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.client.internal;

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SERVER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.security.AuthenticationRequiredException;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.Invoke;
import org.apache.geode.test.dunit.RMIException;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.util.test.TestUtil;

/**
 * Tests cacheserver ssl support added. See https://svn.gemstone.com/trac/gemfire/ticket/48995 for
 * details
 */
@Category({ClientServerTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class CacheServerSSLConnectionDUnitTest extends JUnit4DistributedTestCase {

  private static boolean useOldSSLSettings;

  @Parameterized.Parameters
  public static Collection<Boolean> data() {
    List<Boolean> result = new ArrayList<>();
    result.add(Boolean.TRUE);
    result.add(Boolean.FALSE);
    return result;
  }

  public CacheServerSSLConnectionDUnitTest(Boolean useOldSSLSettings) {
    super();
    CacheServerSSLConnectionDUnitTest.useOldSSLSettings = useOldSSLSettings.booleanValue();
  }

  private static final String TRUSTED_STORE = "trusted.keystore";
  private static final String CLIENT_KEY_STORE = "default.keystore";
  private static final String CLIENT_TRUST_STORE = "default.keystore";
  private static final String SERVER_KEY_STORE = "default.keystore";
  private static final String SERVER_TRUST_STORE = "default.keystore";

  private static CacheServerSSLConnectionDUnitTest instance;

  private Cache cache;
  private CacheServer cacheServer;
  private ClientCache clientCache;
  private int cacheServerPort;
  private String hostName;

  @Override
  public final void preSetUp() throws Exception {
    disconnectAllFromDS();
    instance = this;
    Invoke
        .invokeInEveryVM(() -> instance = new CacheServerSSLConnectionDUnitTest(useOldSSLSettings));
  }

  @AfterClass
  public static void postClass() {
    Invoke.invokeInEveryVM(() -> instance = null);
    instance = null;
  }

  public Cache createCache(Properties props) throws Exception {
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    cache = new CacheFactory(props).create();
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  private int createServer() throws IOException {
    cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    hostName = cacheServer.getHostnameForClients();
    cacheServerPort = cacheServer.getPort();
    return cacheServerPort;
  }

  public int getCacheServerPort() {
    return cacheServerPort;
  }

  public String getCacheServerHost() {
    return hostName;
  }

  @SuppressWarnings("rawtypes")
  public void setUpServerVM(final boolean cacheServerSslenabled) throws Exception {
    System.setProperty("javax.net.debug", "ssl,handshake");

    Properties gemFireProps = new Properties();

    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";
    boolean cacheServerSslRequireAuth = true;
    if (!useOldSSLSettings) {
      gemFireProps.put(SSL_ENABLED_COMPONENTS,
          SecurableCommunicationChannel.CLUSTER + "," + SecurableCommunicationChannel.SERVER);
      gemFireProps.put(SSL_PROTOCOLS, cacheServerSslprotocols);
      gemFireProps.put(SSL_CIPHERS, cacheServerSslciphers);
      gemFireProps.put(SSL_REQUIRE_AUTHENTICATION, String.valueOf(cacheServerSslRequireAuth));

      String keyStore =
          TestUtil.getResourcePath(CacheServerSSLConnectionDUnitTest.class, SERVER_KEY_STORE);
      String trustStore =
          TestUtil.getResourcePath(CacheServerSSLConnectionDUnitTest.class, SERVER_TRUST_STORE);
      gemFireProps.put(SSL_KEYSTORE_TYPE, "jks");
      gemFireProps.put(SSL_KEYSTORE, keyStore);
      gemFireProps.put(SSL_KEYSTORE_PASSWORD, "password");
      gemFireProps.put(SSL_TRUSTSTORE, trustStore);
      gemFireProps.put(SSL_TRUSTSTORE_PASSWORD, "password");
    } else {
      gemFireProps.put(CLUSTER_SSL_ENABLED, String.valueOf(cacheServerSslenabled));
      gemFireProps.put(CLUSTER_SSL_PROTOCOLS, cacheServerSslprotocols);
      gemFireProps.put(CLUSTER_SSL_CIPHERS, cacheServerSslciphers);
      gemFireProps.put(CLUSTER_SSL_REQUIRE_AUTHENTICATION,
          String.valueOf(cacheServerSslRequireAuth));

      String keyStore =
          TestUtil.getResourcePath(CacheServerSSLConnectionDUnitTest.class, SERVER_KEY_STORE);
      String trustStore =
          TestUtil.getResourcePath(CacheServerSSLConnectionDUnitTest.class, SERVER_TRUST_STORE);
      gemFireProps.put(CLUSTER_SSL_KEYSTORE_TYPE, "jks");
      gemFireProps.put(CLUSTER_SSL_KEYSTORE, keyStore);
      gemFireProps.put(CLUSTER_SSL_KEYSTORE_PASSWORD, "password");
      gemFireProps.put(CLUSTER_SSL_TRUSTSTORE, trustStore);
      gemFireProps.put(CLUSTER_SSL_TRUSTSTORE_PASSWORD, "password");
    }
    StringWriter sw = new StringWriter();
    PrintWriter writer = new PrintWriter(sw);
    gemFireProps.list(writer);
    System.out.println("Starting cacheserver ds with following properties \n" + sw);
    createCache(gemFireProps);

    RegionFactory factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region r = factory.create("serverRegion");
    r.put("serverkey", "servervalue");
  }

  public void setUpClientVM(String host, int port, boolean cacheServerSslenabled,
      boolean cacheServerSslRequireAuth, String keyStore, String trustStore, boolean subscription,
      boolean clientHasTrustedKeystore) {

    System.setProperty("javax.net.debug", "ssl,handshake");
    Properties gemFireProps = new Properties();

    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";

    String keyStorePath =
        TestUtil.getResourcePath(CacheServerSSLConnectionDUnitTest.class, keyStore);
    String trustStorePath =
        TestUtil.getResourcePath(CacheServerSSLConnectionDUnitTest.class, trustStore);

    if (cacheServerSslenabled) {
      if (useOldSSLSettings) {
        gemFireProps.put(SERVER_SSL_ENABLED, String.valueOf(cacheServerSslenabled));
        gemFireProps.put(SERVER_SSL_PROTOCOLS, cacheServerSslprotocols);
        gemFireProps.put(SERVER_SSL_CIPHERS, cacheServerSslciphers);
        gemFireProps.put(SERVER_SSL_REQUIRE_AUTHENTICATION,
            String.valueOf(cacheServerSslRequireAuth));
        if (clientHasTrustedKeystore) {
          gemFireProps.put(SERVER_SSL_KEYSTORE_TYPE, "jks");
          gemFireProps.put(SERVER_SSL_KEYSTORE, keyStorePath);
          gemFireProps.put(SERVER_SSL_KEYSTORE_PASSWORD, "password");
          gemFireProps.put(SERVER_SSL_TRUSTSTORE, trustStorePath);
          gemFireProps.put(SERVER_SSL_TRUSTSTORE_PASSWORD, "password");
        } else {
          gemFireProps.put(SERVER_SSL_KEYSTORE_TYPE, "jks");
          gemFireProps.put(SERVER_SSL_KEYSTORE, "");
          gemFireProps.put(SERVER_SSL_KEYSTORE_PASSWORD, "password");
          gemFireProps.put(SERVER_SSL_TRUSTSTORE, trustStorePath);
          gemFireProps.put(SERVER_SSL_TRUSTSTORE_PASSWORD, "password");
        }
      } else {
        gemFireProps.put(SSL_ENABLED_COMPONENTS, "server");
        gemFireProps.put(SSL_CIPHERS, cacheServerSslciphers);
        gemFireProps.put(SSL_PROTOCOLS, cacheServerSslprotocols);
        gemFireProps.put(SSL_REQUIRE_AUTHENTICATION, String.valueOf(cacheServerSslRequireAuth));
        if (clientHasTrustedKeystore) {
          gemFireProps.put(SSL_KEYSTORE_TYPE, "jks");
          gemFireProps.put(SSL_KEYSTORE, keyStorePath);
          gemFireProps.put(SSL_KEYSTORE_PASSWORD, "password");
          gemFireProps.put(SSL_TRUSTSTORE, trustStorePath);
          gemFireProps.put(SSL_TRUSTSTORE_PASSWORD, "password");
        } else {
          gemFireProps.put(SSL_KEYSTORE_TYPE, "jks");
          gemFireProps.put(SSL_KEYSTORE, "");
          gemFireProps.put(SSL_KEYSTORE_PASSWORD, "password");
          gemFireProps.put(SSL_TRUSTSTORE, trustStorePath);
          gemFireProps.put(SSL_TRUSTSTORE_PASSWORD, "password");
        }
      }
    }


    StringWriter sw = new StringWriter();
    PrintWriter writer = new PrintWriter(sw);
    gemFireProps.list(writer);
    System.out.println("Starting client ds with following properties \n" + sw.getBuffer());

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory(gemFireProps);
    clientCacheFactory.setPoolSubscriptionEnabled(subscription).addPoolServer(host, port);
    clientCache = clientCacheFactory.create();

    ClientRegionFactory<String, String> regionFactory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
    Region<String, String> region = regionFactory.create("serverRegion");
    assertNotNull(region);
  }

  public void doClientRegionTest() {
    Region<String, String> region = clientCache.getRegion("serverRegion");
    assertEquals("servervalue", region.get("serverkey"));
    region.put("clientkey", "clientvalue");
    assertEquals("clientvalue", region.get("clientkey"));
  }

  public void doServerRegionTest() {
    Region<String, String> region = cache.getRegion("serverRegion");
    assertEquals("servervalue", region.get("serverkey"));
    assertEquals("clientvalue", region.get("clientkey"));
  }


  public static void setUpServerVMTask(boolean cacheServerSslenabled) throws Exception {
    instance.setUpServerVM(cacheServerSslenabled);
  }

  public static int createServerTask() throws Exception {
    return instance.createServer();
  }

  public static void setUpClientVMTask(String host, int port, boolean cacheServerSslenabled,
      boolean cacheServerSslRequireAuth, String keyStore, String trustStore,
      boolean clientHasTrustedKeystore) throws Exception {
    instance.setUpClientVM(host, port, cacheServerSslenabled, cacheServerSslRequireAuth, keyStore,
        trustStore, true, clientHasTrustedKeystore);
  }

  public static void setUpClientVMTaskNoSubscription(String host, int port,
      boolean cacheServerSslenabled, boolean cacheServerSslRequireAuth, String keyStore,
      String trustStore) throws Exception {
    instance.setUpClientVM(host, port, cacheServerSslenabled, cacheServerSslRequireAuth, keyStore,
        trustStore, false, true);
  }

  public static void doClientRegionTestTask() {
    instance.doClientRegionTest();
  }

  public static void verifyServerDoesNotReceiveClientUpdate() {
    instance.doVerifyServerDoesNotReceiveClientUpdate();
  }

  private void doVerifyServerDoesNotReceiveClientUpdate() {
    Region<String, String> region = cache.getRegion("serverRegion");
    assertFalse(region.containsKey("clientkey"));
  }

  public static void doServerRegionTestTask() {
    instance.doServerRegionTest();
  }

  public static Object[] getCacheServerEndPointTask() { // TODO: avoid Object[]
    Object[] array = new Object[2];
    array[0] = instance.getCacheServerHost();
    array[1] = instance.getCacheServerPort();
    return array;
  }

  public static void closeCacheTask() {
    if (instance != null && instance.cache != null) {
      instance.cache.close();
    }
  }

  public static void closeClientCacheTask() {
    if (instance != null && instance.clientCache != null) {
      instance.clientCache.close();
    }
  }

  @Test
  public void testCacheServerSSL() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled));
    int port = serverVM.invoke(() -> createServerTask());

    String hostName = host.getHostName();

    clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled,
        cacheClientSslRequireAuth, CLIENT_KEY_STORE, CLIENT_TRUST_STORE, true));
    clientVM.invoke(() -> doClientRegionTestTask());
    serverVM.invoke(() -> doServerRegionTestTask());
  }

  /**
   * GEODE-2898: A non-responsive SSL client can block a server's "acceptor" thread
   * <p>
   * Start a server and then connect to it without completing the SSL handshake
   * </p>
   * <p>
   * Attempt to connect to the server using a real SSL client, demonstrating that the server is not
   * blocked and can process the new connection request.
   * </p>
   */
  @Test
  public void clientSlowToHandshakeDoesNotBlockServer() throws Throwable {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    VM slowClientVM = host.getVM(3);
    getBlackboard().initBlackboard();

    // a plain-text socket is used to connect to an ssl server & the handshake
    // is never performed. The server will log this exception & it should be ignored
    IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException", serverVM);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled));
    int port = serverVM.invoke(() -> createServerTask());

    String hostName = host.getHostName();

    AsyncInvocation slowAsync = slowClientVM.invokeAsync(() -> connectToServer(hostName, port));
    try {
      getBlackboard().waitForGate("serverIsBlocked", 60, TimeUnit.SECONDS);

      clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled,
          cacheClientSslRequireAuth, CLIENT_KEY_STORE, CLIENT_TRUST_STORE, true));
      clientVM.invoke(() -> doClientRegionTestTask());
      serverVM.invoke(() -> doServerRegionTestTask());

    } finally {
      getBlackboard().signalGate("testIsCompleted");
      try {
        if (slowAsync.isAlive()) {
          slowAsync.join(60000);
        }
        if (slowAsync.exceptionOccurred()) {
          throw slowAsync.getException();
        }
      } finally {
        assertFalse(slowAsync.isAlive());
      }
    }

  }

  private void connectToServer(String hostName, int port) throws Exception {
    Socket sock = new Socket();
    sock.connect(new InetSocketAddress(hostName, port));
    try {
      getBlackboard().signalGate("serverIsBlocked");
      getBlackboard().waitForGate("testIsCompleted", 60, TimeUnit.SECONDS);
    } finally {
      sock.close();
    }
  }

  @Test
  public void testNonSSLClient() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = false;
    boolean cacheClientSslRequireAuth = true;

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled));
    serverVM.invoke(() -> createServerTask());

    Object array[] = (Object[]) serverVM.invoke(() -> getCacheServerEndPointTask());
    String hostName = (String) array[0];
    int port = (Integer) array[1];

    IgnoredException expect =
        IgnoredException.addIgnoredException("javax.net.ssl.SSLException", serverVM);
    IgnoredException expect2 = IgnoredException.addIgnoredException("IOException", serverVM);
    try {
      clientVM.invoke(() -> setUpClientVMTaskNoSubscription(hostName, port, cacheClientSslenabled,
          cacheClientSslRequireAuth, TRUSTED_STORE, TRUSTED_STORE));
      clientVM.invoke(() -> doClientRegionTestTask());
      serverVM.invoke(() -> doServerRegionTestTask());
      fail("Test should fail as non-ssl client is trying to connect to ssl configured server");

    } catch (Exception rmiException) {
      Throwable e = rmiException.getCause();
      // getLogWriter().info("ExceptionCause at clientVM " + e);
      if (e instanceof org.apache.geode.cache.client.ServerOperationException) {
        Throwable t = e.getCause();
        // getLogWriter().info("Cause is " + t);
        assertTrue(t instanceof org.apache.geode.security.AuthenticationRequiredException);
      } else {
        // getLogWriter().error("Unexpected exception ", e);
        fail("Unexpected Exception: " + e + " expected: " + AuthenticationRequiredException.class);
      }
    } finally {
      expect.remove();
      expect2.remove();
    }
  }

  @Test
  public void testSSLClientWithNoAuth() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = false;

    IgnoredException.addIgnoredException("SSLHandshakeException");
    IgnoredException.addIgnoredException("ValidatorException");

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled));
    serverVM.invoke(() -> createServerTask());

    Object array[] = (Object[]) serverVM.invoke(() -> getCacheServerEndPointTask());
    String hostName = (String) array[0];
    int port = (Integer) array[1];

    clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled,
        cacheClientSslRequireAuth, CLIENT_KEY_STORE, CLIENT_TRUST_STORE, true));
    clientVM.invoke(() -> CacheServerSSLConnectionDUnitTest.doClientRegionTestTask());
    serverVM.invoke(() -> CacheServerSSLConnectionDUnitTest.doServerRegionTestTask());
  }

  @Test
  public void untrustedClientIsRejected() throws Throwable {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = false;

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled));
    serverVM.invoke(() -> createServerTask());

    Object array[] = (Object[]) serverVM.invoke(() -> getCacheServerEndPointTask());
    String hostName = (String) array[0];
    int port = (Integer) array[1];

    IgnoredException.addIgnoredException("SSLHandshakeException");

    clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled,
        cacheClientSslRequireAuth, "default.keystore", CLIENT_TRUST_STORE, false));

    try {
      clientVM.invoke(() -> CacheServerSSLConnectionDUnitTest.doClientRegionTestTask());
      fail("client should not have been able to execute a cache operation");
    } catch (RMIException e) {
      assertTrue("expected a NoAvailableServersException but received " + e.getCause(),
          e.getCause() instanceof NoAvailableServersException);
    }
    serverVM
        .invoke(() -> CacheServerSSLConnectionDUnitTest.verifyServerDoesNotReceiveClientUpdate());
  }

  @Test
  public void testSSLClientWithNonSSLServer() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);

    boolean cacheServerSslenabled = false;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled));
    serverVM.invoke(() -> createServerTask());

    Object array[] = (Object[]) serverVM.invoke(() -> getCacheServerEndPointTask());
    String hostName = (String) array[0];
    int port = (Integer) array[1];

    IgnoredException expect =
        IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException", serverVM);
    try {
      clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled,
          cacheClientSslRequireAuth, TRUSTED_STORE, TRUSTED_STORE, true));
      clientVM.invoke(() -> doClientRegionTestTask());
      serverVM.invoke(() -> doServerRegionTestTask());
      fail(
          "Test should fail as ssl client with ssl enabled is trying to connect to server with ssl disabled");

    } catch (Exception rmiException) {
      // ignore
    } finally {
      expect.remove();
    }
  }

  @Override
  public final void preTearDown() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    clientVM.invoke(() -> closeClientCacheTask());
    serverVM.invoke(() -> closeCacheTask());

  }
}
