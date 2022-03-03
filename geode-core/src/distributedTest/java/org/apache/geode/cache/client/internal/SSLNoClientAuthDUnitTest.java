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
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category(MembershipTest.class)
public class SSLNoClientAuthDUnitTest extends JUnit4DistributedTestCase {

  private Cache cache;
  private CacheServer cacheServer;
  private ClientCache clientCache;
  private int cacheServerPort;
  private String hostName;

  private static final String DEFAULT_STORE = "default.keystore";

  private static final SSLNoClientAuthDUnitTest instance = new SSLNoClientAuthDUnitTest();

  @Before
  public void setUp() {
    disconnectAllFromDS();
  }

  @After
  public void tearDown() {
    VM serverVM = getVM(1);
    VM clientVM = getVM(2);

    clientVM.invoke(SSLNoClientAuthDUnitTest::closeClientCacheTask);
    serverVM.invoke(SSLNoClientAuthDUnitTest::closeCacheTask);
  }

  @Test
  public void testSSLServerWithNoAuth() {
    VM serverVM = getVM(1);
    VM clientVM = getVM(2);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled));
    serverVM.invoke(SSLNoClientAuthDUnitTest::createServerTask);

    Object[] array = serverVM.invoke(SSLNoClientAuthDUnitTest::getCacheServerEndPointTask);
    String hostName = (String) array[0];
    int port = (Integer) array[1];
    Object[] params = new Object[6];
    params[0] = hostName;
    params[1] = port;
    params[2] = cacheClientSslenabled;
    params[3] = cacheClientSslRequireAuth;
    params[4] = DEFAULT_STORE;
    params[5] = DEFAULT_STORE;

    clientVM.invoke(() -> setUpClientVMTask(hostName, port,
        cacheClientSslenabled, cacheClientSslRequireAuth, DEFAULT_STORE, DEFAULT_STORE));
    clientVM.invoke(SSLNoClientAuthDUnitTest::doClientRegionTestTask);
    serverVM.invoke(SSLNoClientAuthDUnitTest::doServerRegionTestTask);
  }

  private void createCache(Properties props) throws Exception {
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    cache = new CacheFactory(props).create();
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
  }

  private void createServer() throws IOException {
    cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer = cache.addCacheServer();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();
    hostName = cacheServer.getHostnameForClients();
  }

  private int getCacheServerPort() {
    return cacheServerPort;
  }

  private String getCacheServerHost() {
    return hostName;
  }

  private void setUpServerVM(boolean cacheServerSslenabled) throws Exception {
    Properties gemFireProps = new Properties();

    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";
    boolean cacheServerSslRequireAuth = false;

    gemFireProps.setProperty(SERVER_SSL_ENABLED, String.valueOf(cacheServerSslenabled));
    gemFireProps.setProperty(SERVER_SSL_PROTOCOLS, cacheServerSslprotocols);
    gemFireProps.setProperty(SERVER_SSL_CIPHERS, cacheServerSslciphers);
    gemFireProps
        .setProperty(SERVER_SSL_REQUIRE_AUTHENTICATION, String.valueOf(cacheServerSslRequireAuth));

    String keyStore =
        createTempFileFromResource(SSLNoClientAuthDUnitTest.class, DEFAULT_STORE)
            .getAbsolutePath();
    String trustStore =
        createTempFileFromResource(SSLNoClientAuthDUnitTest.class, DEFAULT_STORE)
            .getAbsolutePath();
    gemFireProps.setProperty(SERVER_SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.setProperty(SERVER_SSL_KEYSTORE, keyStore);
    gemFireProps.setProperty(SERVER_SSL_KEYSTORE_PASSWORD, "password");
    gemFireProps.setProperty(SERVER_SSL_TRUSTSTORE, trustStore);
    gemFireProps.setProperty(SERVER_SSL_TRUSTSTORE_PASSWORD, "password");

    StringWriter sw = new StringWriter();
    PrintWriter writer = new PrintWriter(sw);
    gemFireProps.list(writer);
    createCache(gemFireProps);

    RegionFactory factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region r = factory.create("serverRegion");
    r.put("serverkey", "servervalue");
  }

  private void setUpClientVM(String host, int port, boolean cacheServerSslenabled,
      boolean cacheServerSslRequireAuth, String keyStore, String trustStore) {
    Properties gemFireProps = new Properties();

    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";

    String keyStorePath =
        createTempFileFromResource(SSLNoClientAuthDUnitTest.class, keyStore)
            .getAbsolutePath();
    String trustStorePath =
        createTempFileFromResource(SSLNoClientAuthDUnitTest.class, trustStore)
            .getAbsolutePath();
    // using new server-ssl-* properties
    gemFireProps.setProperty(SERVER_SSL_ENABLED, String.valueOf(cacheServerSslenabled));
    gemFireProps.setProperty(SERVER_SSL_PROTOCOLS, cacheServerSslprotocols);
    gemFireProps.setProperty(SERVER_SSL_CIPHERS, cacheServerSslciphers);
    gemFireProps
        .setProperty(SERVER_SSL_REQUIRE_AUTHENTICATION, String.valueOf(cacheServerSslRequireAuth));

    gemFireProps.setProperty(SERVER_SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.setProperty(SERVER_SSL_KEYSTORE, keyStorePath);
    gemFireProps.setProperty(SERVER_SSL_KEYSTORE_PASSWORD, "password");
    gemFireProps.setProperty(SERVER_SSL_TRUSTSTORE, trustStorePath);
    gemFireProps.setProperty(SERVER_SSL_TRUSTSTORE_PASSWORD, "password");

    StringWriter sw = new StringWriter();
    PrintWriter writer = new PrintWriter(sw);
    gemFireProps.list(writer);

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory(gemFireProps);
    clientCacheFactory.addPoolServer(host, port);
    clientCache = clientCacheFactory.create();

    ClientRegionFactory<String, String> regionFactory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
    Region<String, String> region = regionFactory.create("serverRegion");
    assertNotNull(region);
  }

  private void doClientRegionTest() {
    Region<String, String> region = clientCache.getRegion("serverRegion");
    assertEquals("servervalue", region.get("serverkey"));
    region.put("clientkey", "clientvalue");
    assertEquals("clientvalue", region.get("clientkey"));
  }

  private void doServerRegionTest() {
    Region<String, String> region = cache.getRegion("serverRegion");
    assertEquals("servervalue", region.get("serverkey"));
    assertEquals("clientvalue", region.get("clientkey"));
  }

  private static void setUpServerVMTask(boolean cacheServerSslenabled) throws Exception {
    instance.setUpServerVM(cacheServerSslenabled);
  }

  private static void createServerTask() throws Exception {
    instance.createServer();
  }

  private static void setUpClientVMTask(String host, int port, boolean cacheServerSslenabled,
      boolean cacheServerSslRequireAuth, String keyStore, String trustStore) {
    instance.setUpClientVM(host, port, cacheServerSslenabled, cacheServerSslRequireAuth, keyStore,
        trustStore);
  }

  private static void doClientRegionTestTask() {
    instance.doClientRegionTest();
  }

  private static void doServerRegionTestTask() {
    instance.doServerRegionTest();
  }

  private static Object[] getCacheServerEndPointTask() {
    Object[] array = new Object[2];
    array[0] = instance.getCacheServerHost();
    array[1] = instance.getCacheServerPort();
    return array;
  }

  private static void closeCacheTask() {
    if (instance != null && instance.cache != null) {
      instance.cache.close();
    }
  }

  private static void closeClientCacheTask() {
    if (instance != null && instance.clientCache != null) {
      instance.clientCache.close();
    }
  }
}
