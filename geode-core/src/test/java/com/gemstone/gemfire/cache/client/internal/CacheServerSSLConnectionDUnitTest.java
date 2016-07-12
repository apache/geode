/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.client.internal;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.net.SocketCreatorFactory;
import com.gemstone.gemfire.security.AuthenticationRequiredException;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.util.test.TestUtil;

/**
 * Tests cacheserver ssl support added. See https://svn.gemstone.com/trac/gemfire/ticket/48995 for details
 */
@Category(DistributedTest.class)
public class CacheServerSSLConnectionDUnitTest extends JUnit4DistributedTestCase {

  private static final String TRUSTED_STORE = "trusted.keystore";
  private static final String CLIENT_KEY_STORE = "client.keystore";
  private static final String CLIENT_TRUST_STORE = "client.truststore";
  private static final String SERVER_KEY_STORE = "cacheserver.keystore";
  private static final String SERVER_TRUST_STORE = "cacheserver.truststore";

  private static CacheServerSSLConnectionDUnitTest instance = new CacheServerSSLConnectionDUnitTest(); // TODO: memory leak

  private Cache cache;
  private CacheServer cacheServer;
  private ClientCache clientCache;
  private int cacheServerPort;
  private String hostName;

  @Override
  public final void preSetUp() throws Exception {
    disconnectAllFromDS();
  }

  public Cache createCache(Properties props) throws Exception
  {
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    cache = new CacheFactory(props).create();
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }

  private int createServer() throws IOException{
    cacheServer = cache.addCacheServer();
    cacheServer.setPort(0);
    cacheServer.start();
    hostName = cacheServer.getHostnameForClients();
    cacheServerPort = cacheServer.getPort();
    return cacheServerPort;
  }

  public int getCacheServerPort(){
    return cacheServerPort;
  }

  public String getCacheServerHost(){
    return hostName;
  }

  public void stopCacheServer(){
    this.cacheServer.stop();
  }

  @SuppressWarnings("rawtypes")
  public void setUpServerVM(boolean cacheServerSslenabled) throws Exception {
    Properties gemFireProps = new Properties();

    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";
    boolean cacheServerSslRequireAuth = true;
    gemFireProps.put(SERVER_SSL_ENABLED, String.valueOf(cacheServerSslenabled));
    gemFireProps.put(SERVER_SSL_PROTOCOLS, cacheServerSslprotocols);
    gemFireProps.put(SERVER_SSL_CIPHERS, cacheServerSslciphers);
    gemFireProps.put(SERVER_SSL_REQUIRE_AUTHENTICATION, String.valueOf(cacheServerSslRequireAuth));

    String keyStore = TestUtil.getResourcePath(CacheServerSSLConnectionDUnitTest.class, SERVER_KEY_STORE);
    String trustStore = TestUtil.getResourcePath(CacheServerSSLConnectionDUnitTest.class, SERVER_TRUST_STORE);
    gemFireProps.put(SERVER_SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.put(SERVER_SSL_KEYSTORE, keyStore);
    gemFireProps.put(SERVER_SSL_KEYSTORE_PASSWORD, "password");
    gemFireProps.put(SERVER_SSL_TRUSTSTORE, trustStore);
    gemFireProps.put(SERVER_SSL_TRUSTSTORE_PASSWORD, "password");

    StringWriter sw = new StringWriter();
    PrintWriter writer = new PrintWriter(sw);
    gemFireProps.list(writer);
    System.out.println("Starting cacheserver ds with following properties \n" + sw);
    createCache(gemFireProps);

    RegionFactory factory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    Region r = factory.create("serverRegion");
    r.put("serverkey", "servervalue");
  }

  public void setUpClientVM(String host, int port,
                            boolean cacheServerSslenabled, boolean cacheServerSslRequireAuth,
                            String keyStore, String trustStore, boolean subscription) {

    Properties gemFireProps = new Properties();

    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";

    String keyStorePath = TestUtil.getResourcePath(CacheServerSSLConnectionDUnitTest.class, keyStore);
    String trustStorePath = TestUtil.getResourcePath(CacheServerSSLConnectionDUnitTest.class, trustStore);
    //using new server-ssl-* properties
    gemFireProps.put(SERVER_SSL_ENABLED, String.valueOf(cacheServerSslenabled));
    gemFireProps.put(SERVER_SSL_PROTOCOLS, cacheServerSslprotocols);
    gemFireProps.put(SERVER_SSL_CIPHERS, cacheServerSslciphers);
    gemFireProps.put(SERVER_SSL_REQUIRE_AUTHENTICATION, String.valueOf(cacheServerSslRequireAuth));

    gemFireProps.put(SERVER_SSL_KEYSTORE_TYPE, "jks");
    gemFireProps.put(SERVER_SSL_KEYSTORE, keyStorePath);
    gemFireProps.put(SERVER_SSL_KEYSTORE_PASSWORD, "password");
    gemFireProps.put(SERVER_SSL_TRUSTSTORE, trustStorePath);
    gemFireProps.put(SERVER_SSL_TRUSTSTORE_PASSWORD, "password");

    StringWriter sw = new StringWriter();
    PrintWriter writer = new PrintWriter(sw);
    gemFireProps.list(writer);
    System.out.println("Starting client ds with following properties \n" + sw.getBuffer());

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory(gemFireProps);
    clientCacheFactory.setPoolSubscriptionEnabled(subscription).addPoolServer(host, port);
    clientCache = clientCacheFactory.create();

    ClientRegionFactory<String,String> regionFactory = clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);
    Region<String, String> region = regionFactory.create("serverRegion");
    assertNotNull(region);
  }

  public void doClientRegionTest(){
    Region<String, String> region = clientCache.getRegion("serverRegion");
    assertEquals("servervalue",region.get("serverkey"));
    region.put("clientkey", "clientvalue");
    assertEquals("clientvalue",region.get("clientkey"));
  }

  public void doServerRegionTest(){
    Region<String, String> region = cache.getRegion("serverRegion");
    assertEquals("servervalue",region.get("serverkey"));
    assertEquals("clientvalue",region.get("clientkey"));
  }


  public static void setUpServerVMTask(boolean cacheServerSslenabled) throws Exception{
    instance.setUpServerVM(cacheServerSslenabled);
  }

  public static int createServerTask() throws Exception {
    return instance.createServer();
  }

  public static void setUpClientVMTask(String host, int port,
                                       boolean cacheServerSslenabled, boolean cacheServerSslRequireAuth, String keyStore, String trustStore)
          throws Exception {
    instance.setUpClientVM(host, port, cacheServerSslenabled,
            cacheServerSslRequireAuth, keyStore, trustStore, true);
  }
  public static void setUpClientVMTaskNoSubscription(String host, int port,
                                                     boolean cacheServerSslenabled, boolean cacheServerSslRequireAuth, String keyStore, String trustStore)
          throws Exception {
    instance.setUpClientVM(host, port, cacheServerSslenabled,
            cacheServerSslRequireAuth, keyStore, trustStore, false);
  }

  public static void doClientRegionTestTask() {
    instance.doClientRegionTest();
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

  public static void closeCacheTask(){
    if (instance != null && instance.cache != null) {
      instance.cache.close();
      SocketCreatorFactory.close();
    }
  }

  public static void closeClientCacheTask(){
    if (instance != null && instance.clientCache != null) {
      instance.clientCache.close();
      SocketCreatorFactory.close();
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

    clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled, cacheClientSslRequireAuth, CLIENT_KEY_STORE, CLIENT_TRUST_STORE));
    clientVM.invoke(() -> doClientRegionTestTask());
    serverVM.invoke(() -> doServerRegionTestTask());
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

    Object array[] = (Object[])serverVM.invoke(() -> getCacheServerEndPointTask());
    String hostName = (String)array[0];
    int port = (Integer) array[1];

    IgnoredException expect = IgnoredException.addIgnoredException("javax.net.ssl.SSLException", serverVM);
    IgnoredException expect2 = IgnoredException.addIgnoredException("IOException", serverVM);
    try{
      clientVM.invoke(() -> setUpClientVMTaskNoSubscription(hostName, port, cacheClientSslenabled, cacheClientSslRequireAuth, TRUSTED_STORE, TRUSTED_STORE));
      clientVM.invoke(() -> doClientRegionTestTask());
      serverVM.invoke(() -> doServerRegionTestTask());
      fail("Test should fail as non-ssl client is trying to connect to ssl configured server");

    } catch (Exception rmiException) {
      Throwable e = rmiException.getCause();
      //getLogWriter().info("ExceptionCause at clientVM " + e);
      if (e instanceof com.gemstone.gemfire.cache.client.ServerOperationException) {
        Throwable t = e.getCause();
        //getLogWriter().info("Cause is " + t);
        assertTrue(t instanceof com.gemstone.gemfire.security.AuthenticationRequiredException);
      } else {
        //getLogWriter().error("Unexpected exception ", e);
        fail("Unexpected Exception: " + e + " expected: "
                + AuthenticationRequiredException.class);
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

    serverVM.invoke(() -> setUpServerVMTask(cacheServerSslenabled));
    serverVM.invoke(() -> createServerTask());

    Object array[] = (Object[])serverVM.invoke(() -> getCacheServerEndPointTask());
    String hostName = (String)array[0];
    int port = (Integer) array[1];

    try {
      clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled, cacheClientSslRequireAuth, CLIENT_KEY_STORE, CLIENT_TRUST_STORE));
      clientVM.invoke(() -> CacheServerSSLConnectionDUnitTest.doClientRegionTestTask());
      serverVM.invoke(() -> CacheServerSSLConnectionDUnitTest.doServerRegionTestTask());

    } catch (Exception rmiException) {
      Throwable e = rmiException.getCause();
      //getLogWriter().info("ExceptionCause at clientVM " + e);
      if (e instanceof com.gemstone.gemfire.cache.client.ServerOperationException) {
        Throwable t = e.getCause();
        //getLogWriter().info("Cause is " + t);
        assertTrue(t instanceof com.gemstone.gemfire.security.AuthenticationRequiredException);
      } else {
        //getLogWriter().error("Unexpected exception ", e);
        fail("Unexpected Exception...expected "
                + AuthenticationRequiredException.class);
      }
    }
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

    Object array[] = (Object[])serverVM.invoke(() -> getCacheServerEndPointTask());
    String hostName = (String)array[0];
    int port = (Integer) array[1];

    IgnoredException expect = IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException", serverVM);
    try{
      clientVM.invoke(() -> setUpClientVMTask(hostName, port, cacheClientSslenabled, cacheClientSslRequireAuth, TRUSTED_STORE, TRUSTED_STORE));
      clientVM.invoke(() -> doClientRegionTestTask());
      serverVM.invoke(() -> doServerRegionTestTask());
      fail("Test should fail as ssl client with ssl enabled is trying to connect to server with ssl disabled");

    } catch (Exception rmiException) {
      //ignore
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

