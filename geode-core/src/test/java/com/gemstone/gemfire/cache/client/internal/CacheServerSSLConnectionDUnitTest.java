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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
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
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.security.AuthenticationRequiredException;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.util.test.TestUtil;

/**
 * Tests cacheserver ssl support added. See https://svn.gemstone.com/trac/gemfire/ticket/48995 for details
 * @author tushark
 *
 */
public class CacheServerSSLConnectionDUnitTest extends DistributedTestCase {
  
  private static final long serialVersionUID = 1L;
  private Cache cache;
  private CacheServer cacheServer;
  private ClientCache clientCache;
  private int cacheServerPort;
  private String hostName;
  
  private static final String TRUSTED_STORE = "trusted.keystore";
  private static final String CLIENT_KEY_STORE = "client.keystore";
  private static final String CLIENT_TRUST_STORE = "client.truststore";
  private static final String SERVER_KEY_STORE = "cacheserver.keystore";
  private static final String SERVER_TRUST_STORE = "cacheserver.truststore";
  
  private static CacheServerSSLConnectionDUnitTest instance = new CacheServerSSLConnectionDUnitTest("CacheServerSSLConnectionDUnit");
  
  
  public void setUp() throws Exception {
    disconnectAllFromDS();
    super.setUp();
  }

  public CacheServerSSLConnectionDUnitTest(String name) {
    super(name);
  }  

  public Cache createCache(Properties props) throws Exception
  {
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    cache = new CacheFactory(props).create();
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }
  
  private void createServer() throws IOException{
    cacheServerPort = AvailablePortHelper.getRandomAvailableTCPPort();
    cacheServer = cache.addCacheServer();
    cacheServer.setPort(cacheServerPort);
    cacheServer.start();
    hostName = cacheServer.getHostnameForClients();
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
    gemFireProps.put(DistributionConfig.SERVER_SSL_ENABLED_NAME,
        String.valueOf(cacheServerSslenabled));
    gemFireProps.put(DistributionConfig.SERVER_SSL_PROTOCOLS_NAME,
        cacheServerSslprotocols);
    gemFireProps.put(DistributionConfig.SERVER_SSL_CIPHERS_NAME,
        cacheServerSslciphers);
    gemFireProps.put(
        DistributionConfig.SERVER_SSL_REQUIRE_AUTHENTICATION_NAME,
        String.valueOf(cacheServerSslRequireAuth));

    String keyStore = TestUtil.getResourcePath(CacheServerSSLConnectionDUnitTest.class, SERVER_KEY_STORE);
    String trustStore = TestUtil.getResourcePath(CacheServerSSLConnectionDUnitTest.class, SERVER_TRUST_STORE);
    gemFireProps.put(DistributionConfig.SERVER_SSL_KEYSTORE_TYPE_NAME, "jks");
    gemFireProps.put(DistributionConfig.SERVER_SSL_KEYSTORE_NAME, keyStore);
    gemFireProps.put(DistributionConfig.SERVER_SSL_KEYSTORE_PASSWORD_NAME, "password");
    gemFireProps.put(DistributionConfig.SERVER_SSL_TRUSTSTORE_NAME, trustStore);
    gemFireProps.put(DistributionConfig.SERVER_SSL_TRUSTSTORE_PASSWORD_NAME, "password");
    
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
    gemFireProps.put(DistributionConfig.SERVER_SSL_ENABLED_NAME,
        String.valueOf(cacheServerSslenabled));
    gemFireProps.put(DistributionConfig.SERVER_SSL_PROTOCOLS_NAME,
        cacheServerSslprotocols);
    gemFireProps.put(DistributionConfig.SERVER_SSL_CIPHERS_NAME,
        cacheServerSslciphers);
    gemFireProps.put(
        DistributionConfig.SERVER_SSL_REQUIRE_AUTHENTICATION_NAME,
        String.valueOf(cacheServerSslRequireAuth));

    gemFireProps.put(DistributionConfig.SERVER_SSL_KEYSTORE_TYPE_NAME, "jks");
    gemFireProps.put(DistributionConfig.SERVER_SSL_KEYSTORE_NAME, keyStorePath);
    gemFireProps.put(DistributionConfig.SERVER_SSL_KEYSTORE_PASSWORD_NAME, "password");
    gemFireProps.put(DistributionConfig.SERVER_SSL_TRUSTSTORE_NAME, trustStorePath);
    gemFireProps.put(DistributionConfig.SERVER_SSL_TRUSTSTORE_PASSWORD_NAME, "password");

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
  
  public static void createServerTask() throws Exception {
    instance.createServer();
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
  
  public static Object[] getCacheServerEndPointTask() {
    Object[] array = new Object[2];
    array[0] = instance.getCacheServerHost();
    array[1] = instance.getCacheServerPort();
    return array;
  }
  
  public static void closeCacheTask(){
    if (instance != null && instance.cache != null) {
      instance.cache.close();
    }
  }
  
  public static void closeClientCacheTask(){
    if (instance != null && instance.clientCache != null) {
      instance.clientCache.close();
    }
  }
  
  public void testCacheServerSSL() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    
    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;
    
    serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "setUpServerVMTask", new Object[]{cacheServerSslenabled});
    serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "createServerTask");
    
    Object array[] = (Object[])serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "getCacheServerEndPointTask"); 
    String hostName = (String)array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[6];
    params[0] = hostName;
    params[1] = port;
    params[2] = cacheClientSslenabled;
    params[3] = cacheClientSslRequireAuth;
    params[4] = CLIENT_KEY_STORE;
    params[5] = CLIENT_TRUST_STORE;
    //getLogWriter().info("Starting client with server endpoint " + hostName + ":" + port);
    clientVM.invoke(CacheServerSSLConnectionDUnitTest.class, "setUpClientVMTask", params);
    clientVM.invoke(CacheServerSSLConnectionDUnitTest.class, "doClientRegionTestTask");
    serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "doServerRegionTestTask");
    
  }
  
  
  public void testNonSSLClient() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    
    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = false;
    boolean cacheClientSslRequireAuth = true;
    
    serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "setUpServerVMTask", new Object[]{cacheServerSslenabled});
    serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "createServerTask");
    
    Object array[] = (Object[])serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "getCacheServerEndPointTask"); 
    String hostName = (String)array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[6];
    params[0] = hostName;
    params[1] = port;
    params[2] = cacheClientSslenabled;
    params[3] = cacheClientSslRequireAuth;
    params[4] = TRUSTED_STORE;
    params[5] = TRUSTED_STORE;
    IgnoredException expect = IgnoredException.addIgnoredException("javax.net.ssl.SSLException", serverVM);
    IgnoredException expect2 = IgnoredException.addIgnoredException("IOException", serverVM);
    try{
      //getLogWriter().info("Starting client with server endpoint " + hostName + ":" + port);    
      clientVM.invoke(CacheServerSSLConnectionDUnitTest.class, "setUpClientVMTaskNoSubscription", params);
      clientVM.invoke(CacheServerSSLConnectionDUnitTest.class, "doClientRegionTestTask");
      serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "doServerRegionTestTask");
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
  
  public void testSSLClientWithNoAuth() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = false;

    serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "setUpServerVMTask", new Object[]{cacheServerSslenabled});
    serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "createServerTask");

    Object array[] = (Object[])serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "getCacheServerEndPointTask"); 
    String hostName = (String)array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[6];
    params[0] = hostName;
    params[1] = port;
    params[2] = cacheClientSslenabled;
    params[3] = cacheClientSslRequireAuth;
    params[4] = CLIENT_KEY_STORE;
    params[5] = CLIENT_TRUST_STORE;
    //getLogWriter().info("Starting client with server endpoint " + hostName + ":" + port);
    try {
      clientVM.invoke(CacheServerSSLConnectionDUnitTest.class, "setUpClientVMTask", params);
      clientVM.invoke(CacheServerSSLConnectionDUnitTest.class, "doClientRegionTestTask");
      serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "doServerRegionTestTask");
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
  
  public void testSSLClientWithNonSSLServer() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    
    boolean cacheServerSslenabled = false;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;
    
    serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "setUpServerVMTask", new Object[]{cacheServerSslenabled});
    serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "createServerTask");
    
    Object array[] = (Object[])serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "getCacheServerEndPointTask"); 
    String hostName = (String)array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[6];
    params[0] = hostName;
    params[1] = port;
    params[2] = cacheClientSslenabled;
    params[3] = cacheClientSslRequireAuth;
    params[4] = TRUSTED_STORE;
    params[5] = TRUSTED_STORE;
    IgnoredException expect = IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException", serverVM);
    try{
      //getLogWriter().info("Starting client with server endpoint " + hostName + ":" + port);    
      clientVM.invoke(CacheServerSSLConnectionDUnitTest.class, "setUpClientVMTask", params);
      clientVM.invoke(CacheServerSSLConnectionDUnitTest.class, "doClientRegionTestTask");
      serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "doServerRegionTestTask");
      fail("Test should fail as ssl client with ssl enabled is trying to connect to server with ssl disabled");
    }catch (Exception rmiException) {
      
      //ignore
      
      /*Throwable e = rmiException.getCause();
      getLogWriter().info("ExceptionCause at clientVM " + e);
      if (e instanceof com.gemstone.gemfire.cache.client.ServerOperationException) {
        Throwable t = e.getCause();
        getLogWriter().info("Cause is " + t);
        assertTrue(t instanceof com.gemstone.gemfire.security.AuthenticationRequiredException);
      } else {
        getLogWriter().error("Unexpected exception ", e);
        fail("Unexpected Exception...expected "
            + AuthenticationRequiredException.class);
      }*/
    } finally {
      expect.remove();
    }
  }
  
  @Override
  protected final void preTearDown() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    clientVM.invoke(CacheServerSSLConnectionDUnitTest.class, "closeClientCacheTask");
    serverVM.invoke(CacheServerSSLConnectionDUnitTest.class, "closeCacheTask");
  }
}

