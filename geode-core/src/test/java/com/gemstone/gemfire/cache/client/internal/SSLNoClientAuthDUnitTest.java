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

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
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
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.util.test.TestUtil;

/**
 * Test for GEODE-396
 */
@Category(DistributedTest.class)
public class SSLNoClientAuthDUnitTest extends JUnit4DistributedTestCase {
  
  private Cache cache;
  private CacheServer cacheServer;
  private ClientCache clientCache;
  private int cacheServerPort;
  private String hostName;
  
  private static final String DEFAULT_STORE = "default.keystore";
  
  private static SSLNoClientAuthDUnitTest instance = new SSLNoClientAuthDUnitTest();
  
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
    boolean cacheServerSslRequireAuth = false;
    gemFireProps.put(SERVER_SSL_ENABLED,
        String.valueOf(cacheServerSslenabled));
    gemFireProps.put(SERVER_SSL_PROTOCOLS,
        cacheServerSslprotocols);
    gemFireProps.put(SERVER_SSL_CIPHERS,
        cacheServerSslciphers);
    gemFireProps.put(
        SERVER_SSL_REQUIRE_AUTHENTICATION,
        String.valueOf(cacheServerSslRequireAuth));

    String keyStore = TestUtil.getResourcePath(SSLNoClientAuthDUnitTest.class, DEFAULT_STORE);
    String trustStore = TestUtil.getResourcePath(SSLNoClientAuthDUnitTest.class, DEFAULT_STORE);
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
      String keyStore, String trustStore) {

    Properties gemFireProps = new Properties();

    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";

    String keyStorePath = TestUtil.getResourcePath(SSLNoClientAuthDUnitTest.class, keyStore);
    String trustStorePath = TestUtil.getResourcePath(SSLNoClientAuthDUnitTest.class, trustStore);
    //using new server-ssl-* properties
    gemFireProps.put(SERVER_SSL_ENABLED,
        String.valueOf(cacheServerSslenabled));
    gemFireProps.put(SERVER_SSL_PROTOCOLS,
        cacheServerSslprotocols);
    gemFireProps.put(SERVER_SSL_CIPHERS,
        cacheServerSslciphers);
    gemFireProps.put(
        SERVER_SSL_REQUIRE_AUTHENTICATION,
        String.valueOf(cacheServerSslRequireAuth));

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
    clientCacheFactory.addPoolServer(host, port);
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
    instance.setUpClientVM(host, port, cacheServerSslenabled, cacheServerSslRequireAuth, keyStore, trustStore);
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
  
  /**
   * Test for GEODE-396
   */
  @Test
  public void testSSLServerWithNoAuth() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);

    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;

    serverVM.invoke(() -> SSLNoClientAuthDUnitTest.setUpServerVMTask(cacheServerSslenabled));
    serverVM.invoke(() -> SSLNoClientAuthDUnitTest.createServerTask());

    Object array[] = (Object[])serverVM.invoke(() -> SSLNoClientAuthDUnitTest.getCacheServerEndPointTask()); 
    String hostName = (String)array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[6];
    params[0] = hostName;
    params[1] = port;
    params[2] = cacheClientSslenabled;
    params[3] = cacheClientSslRequireAuth;
    params[4] = DEFAULT_STORE;
    params[5] = DEFAULT_STORE;
    //getLogWriter().info("Starting client with server endpoint " + hostName + ":" + port);
    try {
      clientVM.invoke(SSLNoClientAuthDUnitTest.class, "setUpClientVMTask", params);
      clientVM.invoke(() -> SSLNoClientAuthDUnitTest.doClientRegionTestTask());
      serverVM.invoke(() -> SSLNoClientAuthDUnitTest.doServerRegionTestTask());
    } catch (Exception rmiException) {
      Throwable e = rmiException.getCause();
      //getLogWriter().info("ExceptionCause at clientVM " + e);
      fail("Unexpected Exception " + e);
    }
  }
  
  @Override
  public final void preTearDown() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    clientVM.invoke(() -> SSLNoClientAuthDUnitTest.closeClientCacheTask());
    serverVM.invoke(() -> SSLNoClientAuthDUnitTest.closeCacheTask());
  }
}
