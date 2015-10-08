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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.ServerOperationException;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.security.AuthenticationRequiredException;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

/**
 * Tests cacheserver ssl support added. See https://svn.gemstone.com/trac/gemfire/ticket/48995 for details
 * @author tushark
 *
 */
public class CacheServerSSLConnectionDUnit extends DistributedTestCase {
  
  private static final long serialVersionUID = 1L;
  private Cache cache;
  private DistributedSystem ds;
  private CacheServer cacheServer;
  private ClientCache clientCache;
  private int cacheServerPort;
  private String hostName;
  private int locatorPort;
  private Locator locator;
  
  private static final String jtests = System.getProperty("JTESTS");
  
  private static final String TRUSTED_STORE = "trusted.keystore";
  private static final String CLIENT_KEY_STORE = "client.keystore";
  private static final String CLIENT_TRUST_STORE = "client.truststore";
  private static final String SERVER_KEY_STORE = "cacheserver.keystore";
  private static final String SERVER_TRUST_STORE = "cacheserver.truststore";
  
  private static CacheServerSSLConnectionDUnit instance = new CacheServerSSLConnectionDUnit("CacheServerSSLConnectionDUnit");
  
  
  public void setUp() throws Exception {
    disconnectAllFromDS();
    super.setUp();
  }

  public CacheServerSSLConnectionDUnit(String name) {
    super(name);
  }  

  public Cache createCache(Properties props) throws Exception
  {
    ds = getSystem(props);
    cache = CacheFactory.create(ds);
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
  public void setUpServerVM(boolean cacheServerSslenabled, boolean p2pProps, String locators) throws Exception {
    Properties gemFireProps = new Properties();

    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";
    boolean cacheServerSslRequireAuth = true;
    
    String filePathPrefix = jtests
    + File.separator
    + makePath(new String[] { "com", "gemstone", "gemfire", "cache",
        "client", "internal" });
    
    if (!p2pProps) {
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
      gemFireProps.put(DistributionConfig.SERVER_SSL_KEYSTORE_NAME, filePathPrefix
          + SERVER_KEY_STORE);
      gemFireProps.put(DistributionConfig.SERVER_SSL_KEYSTORE_PASSWORD_NAME, "password");
      gemFireProps.put(DistributionConfig.SERVER_SSL_TRUSTSTORE_NAME, filePathPrefix
          + SERVER_TRUST_STORE);
      gemFireProps.put(DistributionConfig.SERVER_SSL_TRUSTSTORE_PASSWORD_NAME, "password");
    } else {
      
      gemFireProps.put(DistributionConfig.SSL_ENABLED_NAME,
          String.valueOf(cacheServerSslenabled));
      gemFireProps.put(DistributionConfig.SSL_PROTOCOLS_NAME,
          cacheServerSslprotocols);
      gemFireProps.put(DistributionConfig.SSL_CIPHERS_NAME,
          cacheServerSslciphers);
      gemFireProps.put(
          DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME,
          String.valueOf(cacheServerSslRequireAuth));

      gemFireProps.put("javax.net.ssl.keyStoreType", "jks");
      gemFireProps.put("javax.net.ssl.keyStore", filePathPrefix + TRUSTED_STORE);
      gemFireProps.put("javax.net.ssl.keyStorePassword", "password");
      gemFireProps.put("javax.net.ssl.trustStore", filePathPrefix + TRUSTED_STORE);
      gemFireProps.put("javax.net.ssl.trustStorePassword", "password");
      gemFireProps.put(DistributionConfig.LOCATORS_NAME, locators);
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
  
  public static String makePath(String[] strings) {
    StringBuilder sb = new StringBuilder();
    for(int i=0;i<strings.length;i++){
      sb.append(strings[i]);      
      sb.append(File.separator);
    }
    return sb.toString();
  }

  public void setUpClientVM(String host, int port,
      boolean cacheServerSslenabled, boolean cacheServerSslRequireAuth,
      boolean p2pProps, String locators, String keyStore, String trustStore) {

    Properties gemFireProps = new Properties();

    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";

    String filePathPrefix = jtests
    + File.separator
    + makePath(new String[] { "com", "gemstone", "gemfire", "cache",
        "client", "internal" });

    //using new server-ssl-* properties
    if (!p2pProps) {
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
      gemFireProps.put(DistributionConfig.SERVER_SSL_KEYSTORE_NAME, filePathPrefix
          + keyStore);
      gemFireProps.put(DistributionConfig.SERVER_SSL_KEYSTORE_PASSWORD_NAME, "password");
      gemFireProps.put(DistributionConfig.SERVER_SSL_TRUSTSTORE_NAME, filePathPrefix
          + trustStore);
      gemFireProps.put(DistributionConfig.SERVER_SSL_TRUSTSTORE_PASSWORD_NAME, "password");
    } else {
      //using p2p ssl-* properties
      gemFireProps.put(DistributionConfig.SSL_ENABLED_NAME,
          String.valueOf(cacheServerSslenabled));
      gemFireProps.put(DistributionConfig.SSL_PROTOCOLS_NAME,
          cacheServerSslprotocols);
      gemFireProps.put(DistributionConfig.SSL_CIPHERS_NAME,
          cacheServerSslciphers);
      gemFireProps.put(
          DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME,
          String.valueOf(cacheServerSslRequireAuth));

      gemFireProps.put("javax.net.ssl.keyStoreType", "jks");
      gemFireProps.put("javax.net.ssl.keyStore", filePathPrefix+ keyStore);
      gemFireProps.put("javax.net.ssl.keyStorePassword", "password");
      gemFireProps.put("javax.net.ssl.trustStore", filePathPrefix + trustStore);
      gemFireProps.put("javax.net.ssl.trustStorePassword", "password");
    }

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
  
  
  public static void setUpServerVMTask(boolean cacheServerSslenabled, boolean p2pProps, String locators) throws Exception{
    instance.setUpServerVM(cacheServerSslenabled,p2pProps,locators);
  }
  
  public static void createServerTask() throws Exception {
    instance.createServer();
  }
  
  public static void setUpClientVMTask(String host, int port,
      boolean cacheServerSslenabled, boolean cacheServerSslRequireAuth, boolean p2pProps, String locators, String keyStore, String trustStore)
      throws Exception {
    instance.setUpClientVM(host, port, cacheServerSslenabled,
        cacheServerSslRequireAuth,p2pProps,locators, keyStore, trustStore);
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
    instance.cache.close();
  }
  
  public static void closeClientCacheTask(){
    instance.clientCache.close();
  }
  
  public static void setUpSSLLocatorVMTask() throws Exception {
    
    Properties props = new Properties();
    instance.locatorPort = AvailablePortHelper.getRandomAvailableTCPPort();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME,"0");
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
    
    String cacheServerSslprotocols = "any";
    String cacheServerSslciphers = "any";
    String filePathPrefix = jtests
    + File.separator
    + makePath(new String[] { "com", "gemstone", "gemfire", "cache",
        "client", "internal" });
    
    props.put(DistributionConfig.SSL_ENABLED_NAME,
        String.valueOf(true));
    props.put(DistributionConfig.SSL_PROTOCOLS_NAME,
        cacheServerSslprotocols);
    props.put(DistributionConfig.SSL_CIPHERS_NAME,
        cacheServerSslciphers);
    
    props.put(
        DistributionConfig.SSL_REQUIRE_AUTHENTICATION_NAME,
        String.valueOf(true)); 
    
    props.put("javax.net.ssl.keyStoreType", "jks");
    props.put("javax.net.ssl.keyStore", filePathPrefix + TRUSTED_STORE);
    props.put("javax.net.ssl.keyStorePassword", "password");
    props.put("javax.net.ssl.trustStore", filePathPrefix + TRUSTED_STORE);
    props.put("javax.net.ssl.trustStorePassword", "password");
    StringWriter sw = new StringWriter();
    PrintWriter writer = new PrintWriter(sw);
    props.list(writer);
    getLogWriter().info("Starting locator ds with following properties \n" + sw.getBuffer());
    
    try {
      File logFile = new File(testName + "-locator" + instance.locatorPort
          + ".log");
      InetAddress bindAddr = null;
      try {
        bindAddr = InetAddress.getLocalHost();
      } catch (UnknownHostException uhe) {
        fail("While resolving bind address ", uhe);
      }
      props.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
      instance.locator = Locator.startLocatorAndDS(instance.locatorPort, logFile, bindAddr, props);      
    } catch (IOException ex) {
      fail("While starting locator on port " + instance.locatorPort, ex);
    }    
    //instance.getSystem(props);
    getLogWriter().info("Locator has started ...");
  }
  
  
  public static String getLocatorPortTask() {    
    return instance.locator.getBindAddress().getHostName() + "[" + instance.locatorPort +"]";
  }
  
  public static void resetSSLPropertiesTask() {
    System.clearProperty("javax.net.ssl.keyStoreType");
    System.clearProperty("javax.net.ssl.keyStore");
    System.clearProperty("javax.net.ssl.keyStorePassword");
    System.clearProperty("javax.net.ssl.trustStore");
    System.clearProperty("javax.net.ssl.trustStorePassword");    
  }

  
  /*
   * Tests that cacheserver started with ssl-enabled properties accepts connection with client configured with ssl-enabled property
   */
  public void testCacheServerSSL_OldWay_P2P() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    VM locatorVM = host.getVM(0);
    
    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;
    boolean useP2pSSLProperties = true;
    
    getLogWriter().info("JTESTS : " + System.getProperty("JTESTS"));
    
    //start SSL Locator
    locatorVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpSSLLocatorVMTask");
    String locatorport = (String)locatorVM.invoke(CacheServerSSLConnectionDUnit.class, "getLocatorPortTask");
    
    Thread.sleep(10);
    
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpServerVMTask", new Object[]{cacheServerSslenabled, useP2pSSLProperties, locatorport});
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "createServerTask");
    
    Object array[] = (Object[])serverVM.invoke(CacheServerSSLConnectionDUnit.class, "getCacheServerEndPointTask"); 
    String hostName = (String)array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[8];
    params[0] = hostName;
    params[1] = port;
    params[2] = cacheClientSslenabled;
    params[3] = cacheClientSslRequireAuth;
    params[4] = useP2pSSLProperties;
    params[5] = locatorport;
    params[6] = TRUSTED_STORE;
    params[7] = TRUSTED_STORE;
    getLogWriter().info("Starting client with server endpoint " + hostName + ":" + port);
    clientVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpClientVMTask", params);
    clientVM.invoke(CacheServerSSLConnectionDUnit.class, "doClientRegionTestTask");
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "doServerRegionTestTask");
    
    locatorVM.invoke(CacheServerSSLConnectionDUnit.class, "resetSSLPropertiesTask");
    
  }
  
  /*
   * Tests that cacheserver started with ssl-enabled properties  accepts connection with client configured with ssl-enabled property
   */  
  public void testCacheServerSSL_ServerOldWay_ClientNewWay() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    VM locatorVM = host.getVM(0);
    
    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;
    boolean useP2pSSLProperties = true;
    
    getLogWriter().info("JTESTS : " + System.getProperty("JTESTS"));
    
    //start SSL Locator
    locatorVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpSSLLocatorVMTask");
    String locatorport = (String)locatorVM.invoke(CacheServerSSLConnectionDUnit.class, "getLocatorPortTask");
    
    Thread.sleep(10);
    
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpServerVMTask", new Object[]{cacheServerSslenabled, useP2pSSLProperties, locatorport});
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "createServerTask");
    
    Object array[] = (Object[])serverVM.invoke(CacheServerSSLConnectionDUnit.class, "getCacheServerEndPointTask"); 
    String hostName = (String)array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[8];
    params[0] = hostName;
    params[1] = port;
    params[2] = cacheClientSslenabled;
    params[3] = cacheClientSslRequireAuth;
    params[4] = !useP2pSSLProperties;
    params[5] = locatorport;
    params[6] = TRUSTED_STORE;
    params[7] = TRUSTED_STORE;
    getLogWriter().info("Starting client with server endpoint " + hostName + ":" + port);
    clientVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpClientVMTask", params);
    clientVM.invoke(CacheServerSSLConnectionDUnit.class, "doClientRegionTestTask");
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "doServerRegionTestTask");
    
    locatorVM.invoke(CacheServerSSLConnectionDUnit.class, "resetSSLPropertiesTask");
    
  }
  
  
  public void testCacheServerSSL() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    
    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;
    boolean useP2pSSLProperties = true;
    
    getLogWriter().info("JTESTS : " + System.getProperty("JTESTS"));
    
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpServerVMTask", new Object[]{cacheServerSslenabled, !useP2pSSLProperties,null});
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "createServerTask");
    
    Object array[] = (Object[])serverVM.invoke(CacheServerSSLConnectionDUnit.class, "getCacheServerEndPointTask"); 
    String hostName = (String)array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[8];
    params[0] = hostName;
    params[1] = port;
    params[2] = cacheClientSslenabled;
    params[3] = cacheClientSslRequireAuth;
    params[4] = !useP2pSSLProperties; //using new server-ssl properties not p2p ssl properties
    params[5] = null;
    params[6] = CLIENT_KEY_STORE;
    params[7] = CLIENT_TRUST_STORE;
    getLogWriter().info("Starting client with server endpoint " + hostName + ":" + port);
    clientVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpClientVMTask", params);
    clientVM.invoke(CacheServerSSLConnectionDUnit.class, "doClientRegionTestTask");
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "doServerRegionTestTask");
    
  }
  
  
  public void testNonSSLClient() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    
    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = false;
    boolean cacheClientSslRequireAuth = true;
    boolean useP2pSSLProperties = true;
    
    getLogWriter().info("JTESTS : " + System.getProperty("JTESTS"));
    
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpServerVMTask", new Object[]{cacheServerSslenabled, !useP2pSSLProperties, null});
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "createServerTask");
    
    Object array[] = (Object[])serverVM.invoke(CacheServerSSLConnectionDUnit.class, "getCacheServerEndPointTask"); 
    String hostName = (String)array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[8];
    params[0] = hostName;
    params[1] = port;
    params[2] = cacheClientSslenabled;
    params[3] = cacheClientSslRequireAuth;
    params[4] = !useP2pSSLProperties; //using new server-ssl properties not p2p ssl properties
    params[5] = null;
    params[6] = TRUSTED_STORE;
    params[7] = TRUSTED_STORE;
    try{
      getLogWriter().info("Starting client with server endpoint " + hostName + ":" + port);    
      clientVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpClientVMTask", params);
      clientVM.invoke(CacheServerSSLConnectionDUnit.class, "doClientRegionTestTask");
      serverVM.invoke(CacheServerSSLConnectionDUnit.class, "doServerRegionTestTask");
      fail("Test should fail as non-ssl client is tryting to connect to ssl configured server");
    } catch (Exception rmiException) {
      Throwable e = rmiException.getCause();
      getLogWriter().info("ExceptionCause at clientVM " + e);
      if (e instanceof com.gemstone.gemfire.cache.client.ServerOperationException) {
        Throwable t = e.getCause();
        getLogWriter().info("Cause is " + t);
        assertTrue(t instanceof com.gemstone.gemfire.security.AuthenticationRequiredException);
      } else {
        getLogWriter().error("Unexpected exception ", e);
        fail("Unexpected Exception...expected "
            + AuthenticationRequiredException.class);
      }
    }
  }
  
  /* It seems it is allowed if client chooses to not insist on authentication. wierd let this test fail??? */
  /*
  public void testSSLClientWithNoAuth() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    
    boolean cacheServerSslenabled = true;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = false;
    
    getLogWriter().info("JTESTS : " + System.getProperty("JTESTS"));
    
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpServerVMTask", new Object[]{cacheServerSslenabled, null});
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "createServerTask");
    
    Object array[] = (Object[])serverVM.invoke(CacheServerSSLConnectionDUnit.class, "getCacheServerEndPointTask"); 
    String hostName = (String)array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[4];
    params[0] = hostName;
    params[1] = port;
    params[2] = cacheClientSslenabled;
    params[3] = cacheClientSslRequireAuth;
    try{
      getLogWriter().info("Starting client with server endpoint " + hostName + ":" + port);    
      clientVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpClientVMTask", params);
      clientVM.invoke(CacheServerSSLConnectionDUnit.class, "doClientRegionTestTask");
      serverVM.invoke(CacheServerSSLConnectionDUnit.class, "doServerRegionTestTask");
      fail("Test should fail as ssl client with wrong config(SslRequireAuth=false) is trying to connect to ssl configured server");
    } catch (Exception rmiException) {
      Throwable e = rmiException.getCause();
      getLogWriter().info("ExceptionCause at clientVM " + e);
      if (e instanceof com.gemstone.gemfire.cache.client.ServerOperationException) {
        Throwable t = e.getCause();
        getLogWriter().info("Cause is " + t);
        assertTrue(t instanceof com.gemstone.gemfire.security.AuthenticationRequiredException);
      } else {
        getLogWriter().error("Unexpected exception ", e);
        fail("Unexpected Exception...expected "
            + AuthenticationRequiredException.class);
      }
    }
  }*/
  
  
  public void testSSLClientWithNonSSLServer() throws Exception {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    
    boolean cacheServerSslenabled = false;
    boolean cacheClientSslenabled = true;
    boolean cacheClientSslRequireAuth = true;
    boolean useP2pSSLProperties = true;
    
    getLogWriter().info("JTESTS : " + System.getProperty("JTESTS"));
    
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpServerVMTask", new Object[]{cacheServerSslenabled, !useP2pSSLProperties, null});
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "createServerTask");
    
    Object array[] = (Object[])serverVM.invoke(CacheServerSSLConnectionDUnit.class, "getCacheServerEndPointTask"); 
    String hostName = (String)array[0];
    int port = (Integer) array[1];
    Object params[] = new Object[8];
    params[0] = hostName;
    params[1] = port;
    params[2] = cacheClientSslenabled;
    params[3] = cacheClientSslRequireAuth;
    params[4] = !useP2pSSLProperties;
    params[5] = null;
    params[6] = TRUSTED_STORE;
    params[7] = TRUSTED_STORE;
    try{
      getLogWriter().info("Starting client with server endpoint " + hostName + ":" + port);    
      clientVM.invoke(CacheServerSSLConnectionDUnit.class, "setUpClientVMTask", params);
      clientVM.invoke(CacheServerSSLConnectionDUnit.class, "doClientRegionTestTask");
      serverVM.invoke(CacheServerSSLConnectionDUnit.class, "doServerRegionTestTask");
      fail("Test should fail as ssl client with wrong config(SslRequireAuth=false) is tryting to connect to server configured with (SslRequireAuth=true)");
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
    }
  }
  
  public void tearDown2() throws Exception
  {
    final Host host = Host.getHost(0);
    VM serverVM = host.getVM(1);
    VM clientVM = host.getVM(2);
    clientVM.invoke(CacheServerSSLConnectionDUnit.class, "closeClientCacheTask");
    serverVM.invoke(CacheServerSSLConnectionDUnit.class, "closeCacheTask");
    super.tearDown2();
  }

}

