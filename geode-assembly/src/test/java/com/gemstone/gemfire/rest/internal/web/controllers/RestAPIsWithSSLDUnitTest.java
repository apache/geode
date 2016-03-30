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
package com.gemstone.gemfire.rest.internal.web.controllers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.BindException;
import java.security.KeyStore;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import com.gemstone.gemfire.test.dunit.*;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONObject;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.internal.LocatorTestBase;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.ManagementException;
import com.gemstone.gemfire.management.ManagementTestBase;
import com.gemstone.gemfire.util.test.TestUtil;

/**
 * 
 * @since 8.0
 */
public class RestAPIsWithSSLDUnitTest extends LocatorTestBase {

  private static final long serialVersionUID = -254776154266339226L;

  private ManagementTestBase helper;

  private final String PEOPLE_REGION_NAME = "People";
  
  private File jks;

  public RestAPIsWithSSLDUnitTest(String name) {
    super(name);
    this.helper = new ManagementTestBase(name);
    this.jks  = findTrustedJKS();

  }

  @Override
  public final void preSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  protected final void postTearDownLocatorTestBase() throws Exception {
    closeInfra();
    disconnectAllFromDS();
  }

  private File findTrustedJKS() {
    if(jks == null){
      jks = new File(TestUtil.getResourcePath(RestAPIsWithSSLDUnitTest.class, "/ssl/trusted.keystore"));
    }
    return jks;
  }

  public String startBridgeServerWithRestServiceOnInVM(final VM vm, final String locators, final String[] regions,
      final Properties sslProperties, final boolean clusterLevel) {

    final String hostName = vm.getHost().getHostName();
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    vm.invoke("startBridge", () -> startBridgeServer(hostName,serverPort,locators,regions,sslProperties,clusterLevel));
    return "https://" + hostName + ":" + serverPort + "/gemfire-api/v1";

  }

  @SuppressWarnings("deprecation")
  protected int startBridgeServer(String hostName, int restServicerPort, final String locators, final String[] regions,
      final Properties sslProperties, boolean clusterLevel) {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    props.setProperty(DistributionConfig.START_DEV_REST_API_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_BIND_ADDRESS_NAME, hostName);
    props.setProperty(DistributionConfig.HTTP_SERVICE_PORT_NAME, String.valueOf(restServicerPort));

    System.setProperty("javax.net.debug", "ssl,handshake");
    configureSSL(props, sslProperties, clusterLevel);

    DistributedSystem ds = getSystem(props);
    Cache cache = CacheFactory.create(ds);
    ((GemFireCacheImpl) cache).setReadSerialized(true);
    AttributesFactory factory = new AttributesFactory();

    factory.setEnableBridgeConflation(true);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    RegionAttributes attrs = factory.create();
    for (int i = 0; i < regions.length; i++) {
      cache.createRegion(regions[i], attrs);
    }

    CacheServer server = cache.addCacheServer();
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
    server.setPort(serverPort);
    try {
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
    remoteObjects.put(CACHE_KEY, cache);
    return new Integer(serverPort);
  }

  public void doPutsInClientCache() {
    ClientCache cache = GemFireCacheImpl.getInstance();
    assertNotNull(cache);
    Region<String, Object> region = cache.getRegion(PEOPLE_REGION_NAME);

    // put person object
    final Person person1 = new Person(101L, "Mithali", "Dorai", "Raj", DateTimeUtils.createDate(1982,
        Calendar.DECEMBER, 4), Gender.FEMALE);
    final Person person2 = new Person(102L, "Sachin", "Ramesh", "Tendulkar", DateTimeUtils.createDate(1975,
        Calendar.DECEMBER, 14), Gender.MALE);
    final Person person3 = new Person(103L, "Saurabh", "Baburav", "Ganguly", DateTimeUtils.createDate(1972,
        Calendar.AUGUST, 29), Gender.MALE);
    final Person person4 = new Person(104L, "Rahul", "subrymanyam", "Dravid", DateTimeUtils.createDate(1979,
        Calendar.MARCH, 17), Gender.MALE);
    final Person person5 = new Person(105L, "Jhulan", "Chidambaram", "Goswami", DateTimeUtils.createDate(1983,
        Calendar.NOVEMBER, 25), Gender.FEMALE);

    region.put("1", person1);
    region.put("2", person2);
    region.put("3", person3);
    region.put("4", person4);
    region.put("5", person5);

    final Person person6 = new Person(101L, "Rahul", "Rajiv", "Gndhi",
        DateTimeUtils.createDate(1970, Calendar.MAY, 14), Gender.MALE);
    final Person person7 = new Person(102L, "Narendra", "Damodar", "Modi", DateTimeUtils.createDate(1945,
        Calendar.DECEMBER, 24), Gender.MALE);
    final Person person8 = new Person(103L, "Atal", "Bihari", "Vajpayee", DateTimeUtils.createDate(1920,
        Calendar.AUGUST, 9), Gender.MALE);
    final Person person9 = new Person(104L, "Soniya", "Rajiv", "Gandhi", DateTimeUtils.createDate(1929, Calendar.MARCH,
        27), Gender.FEMALE);
    final Person person10 = new Person(104L, "Priyanka", "Robert", "Gandhi", DateTimeUtils.createDate(1973,
        Calendar.APRIL, 15), Gender.FEMALE);

    final Person person11 = new Person(104L, "Murali", "Manohar", "Joshi", DateTimeUtils.createDate(1923,
        Calendar.APRIL, 25), Gender.MALE);
    final Person person12 = new Person(104L, "Lalkrishna", "Parmhansh", "Advani", DateTimeUtils.createDate(1910,
        Calendar.JANUARY, 01), Gender.MALE);
    final Person person13 = new Person(104L, "Shushma", "kumari", "Swaraj", DateTimeUtils.createDate(1943,
        Calendar.AUGUST, 10), Gender.FEMALE);
    final Person person14 = new Person(104L, "Arun", "raman", "jetly", DateTimeUtils.createDate(1942, Calendar.OCTOBER,
        27), Gender.MALE);
    final Person person15 = new Person(104L, "Amit", "kumar", "shah", DateTimeUtils.createDate(1958, Calendar.DECEMBER,
        21), Gender.MALE);
    final Person person16 = new Person(104L, "Shila", "kumari", "Dixit", DateTimeUtils.createDate(1927,
        Calendar.FEBRUARY, 15), Gender.FEMALE);

    Map<String, Object> userMap = new HashMap<String, Object>();
    userMap.put("6", person6);
    userMap.put("7", person7);
    userMap.put("8", person8);
    userMap.put("9", person9);
    userMap.put("10", person10);
    userMap.put("11", person11);
    userMap.put("12", person12);
    userMap.put("13", person13);
    userMap.put("14", person14);
    userMap.put("15", person15);
    userMap.put("16", person16);

    region.putAll(userMap);

    if (cache != null)
      cache.getLogger().info("Gemfire Cache Client: Puts successfully done");

  }

  private String startInfraWithSSL(final Properties sslProperties, boolean clusterLevel) throws Exception {

    final Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    VM manager = host.getVM(1);
    VM server = host.getVM(2);
    VM client = host.getVM(3);

    // start locator
    int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    startLocatorInVM(locator, locatorPort, "");

    // find locators
    String locators = NetworkUtils.getServerHostName(locator.getHost()) + "[" + locatorPort + "]";

    // start manager (peer cache)
    startManagerInVM(manager, locators, new String[] { REGION_NAME }, sslProperties);

    // start startBridgeServer With RestService enabled
    String restEndpoint = startBridgeServerWithRestServiceOnInVM(server, locators, new String[] { REGION_NAME },
        sslProperties, clusterLevel);

    // create a client cache
    createClientCacheInVM(client, NetworkUtils.getServerHostName(locator.getHost()), locatorPort);

    // create region in Manager, peer cache and Client cache nodes
    manager.invoke("createRegionInManager",() -> createRegionInManager());
    server.invoke("createRegionInPeerServer", () -> createRegionInPeerServer());
    client.invoke("createRegionInClientCache", () -> createRegionInClientCache());

    // do some person puts from clientcache
    client.invoke("doPutsInClientCache", () -> doPutsInClientCache());

    return restEndpoint;

  }

  private void closeInfra() throws Exception {

    final Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    VM manager = host.getVM(1);
    VM server = host.getVM(2);
    VM client = host.getVM(3);

    // stop the client and make sure the bridge server notifies
    // stopBridgeMemberVM(client);
    helper.closeCache(locator);
    helper.closeCache(manager);
    helper.closeCache(server);
    helper.closeCache(client);
  }

  private void createClientCacheInVM(VM vm, final String host, final int port) throws Exception {
    SerializableRunnable connect = new SerializableRunnable("Start Cache client") {
      public void run() {
        // Connect using the GemFire locator and create a Caching_Proxy cache
        ClientCache clientCache = new ClientCacheFactory().setPdxReadSerialized(true).addPoolLocator(host, port).create();
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
      }
    };

    if (vm == null) {
      connect.run();
    } else {
      vm.invoke(connect);
    }
  }

  private void configureSSL(Properties props, Properties sslProperties, boolean clusterLevel) {

    if(clusterLevel){
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME) != null) {
        props.setProperty(DistributionConfig.CLUSTER_SSL_ENABLED_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME) != null) {
        props.setProperty(DistributionConfig.CLUSTER_SSL_KEYSTORE_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME) != null) {
        props.setProperty(DistributionConfig.CLUSTER_SSL_KEYSTORE_PASSWORD_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME) != null) {
        props.setProperty(DistributionConfig.CLUSTER_SSL_KEYSTORE_TYPE_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME) != null) {
        props.setProperty(DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME) != null) {
        props.setProperty(DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_NAME) != null) {
        props.setProperty(DistributionConfig.CLUSTER_SSL_TRUSTSTORE_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME) != null) {
        props.setProperty(DistributionConfig.CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME));
      }

    }else{
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME) != null) {
        props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME) != null) {
        props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME) != null) {
        props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME) != null) {
        props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME) != null) {
        props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME) != null) {
        props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_NAME) != null) {
        props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_NAME));
      }
      if (sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME) != null) {
        props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME,
            sslProperties.getProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME));
      }

    }
    
   
  }

  private int startManagerInVM(VM vm, final String locators, final String[] regions, final Properties sslProperties) {
    
    IgnoredException.addIgnoredException("java.net.BindException");
    IgnoredException.addIgnoredException("java.rmi.server.ExportException");
    IgnoredException.addIgnoredException("com.gemstone.gemfire.management.ManagementException");
    
    SerializableCallable connect = new SerializableCallable("Start Manager ") {
      public Object call() throws IOException {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(0));
        props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
        props.setProperty("jmx-manager", "true");
        props.setProperty("jmx-manager-start", "true");

        Cache cache = null;
        while (true) {
          try {
            configureSSL(props, sslProperties, false);
            DistributedSystem ds = getSystem(props);
            System.out.println("Creating cache with http-service-port " + props.getProperty("http-service-port", "7070") 
            + " and jmx-manager-port " + props.getProperty("jmx-manager-port", "1099"));            
            cache = CacheFactory.create(ds);
            System.out.println("Successfully created cache.");
            break;
          }
          catch (ManagementException ex) {
            if ((ex.getCause() instanceof BindException) 
                || (ex.getCause() != null && ex.getCause().getCause() instanceof BindException)) {
              //close cache and disconnect
              GemFireCacheImpl existingInstance = GemFireCacheImpl.getInstance();
              if (existingInstance != null) {
                existingInstance.close();
              }
              InternalDistributedSystem ids = InternalDistributedSystem
                  .getConnectedInstance();
              if (ids != null) {
                ids.disconnect();
              }
              //try a different port
              int httpServicePort = AvailablePortHelper.getRandomAvailableTCPPort();
              int jmxManagerPort = AvailablePortHelper.getRandomAvailableTCPPort();
              props.setProperty("http-service-port", Integer.toString(httpServicePort));
              props.setProperty("jmx-manager-port", Integer.toString(jmxManagerPort));
              System.out.println("Try a different http-service-port " + httpServicePort);
              System.out.println("Try a different jmx-manager-port " + jmxManagerPort);
            }
            else {
              throw ex;
            }
          }
        } 
        AttributesFactory factory = new AttributesFactory();

        factory.setEnableBridgeConflation(true);
        factory.setDataPolicy(DataPolicy.REPLICATE);
        RegionAttributes attrs = factory.create();
        for (int i = 0; i < regions.length; i++) {
          cache.createRegion(regions[i], attrs);
        }
        CacheServer server = cache.addCacheServer();
        final int serverPort = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(serverPort);
        server.start();

        return new Integer(serverPort);
      }
    };
    Integer port = (Integer) vm.invoke(connect);
    return port.intValue();
  }

  private void createRegionInClientCache() {
    ClientCache cache = GemFireCacheImpl.getInstance();
    assertNotNull(cache);
    ClientRegionFactory<String, Object> crf = cache.createClientRegionFactory(ClientRegionShortcut.PROXY);
    crf.create(PEOPLE_REGION_NAME);
  }

  private void createRegionInManager() {
    Cache cache = GemFireCacheImpl.getInstance();
    assertNotNull(cache);
    RegionFactory<String, Object> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.create(PEOPLE_REGION_NAME);
  }

  private void createRegionInPeerServer() {
    Cache cache = GemFireCacheImpl.getInstance();
    assertNotNull(cache);
    RegionFactory<String, Object> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.create(PEOPLE_REGION_NAME);
  }
  
  private CloseableHttpClient getSSLBasedHTTPClient(String algo) throws Exception {
    
    File jks = findTrustedJKS();

    KeyStore clientKeys = KeyStore.getInstance("JKS");
    clientKeys.load(new FileInputStream(jks.getCanonicalPath()), "password".toCharArray());

    // this is needed
    SSLContext sslcontext = SSLContexts.custom()
        .loadTrustMaterial(clientKeys, new TrustSelfSignedStrategy())
        .loadKeyMaterial(clientKeys, "password".toCharArray())
    .build();

    // Host checking is disabled here , as tests might run on multiple hosts and
    // host entries can not be assumed
    SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslcontext,
        SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

    CloseableHttpClient httpclient = HttpClients.custom().setSSLSocketFactory(sslsf).build();

    return httpclient;
  }

  private void validateConnection(String restEndpoint, String algo) {

    try {
      // 1. Get on key="1" and validate result.
      {
        HttpGet get = new HttpGet(restEndpoint + "/People/1");
        get.addHeader("Content-Type", "application/json");
        get.addHeader("Accept", "application/json");

       
        CloseableHttpClient httpclient = getSSLBasedHTTPClient(algo);
        CloseableHttpResponse response = httpclient.execute(get);

        HttpEntity entity = response.getEntity();
        InputStream content = entity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(content));
        String line;
        StringBuffer str = new StringBuffer();
        while ((line = reader.readLine()) != null) {
          str.append(line);
        }

        JSONObject jObject = new JSONObject(str.toString());

        assertEquals(jObject.get("id"), 101);
        assertEquals(jObject.get("firstName"), "Mithali");
        assertEquals(jObject.get("middleName"), "Dorai");
        assertEquals(jObject.get("lastName"), "Raj");
        assertEquals(jObject.get("gender"), Gender.FEMALE.name());
      }

    } catch (Exception e) {
      throw new RuntimeException("unexpected exception", e);
    }
  }
  
  // Actual Tests starts here.

  public void testSimpleSSL() throws Exception {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME, "JKS");
    String restEndpoint = startInfraWithSSL(props,false);
    validateConnection(restEndpoint, "SSL");
  }
  
  public void testSSLWithoutKeyStoreType() throws Exception {

   

    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
  
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL");


  }
  
  public void testSSLWithSSLProtocol() throws Exception {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME,"SSL");
    
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL");

  }
  
  public void testSSLWithTLSProtocol() throws Exception {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME,"TLS");
    
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLS");

  }
  
  public void testSSLWithTLSv11Protocol() throws Exception {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME,"TLSv1.1");
    
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.1");

  }
  
  public void testSSLWithTLSv12Protocol() throws Exception {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME,"TLSv1.2");
    
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2");

  }
  
  public void testWithMultipleProtocol() throws Exception {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME,"SSL,TLSv1.2");
    
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2");

  }
  
  public void testSSLWithCipherSuite() throws Exception {

    System.setProperty("javax.net.debug", "ssl");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME,"TLSv1.2");
    
    SSLContext ssl = SSLContext.getInstance("TLSv1.2");
    
    ssl.init(null, null, new java.security.SecureRandom());
    String[] cipherSuites = ssl.getSocketFactory().getSupportedCipherSuites();
    
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_CIPHERS_NAME,cipherSuites[0]);
    
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2");

  }
  
  public void testSSLWithMultipleCipherSuite() throws Exception {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME,"TLSv1.2");
    
    SSLContext ssl = SSLContext.getInstance("TLSv1.2");
    
    ssl.init(null, null, new java.security.SecureRandom());
    String[] cipherSuites = ssl.getSocketFactory().getSupportedCipherSuites();
    
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_CIPHERS_NAME,cipherSuites[0]+","+cipherSuites[1]);
    
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2");

  }
  
  
  public void testMutualAuthentication() throws Exception {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME,"SSL");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME,"true");

    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_NAME,jks.getCanonicalPath());

    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME,"password");
    
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL");
  }


}
