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

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
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
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.IgnoredException;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.util.test.TestUtil;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONObject;

import javax.net.ssl.SSLContext;
import java.io.*;
import java.net.BindException;
import java.security.KeyStore;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 
 * @since GemFire 8.0
 */
public class RestAPIsWithSSLDUnitTest extends LocatorTestBase {

  private static final long serialVersionUID = -254776154266339226L;

  private final String PEOPLE_REGION_NAME = "People";

  private File jks;

  public RestAPIsWithSSLDUnitTest(String name) {
    super(name);
    this.jks = findTrustedJKS();
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
    if (jks == null) {
      jks = new File(TestUtil.getResourcePath(RestAPIsWithSSLDUnitTest.class, "/ssl/trusted.keystore"));
    }
    return jks;
  }

  @SuppressWarnings("deprecation")
  protected int startBridgeServer(String hostName, int restServicePort, final String locators, final String[] regions,
      final Properties sslProperties, boolean clusterLevel) {

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    props.setProperty(DistributionConfig.START_DEV_REST_API_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_BIND_ADDRESS_NAME, hostName);
    props.setProperty(DistributionConfig.HTTP_SERVICE_PORT_NAME, String.valueOf(restServicePort));

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
    server.setPort(0);
    try {
      server.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
    remoteObjects.put(CACHE_KEY, cache);
    return new Integer(server.getPort());
  }

  public void doPutsInClientCache() {
    ClientCache clientCache = ClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    Region<String, Object> region = clientCache.getRegion(PEOPLE_REGION_NAME);

    // put person object
    region.put("1", new Person(101L, "Mithali", "Dorai", "Raj", new Date(), Gender.FEMALE));
    region.put("2", new Person(102L, "Sachin", "Ramesh", "Tendulkar", new Date(), Gender.MALE));
    region.put("3", new Person(103L, "Saurabh", "Baburav", "Ganguly", new Date(), Gender.MALE));
    region.put("4", new Person(104L, "Rahul", "subrymanyam", "Dravid", new Date(), Gender.MALE));
    region.put("5", new Person(105L, "Jhulan", "Chidambaram", "Goswami", new Date(), Gender.FEMALE));

    Map<String, Object> userMap = new HashMap<String, Object>();
    userMap.put("6", new Person(101L, "Rahul", "Rajiv", "Gndhi", new Date(), Gender.MALE));
    userMap.put("7", new Person(102L, "Narendra", "Damodar", "Modi", new Date(), Gender.MALE));
    userMap.put("8", new Person(103L, "Atal", "Bihari", "Vajpayee", new Date(), Gender.MALE));
    userMap.put("9", new Person(104L, "Soniya", "Rajiv", "Gandhi", new Date(), Gender.FEMALE));
    userMap.put("10", new Person(104L, "Priyanka", "Robert", "Gandhi", new Date(), Gender.FEMALE));
    userMap.put("11", new Person(104L, "Murali", "Manohar", "Joshi", new Date(), Gender.MALE));
    userMap.put("12", new Person(104L, "Lalkrishna", "Parmhansh", "Advani", new Date(), Gender.MALE));
    userMap.put("13", new Person(104L, "Shushma", "kumari", "Swaraj", new Date(), Gender.FEMALE));
    userMap.put("14", new Person(104L, "Arun", "raman", "jetly", new Date(), Gender.MALE));
    userMap.put("15", new Person(104L, "Amit", "kumar", "shah", new Date(), Gender.MALE));
    userMap.put("16", new Person(104L, "Shila", "kumari", "Dixit", new Date(), Gender.FEMALE));

    region.putAll(userMap);

    if (clientCache != null)
      clientCache.getLogger().info("Gemfire Cache Client: Puts successfully done");

  }

  private String startInfraWithSSL(final Properties sslProperties, boolean clusterLevel) throws Exception {

    final Host host = Host.getHost(0);
    VM locator = host.getVM(0);
    VM manager = host.getVM(1);
    VM server = host.getVM(2);
    VM client = host.getVM(3);

    // start locator
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final String locatorHostName = NetworkUtils.getServerHostName(locator.getHost());

    locator.invoke("Start Locator", () -> {
      startLocator(locatorHostName, locatorPort, "");
    });

    // find locators
    String locators = locatorHostName + "[" + locatorPort + "]";

    // start manager (peer cache)
    manager.invoke("StartManager", () -> startManager(locators, new String[] { REGION_NAME }, sslProperties));

    // start startBridgeServer With RestService enabled
    String restEndpoint = server.invoke("startBridgeServerWithRestServiceOnInVM", () -> {
      final String hostName = server.getHost().getHostName();
      final int restServicePort = AvailablePortHelper.getRandomAvailableTCPPort();
      startBridgeServer(hostName, restServicePort, locators, new String[] { REGION_NAME }, sslProperties, clusterLevel);
      return "https://" + hostName + ":" + restServicePort + "/gemfire-api/v1";
    });

    // create a client cache
    client.invoke("Create ClientCache", () -> {
      new ClientCacheFactory()
          .setPdxReadSerialized(true)
          .addPoolLocator(locatorHostName, locatorPort).create();
      return null;
    });

    // create region in Manager, peer cache and Client cache nodes
    manager.invoke("createRegionInManager", () -> createRegionInCache());
    server.invoke("createRegionInPeerServer", () -> createRegionInCache());
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
    locator.invoke(()-> closeCache());
    manager.invoke(()-> closeCache());
    server.invoke(()-> closeCache());
    client.invoke(()-> closeCache());
  }

  private void closeCache()
  {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null && !cache.isClosed()) {
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }

  private void sslPropertyConverter(Properties properties, Properties newProperties, String propertyName, String newPropertyName) {
    String property = properties.getProperty(propertyName);
    if (property != null) {
      newProperties.setProperty((newPropertyName != null ? newPropertyName : propertyName), property);
    }
  }

  private void configureSSL(Properties props, Properties sslProperties, boolean clusterLevel) {

    if (clusterLevel) {
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, DistributionConfig.CLUSTER_SSL_ENABLED_NAME);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, DistributionConfig.CLUSTER_SSL_KEYSTORE_NAME);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME,
          DistributionConfig.CLUSTER_SSL_KEYSTORE_PASSWORD_NAME);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME, DistributionConfig.CLUSTER_SSL_KEYSTORE_TYPE_NAME);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME, DistributionConfig.CLUSTER_SSL_PROTOCOLS_NAME);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME,
          DistributionConfig.CLUSTER_SSL_REQUIRE_AUTHENTICATION_NAME);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_NAME, DistributionConfig.CLUSTER_SSL_TRUSTSTORE_NAME);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME,
          DistributionConfig.CLUSTER_SSL_TRUSTSTORE_PASSWORD_NAME);
    } else {
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, null);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, null);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, null);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_TYPE_NAME, null);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME, null);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME, null);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_NAME, null);
      sslPropertyConverter(sslProperties, props, DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME, null);
    }
  }

  private int startManager(final String locators, final String[] regions, final Properties sslProperties) throws IOException {

    IgnoredException.addIgnoredException("java.net.BindException");
    IgnoredException.addIgnoredException("java.rmi.server.ExportException");
    IgnoredException.addIgnoredException("com.gemstone.gemfire.management.ManagementException");

    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locators);
    props.setProperty("jmx-manager", "true");
    props.setProperty("jmx-manager-start", "true");

    Cache cache = null;
    configureSSL(props, sslProperties, false);
    while (true) {
      try {
        DistributedSystem ds = getSystem(props);
        System.out.println("Creating cache with http-service-port " + props.getProperty("http-service-port", "7070")
            + " and jmx-manager-port " + props.getProperty("jmx-manager-port", "1099"));
        cache = CacheFactory.create(ds);
        System.out.println("Successfully created cache.");
        break;
      } catch (ManagementException ex) {
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
        } else {
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
    server.setPort(0);
    server.start();
    return server.getPort();
  }

  private void createRegionInClientCache() {
    ClientCache clientCache = ClientCacheFactory.getAnyInstance();
    assertNotNull(clientCache);
    clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(PEOPLE_REGION_NAME);
    clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(REGION_NAME);
  }

  private void createRegionInCache() {
    Cache cache = GemFireCacheImpl.getInstance();
    assertNotNull(cache);
    RegionFactory<String, Object> regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);
    regionFactory.create(PEOPLE_REGION_NAME);
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
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME, "SSL");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL");
  }

  public void testSSLWithTLSProtocol() throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME, "TLS");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLS");
  }

  public void testSSLWithTLSv11Protocol() throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME, "TLSv1.1");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.1");
  }

  public void testSSLWithTLSv12Protocol() throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME, "TLSv1.2");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2");
  }

  public void testWithMultipleProtocol() throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME, "SSL,TLSv1.2");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2");
  }

  public void testSSLWithCipherSuite() throws Exception {
    System.setProperty("javax.net.debug", "ssl");
    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME, "TLSv1.2");

    SSLContext ssl = SSLContext.getInstance("TLSv1.2");

    ssl.init(null, null, new java.security.SecureRandom());
    String[] cipherSuites = ssl.getSocketFactory().getSupportedCipherSuites();

    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_CIPHERS_NAME, cipherSuites[0]);

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2");
  }

  public void testSSLWithMultipleCipherSuite() throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME, "TLSv1.2");

    SSLContext ssl = SSLContext.getInstance("TLSv1.2");

    ssl.init(null, null, new java.security.SecureRandom());
    String[] cipherSuites = ssl.getSocketFactory().getSupportedCipherSuites();

    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_CIPHERS_NAME, cipherSuites[0] + "," + cipherSuites[1]);

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2");
  }

  public void testMutualAuthentication() throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_ENABLED_NAME, "true");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_NAME, jks.getCanonicalPath());
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD_NAME, "password");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_PROTOCOLS_NAME, "SSL");
    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION_NAME, "true");

    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_NAME, jks.getCanonicalPath());

    props.setProperty(DistributionConfig.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD_NAME, "password");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL");
  }

}
