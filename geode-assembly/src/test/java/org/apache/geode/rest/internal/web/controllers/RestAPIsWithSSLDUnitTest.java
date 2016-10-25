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
package org.apache.geode.rest.internal.web.controllers;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.BindException;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.SSLContext;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.internal.LocatorTestBase;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.ManagementException;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.util.test.TestUtil;

/**
 * @since GemFire 8.0
 */
@Category(DistributedTest.class)
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RestAPIsWithSSLDUnitTest extends LocatorTestBase {

  private static final long serialVersionUID = -254776154266339226L;

  private final String PEOPLE_REGION_NAME = "People";
  private final String INVALID_CLIENT_ALIAS = "INVALID_CLIENT_ALIAS";

  @Parameterized.Parameter
  public String urlContext;

  @Parameterized.Parameters
  public static Collection<String> data() {
    return Arrays.asList("/geode", "/gemfire-api");
  }

  public RestAPIsWithSSLDUnitTest() {
    super();
  }

  @Override
  public final void preSetUp() {
    disconnectAllFromDS();
  }

  @Override
  protected final void postTearDownLocatorTestBase() throws Exception {
    closeInfra();
    disconnectAllFromDS();
  }

  private File findTrustedJKSWithSingleEntry() {
    return new File(TestUtil.getResourcePath(RestAPIsWithSSLDUnitTest.class, "/ssl/trusted.keystore"));
  }

  private File findTrustStoreJKSForPath(Properties props) {
    String propertyValue = props.getProperty(SSL_TRUSTSTORE);
    if (StringUtils.isEmpty(propertyValue)) {
      propertyValue = props.getProperty(HTTP_SERVICE_SSL_TRUSTSTORE);
    }
    if (StringUtils.isEmpty(propertyValue)) {
      propertyValue = props.getProperty(HTTP_SERVICE_SSL_KEYSTORE);
    }
    return new File(propertyValue);
  }

  private File findKeyStoreJKS(Properties props) {
    String propertyValue = props.getProperty(SSL_KEYSTORE);
    if (StringUtils.isEmpty(propertyValue)) {
      propertyValue = props.getProperty(HTTP_SERVICE_SSL_KEYSTORE);
    }
    return new File(propertyValue);
  }

  @SuppressWarnings("deprecation")
  protected int startBridgeServer(String hostName,
                                  int restServicePort,
                                  final String locators,
                                  final String[] regions,
                                  final Properties sslProperties,
                                  boolean clusterLevel) {

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);
    props.setProperty(START_DEV_REST_API, "true");
    props.setProperty(HTTP_SERVICE_BIND_ADDRESS, hostName);
    props.setProperty(HTTP_SERVICE_PORT, String.valueOf(restServicePort));

    System.setProperty("javax.net.debug", "ssl,handshake");
    props = configureSSL(props, sslProperties, clusterLevel);

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
    return server.getPort();
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
      return "https://" + hostName + ":" + restServicePort + urlContext + "/v1";
    });

    // create a client cache
    client.invoke("Create ClientCache", () -> {
      new ClientCacheFactory().setPdxReadSerialized(true).addPoolLocator(locatorHostName, locatorPort).create();
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
    locator.invoke(() -> closeCache());
    manager.invoke(() -> closeCache());
    server.invoke(() -> closeCache());
    client.invoke(() -> closeCache());
  }

  private void closeCache() {
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

  /**
   * @Deprecated once the legacy SSL properties have been removed we need to remove this logic.
   */
  @Deprecated()
  private Properties configureSSL(Properties props, Properties sslProperties, boolean clusterLevel) {

    if (clusterLevel) {
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_ENABLED, CLUSTER_SSL_ENABLED);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_KEYSTORE, CLUSTER_SSL_KEYSTORE);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, CLUSTER_SSL_KEYSTORE_PASSWORD);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_KEYSTORE_TYPE, CLUSTER_SSL_KEYSTORE_TYPE);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_PROTOCOLS, CLUSTER_SSL_PROTOCOLS);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION, CLUSTER_SSL_REQUIRE_AUTHENTICATION);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_TRUSTSTORE, CLUSTER_SSL_TRUSTSTORE);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD, CLUSTER_SSL_TRUSTSTORE_PASSWORD);
    } else {
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_ENABLED, null);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_KEYSTORE, null);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, null);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_KEYSTORE_TYPE, null);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_PROTOCOLS, null);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION, null);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_TRUSTSTORE, null);
      sslPropertyConverter(sslProperties, props, HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD, null);
    }
    String SecurableComponentProperty = sslProperties.getProperty(SSL_ENABLED_COMPONENTS);
    if (SecurableComponentProperty != null && SecurableComponentProperty.length() > 0) {
      sslPropertyConverter(sslProperties, props, SSL_KEYSTORE, null);
      sslPropertyConverter(sslProperties, props, SSL_KEYSTORE_PASSWORD, null);
      sslPropertyConverter(sslProperties, props, SSL_KEYSTORE_TYPE, null);
      sslPropertyConverter(sslProperties, props, SSL_PROTOCOLS, null);
      sslPropertyConverter(sslProperties, props, SSL_REQUIRE_AUTHENTICATION, null);
      sslPropertyConverter(sslProperties, props, SSL_TRUSTSTORE, null);
      sslPropertyConverter(sslProperties, props, SSL_TRUSTSTORE_PASSWORD, null);
      sslPropertyConverter(sslProperties, props, SSL_WEB_ALIAS, null);
      sslPropertyConverter(sslProperties, props, SSL_ENABLED_COMPONENTS, null);
      sslPropertyConverter(sslProperties, props, SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION, null);
      sslPropertyConverter(sslProperties, props, SSL_DEFAULT_ALIAS, null);
    }
    return props;
  }

  private int startManager(final String locators, final String[] regions, final Properties sslProperties) throws IOException {

    IgnoredException.addIgnoredException("java.net.BindException");
    IgnoredException.addIgnoredException("java.rmi.server.ExportException");
    IgnoredException.addIgnoredException("org.apache.geode.management.ManagementException");

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);
    props.setProperty(JMX_MANAGER, "true");
    props.setProperty(JMX_MANAGER_START, "true");

    Cache cache = null;
    configureSSL(props, sslProperties, false);
    while (true) {
      try {
        DistributedSystem ds = getSystem(props);
        System.out.println("Creating cache with http-service-port " + props.getProperty(HTTP_SERVICE_PORT, "7070") + " and jmx-manager-port " + props.getProperty(JMX_MANAGER_PORT, "1099"));
        cache = CacheFactory.create(ds);
        System.out.println("Successfully created cache.");
        break;
      } catch (ManagementException ex) {
        if ((ex.getCause() instanceof BindException) || (ex.getCause() != null && ex.getCause().getCause() instanceof BindException)) {
          //close cache and disconnect
          GemFireCacheImpl existingInstance = GemFireCacheImpl.getInstance();
          if (existingInstance != null) {
            existingInstance.close();
          }
          InternalDistributedSystem ids = InternalDistributedSystem.getConnectedInstance();
          if (ids != null) {
            ids.disconnect();
          }
          //try a different port
          int httpServicePort = AvailablePortHelper.getRandomAvailableTCPPort();
          int jmxManagerPort = AvailablePortHelper.getRandomAvailableTCPPort();
          props.setProperty(HTTP_SERVICE_PORT, Integer.toString(httpServicePort));
          props.setProperty(JMX_MANAGER_PORT, Integer.toString(jmxManagerPort));
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

  private CloseableHttpClient getSSLBasedHTTPClient(Properties properties) throws Exception {

    KeyStore clientKeys = KeyStore.getInstance("JKS");
    File keystoreJKSForPath = findKeyStoreJKS(properties);
    clientKeys.load(new FileInputStream(keystoreJKSForPath), "password".toCharArray());

    KeyStore clientTrust = KeyStore.getInstance("JKS");
    File trustStoreJKSForPath = findTrustStoreJKSForPath(properties);
    clientTrust.load(new FileInputStream(trustStoreJKSForPath), "password".toCharArray());

    // this is needed
    SSLContextBuilder custom = SSLContexts.custom();
    SSLContextBuilder sslContextBuilder = custom.loadTrustMaterial(clientTrust, new TrustSelfSignedStrategy());
    SSLContext sslcontext = sslContextBuilder.loadKeyMaterial(clientKeys, "password".toCharArray(), (aliases, socket) -> {
      if (aliases.size() == 1) {
        return aliases.keySet().stream().findFirst().get();
      }
      if (!StringUtils.isEmpty(properties.getProperty(INVALID_CLIENT_ALIAS))) {
        return properties.getProperty(INVALID_CLIENT_ALIAS);
      } else {
        return properties.getProperty(SSL_WEB_ALIAS);
      }
    }).build();

    // Host checking is disabled here , as tests might run on multiple hosts and
    // host entries can not be assumed
    SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslcontext, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

    return HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).build();
  }

  private void validateConnection(String restEndpoint, String algo, Properties properties) {

    try {
      // 1. Get on key="1" and validate result.
      {
        HttpGet get = new HttpGet(restEndpoint + "/People/1");
        get.addHeader("Content-Type", "application/json");
        get.addHeader("Accept", "application/json");


        CloseableHttpClient httpclient = getSSLBasedHTTPClient(properties);
        CloseableHttpResponse response = httpclient.execute(get);

        HttpEntity entity = response.getEntity();
        InputStream content = entity.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(content));
        String line;
        StringBuilder str = new StringBuilder();
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

  @Test
  public void testSimpleSSL() throws Exception {

    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL", props);
  }

  @Test
  public void testSimpleSSLWithMultiKey_KeyStore() throws Exception {

    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKey.jks"));
    props.setProperty(SSL_TRUSTSTORE, TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKeyTrust.jks"));
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());
    props.setProperty(SSL_WEB_ALIAS, "httpservicekey");
    props.setProperty(SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION, "true");
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL", props);
  }

  @Test(expected = RuntimeException.class)
  public void testSimpleSSLWithMultiKey_KeyStore_WithInvalidClientKey() throws Exception {

    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKey.jks"));
    props.setProperty(SSL_TRUSTSTORE, TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKeyTrust.jks"));
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());
    props.setProperty(SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION, "true");
    props.setProperty(SSL_WEB_ALIAS, "httpservicekey");
    props.setProperty(INVALID_CLIENT_ALIAS, "someAlias");
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL", props);
  }

  @Test
  public void testSSLWithoutKeyStoreType() throws Exception {
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL", props);
  }

  @Test
  public void testSSLWithSSLProtocol() throws Exception {
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_PROTOCOLS, "SSL");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL", props);
  }

  @Test
  public void testSSLWithTLSProtocol() throws Exception {
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_PROTOCOLS, "TLS");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLS", props);
  }

  @Test
  public void testSSLWithTLSv11Protocol() throws Exception {
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_PROTOCOLS, "TLSv1.1");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.1", props);
  }

  @Test
  public void testSSLWithTLSv12Protocol() throws Exception {
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_PROTOCOLS, "TLSv1.2");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2", props);
  }

  @Test
  public void testWithMultipleProtocol() throws Exception {
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_PROTOCOLS, "SSL,TLSv1.2");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2", props);
  }

  @Test
  public void testSSLWithCipherSuite() throws Exception {
    System.setProperty("javax.net.debug", "ssl");
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_PROTOCOLS, "TLSv1.2");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    SSLContext ssl = SSLContext.getInstance("TLSv1.2");

    ssl.init(null, null, new java.security.SecureRandom());
    String[] cipherSuites = ssl.getSocketFactory().getSupportedCipherSuites();

    props.setProperty(SSL_CIPHERS, cipherSuites[0]);

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2", props);
  }

  @Test
  public void testSSLWithMultipleCipherSuite() throws Exception {
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_PROTOCOLS, "TLSv1.2");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    SSLContext ssl = SSLContext.getInstance("TLSv1.2");

    ssl.init(null, null, new java.security.SecureRandom());
    String[] cipherSuites = ssl.getSocketFactory().getSupportedCipherSuites();

    props.setProperty(SSL_CIPHERS, cipherSuites[0] + "," + cipherSuites[1]);

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2", props);
  }

  @Test
  public void testMutualAuthentication() throws Exception {
    Properties props = new Properties();

    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_PROTOCOLS, "SSL");
    props.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
    props.setProperty(SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION, "true");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL", props);
  }

  @Test
  public void testSimpleSSLLegacy() throws Exception {

    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_TYPE, "JKS");
    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL", props);
  }

  @Test
  public void testSSLWithoutKeyStoreTypeLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL", props);
  }

  @Test
  public void testSSLWithSSLProtocolLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "SSL");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL", props);
  }

  @Test
  public void testSSLWithTLSProtocolLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "TLS");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLS", props);
  }

  @Test
  public void testSSLWithTLSv11ProtocolLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "TLSv1.1");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.1", props);
  }

  @Test
  public void testSSLWithTLSv12ProtocolLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "TLSv1.2");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2", props);
  }

  @Test
  public void testWithMultipleProtocolLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "SSL,TLSv1.2");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2", props);
  }

  @Test
  public void testSSLWithCipherSuiteLegacy() throws Exception {
    System.setProperty("javax.net.debug", "ssl");
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "TLSv1.2");

    SSLContext ssl = SSLContext.getInstance("TLSv1.2");

    ssl.init(null, null, new java.security.SecureRandom());
    String[] cipherSuites = ssl.getSocketFactory().getSupportedCipherSuites();

    props.setProperty(HTTP_SERVICE_SSL_CIPHERS, cipherSuites[0]);

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2", props);
  }

  @Test
  public void testSSLWithMultipleCipherSuiteLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "TLSv1.2");

    SSLContext ssl = SSLContext.getInstance("TLSv1.2");

    ssl.init(null, null, new java.security.SecureRandom());
    String[] cipherSuites = ssl.getSocketFactory().getSupportedCipherSuites();

    props.setProperty(HTTP_SERVICE_SSL_CIPHERS, cipherSuites[0] + "," + cipherSuites[1]);

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "TLSv1.2", props);
  }

  @Test
  public void testMutualAuthenticationLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "SSL");
    props.setProperty(HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION, "true");

    props.setProperty(HTTP_SERVICE_SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());

    props.setProperty(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD, "password");

    String restEndpoint = startInfraWithSSL(props, false);
    validateConnection(restEndpoint, "SSL", props);
  }

}
