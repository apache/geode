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
package org.apache.geode.rest.internal.web.controllers;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_WEB_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.RestAPITest;

/**
 * @since GemFire 8.0
 */
@Category(RestAPITest.class)
public class RestAPIsWithSSLDUnitTest {

  private static final String PEOPLE_REGION_NAME = "People";
  private static final String INVALID_CLIENT_ALIAS = "INVALID_CLIENT_ALIAS";

  private String urlContext = "/geode";

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  private MemberVM server;
  private ClientVM client;
  private String restEndpoint;

  private static File findTrustedJKSWithSingleEntry() {
    return new File(
        createTempFileFromResource(RestAPIsWithSSLDUnitTest.class, "/ssl/trusted.keystore")
            .getAbsolutePath());
  }

  private static File findTrustStore(Properties props) {
    String propertyValue = props.getProperty(SSL_TRUSTSTORE);
    if (StringUtils.isEmpty(propertyValue)) {
      propertyValue = getDeprecatedTrustStore(props);
    }
    if (StringUtils.isEmpty(propertyValue)) {
      propertyValue = getDeprecatedKeystore(props);
    }
    return new File(propertyValue);
  }

  private static File findKeyStoreJKS(Properties props) {
    String propertyValue = props.getProperty(SSL_KEYSTORE);
    if (StringUtils.isEmpty(propertyValue)) {
      propertyValue = getDeprecatedKeystore(props);
    }
    return new File(propertyValue);
  }

  @SuppressWarnings("deprecation")
  private static String getDeprecatedKeystore(Properties props) {
    return props.getProperty(HTTP_SERVICE_SSL_KEYSTORE);
  }

  @SuppressWarnings("deprecation")
  private static String getDeprecatedTrustStore(Properties props) {
    return props.getProperty(HTTP_SERVICE_SSL_TRUSTSTORE);
  }

  private void startClusterWithSSL(final Properties sslProperties)
      throws Exception {
    MemberVM locator = cluster.startLocatorVM(0);
    server = cluster.startServerVM(1, s -> s.withRestService()
        .withProperties(sslProperties)
        .withConnectionToLocator(locator.getPort())
        .withRegion(RegionShortcut.REPLICATE, PEOPLE_REGION_NAME));
    client = cluster.startClientVM(2, c -> c.withLocatorConnection(locator.getPort())
        .withCacheSetup(cf -> cf.setPdxReadSerialized(true)));

    client.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      Region<String, Person> region =
          clientCache.<String, Person>createClientRegionFactory(ClientRegionShortcut.PROXY)
              .create(PEOPLE_REGION_NAME);

      // put person object
      region.put("1", new Person(101L, "Mithali", "Dorai", "Raj", new Date(), Gender.FEMALE));
      region.put("2", new Person(102L, "Sachin", "Ramesh", "Tendulkar", new Date(), Gender.MALE));
      region.put("3", new Person(103L, "Saurabh", "Baburav", "Ganguly", new Date(), Gender.MALE));
      region.put("4", new Person(104L, "Rahul", "subrymanyam", "Dravid", new Date(), Gender.MALE));
      region.put("5",
          new Person(105L, "Jhulan", "Chidambaram", "Goswami", new Date(), Gender.FEMALE));

      Map<String, Person> userMap = new HashMap<>();
      userMap.put("6", new Person(101L, "Rahul", "Rajiv", "Gndhi", new Date(), Gender.MALE));
      userMap.put("7", new Person(102L, "Narendra", "Damodar", "Modi", new Date(), Gender.MALE));
      userMap.put("8", new Person(103L, "Atal", "Bihari", "Vajpayee", new Date(), Gender.MALE));
      userMap.put("9", new Person(104L, "Soniya", "Rajiv", "Gandhi", new Date(), Gender.FEMALE));
      userMap.put("10",
          new Person(104L, "Priyanka", "Robert", "Gandhi", new Date(), Gender.FEMALE));
      userMap.put("11", new Person(104L, "Murali", "Manohar", "Joshi", new Date(), Gender.MALE));
      userMap.put("12",
          new Person(104L, "Lalkrishna", "Parmhansh", "Advani", new Date(), Gender.MALE));
      userMap.put("13", new Person(104L, "Shushma", "kumari", "Swaraj", new Date(), Gender.FEMALE));
      userMap.put("14", new Person(104L, "Arun", "raman", "jetly", new Date(), Gender.MALE));
      userMap.put("15", new Person(104L, "Amit", "kumar", "shah", new Date(), Gender.MALE));
      userMap.put("16", new Person(104L, "Shila", "kumari", "Dixit", new Date(), Gender.FEMALE));

      region.putAll(userMap);

      clientCache.getLogger().info("Gemfire Cache Client: Puts successfully done");
    });
    restEndpoint = "https://localhost:" + server.getHttpPort() + urlContext + "/v1";
  }

  private static CloseableHttpClient getSSLBasedHTTPClient(Properties properties) throws Exception {
    KeyStore clientKeys = KeyStore.getInstance("JKS");
    File keystoreJKSForPath = findKeyStoreJKS(properties);
    clientKeys.load(new FileInputStream(keystoreJKSForPath), "password".toCharArray());

    KeyStore clientTrust = KeyStore.getInstance("JKS");
    File trustStoreJKSForPath = findTrustStore(properties);
    clientTrust.load(new FileInputStream(trustStoreJKSForPath), "password".toCharArray());

    // this is needed
    SSLContextBuilder custom = SSLContexts.custom();
    SSLContextBuilder sslContextBuilder =
        custom.loadTrustMaterial(clientTrust, new TrustSelfSignedStrategy());
    SSLContext sslcontext = sslContextBuilder
        .loadKeyMaterial(clientKeys, "password".toCharArray(), (aliases, socket) -> {
          if (aliases.size() == 1) {
            return aliases.keySet().stream().findFirst().get();
          }
          if (!StringUtils.isEmpty(properties.getProperty(INVALID_CLIENT_ALIAS))) {
            return properties.getProperty(INVALID_CLIENT_ALIAS);
          } else {
            return properties.getProperty(SSL_WEB_ALIAS);
          }
        }).build();

    // Host checking is disabled here, as tests might run on multiple hosts and
    // host entries can not be assumed
    @SuppressWarnings("deprecation")
    SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(
        sslcontext, SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER);

    return HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).build();
  }

  private void validateConnection(Properties properties) throws Exception {
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

    ObjectMapper mapper = new ObjectMapper();
    JsonNode json = mapper.readTree(str.toString());

    assertEquals(json.get("id").asInt(), 101);
    assertEquals(json.get("firstName").asText(), "Mithali");
    assertEquals(json.get("middleName").asText(), "Dorai");
    assertEquals(json.get("lastName").asText(), "Raj");
    assertEquals(json.get("gender").asText(), Gender.FEMALE.name());
  }

  @Test
  public void testSimpleSSL() throws Exception {
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());
    startClusterWithSSL(props);
    validateConnection(props);
  }

  @Test
  public void testSimpleSSLWithMultiKey_KeyStore() throws Exception {
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE,
        createTempFileFromResource(getClass(), "/org/apache/geode/internal/net/multiKey.jks")
            .getAbsolutePath());
    props.setProperty(SSL_TRUSTSTORE,
        createTempFileFromResource(getClass(),
            "/org/apache/geode/internal/net/multiKeyTrust.jks").getAbsolutePath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());
    props.setProperty(SSL_WEB_ALIAS, "httpservicekey");
    props.setProperty(SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION, "true");
    startClusterWithSSL(props);
    validateConnection(props);
  }

  @Test(expected = RuntimeException.class)
  public void testSimpleSSLWithMultiKey_KeyStore_WithInvalidClientKey() throws Exception {
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE,
        createTempFileFromResource(getClass(), "/org/apache/geode/internal/net/multiKey.jks")
            .getAbsolutePath());
    props.setProperty(SSL_TRUSTSTORE,
        createTempFileFromResource(getClass(),
            "/org/apache/geode/internal/net/multiKeyTrust.jks").getAbsolutePath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());
    props.setProperty(SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION, "true");
    props.setProperty(SSL_WEB_ALIAS, "httpservicekey");
    props.setProperty(INVALID_CLIENT_ALIAS, "someAlias");
    startClusterWithSSL(props);
  }

  @Test
  public void testSSLWithoutKeyStoreType() throws Exception {
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    startClusterWithSSL(props);
    validateConnection(props);
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

    startClusterWithSSL(props);
    validateConnection(props);
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

    startClusterWithSSL(props);
    validateConnection(props);
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
    props.setProperty(SSL_CIPHERS, "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    startClusterWithSSL(props);
    validateConnection(props);
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

    startClusterWithSSL(props);
    validateConnection(props);
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

    startClusterWithSSL(props);
    validateConnection(props);
  }

  private List<String> getRSACiphers() throws Exception {
    SSLContext ssl = SSLContext.getInstance("TLSv1.2");

    ssl.init(null, null, new java.security.SecureRandom());
    String[] cipherSuites = ssl.getSocketFactory().getSupportedCipherSuites();

    return Arrays.stream(cipherSuites).filter(c -> c.contains("RSA")).collect(Collectors.toList());
  }

  @Test
  public void testSSLWithCipherSuite() throws Exception {
    System.setProperty("javax.net.debug", "ssl,handshake");
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_PROTOCOLS, "TLSv1.2");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    // This is the safest in terms of support across various JDK releases
    List<String> rsaCiphers = getRSACiphers();
    props.setProperty(SSL_CIPHERS, rsaCiphers.get(0));

    startClusterWithSSL(props);
    validateConnection(props);
  }

  @Test
  public void testSSLWithMultipleCipherSuite() throws Exception {
    System.setProperty("javax.net.debug", "ssl,handshake");
    Properties props = new Properties();
    props.setProperty(SSL_KEYSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_TRUSTSTORE, findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    props.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    props.setProperty(SSL_PROTOCOLS, "TLSv1.2");
    props.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    // This is the safest in terms of support across various JDK releases
    List<String> rsaCiphers = getRSACiphers();
    props.setProperty(SSL_CIPHERS, rsaCiphers.get(0) + "," + rsaCiphers.get(1));

    startClusterWithSSL(props);
    validateConnection(props);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSSLWithMultipleCipherSuiteLegacy() throws Exception {
    System.setProperty("javax.net.debug", "ssl,handshake");
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE,
        findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "TLSv1.2");

    // This is the safest in terms of support across various JDK releases
    List<String> rsaCiphers = getRSACiphers();
    props.setProperty(SSL_CIPHERS, rsaCiphers.get(0) + "," + rsaCiphers.get(1));

    startClusterWithSSL(props);
    validateConnection(props);
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

    startClusterWithSSL(props);
    validateConnection(props);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSimpleSSLLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE,
        findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_TYPE, "JKS");
    startClusterWithSSL(props);
    validateConnection(props);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSSLWithoutKeyStoreTypeLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE,
        findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");

    startClusterWithSSL(props);
    validateConnection(props);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSSLWithSSLProtocolLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE,
        findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "SSL");

    startClusterWithSSL(props);
    validateConnection(props);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSSLWithTLSProtocolLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE,
        findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "TLS");

    startClusterWithSSL(props);
    validateConnection(props);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSSLWithTLSv11ProtocolLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE,
        findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "TLSv1.1");
    props.setProperty(HTTP_SERVICE_SSL_CIPHERS, "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA");

    startClusterWithSSL(props);
    validateConnection(props);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSSLWithTLSv12ProtocolLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE,
        findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "TLSv1.2");

    startClusterWithSSL(props);
    validateConnection(props);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testWithMultipleProtocolLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE,
        findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "SSL,TLSv1.2");

    startClusterWithSSL(props);
    validateConnection(props);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testSSLWithCipherSuiteLegacy() throws Exception {
    System.setProperty("javax.net.debug", "ssl");
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE,
        findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "TLSv1.2");

    SSLContext ssl = SSLContext.getInstance("TLSv1.2");

    ssl.init(null, null, new java.security.SecureRandom());
    String[] cipherSuites = ssl.getSocketFactory().getSupportedCipherSuites();

    String rsaCipher = Arrays.stream(cipherSuites).filter(c -> c.contains("RSA")).findFirst().get();
    props.setProperty(HTTP_SERVICE_SSL_CIPHERS, rsaCipher);

    startClusterWithSSL(props);
    validateConnection(props);
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testMutualAuthenticationLegacy() throws Exception {
    Properties props = new Properties();
    props.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE,
        findTrustedJKSWithSingleEntry().getCanonicalPath());
    props.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    props.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "SSL");
    props.setProperty(HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION, "true");

    props.setProperty(HTTP_SERVICE_SSL_TRUSTSTORE,
        findTrustedJKSWithSingleEntry().getCanonicalPath());

    props.setProperty(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD, "password");

    startClusterWithSSL(props);
    validateConnection(props);
  }
}
