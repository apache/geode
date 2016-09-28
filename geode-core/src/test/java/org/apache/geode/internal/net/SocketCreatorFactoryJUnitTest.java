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
package org.apache.geode.internal.net;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.util.test.TestUtil;

@Category(UnitTest.class)
public class SocketCreatorFactoryJUnitTest {

  @After
  public void tearDown() throws Exception {
    SocketCreatorFactory.close();
  }

  @Test
  public void testNewSSLConfigSSLComponentLocator() throws Exception {
    Properties properties = configureSSLProperties(SecurableCommunicationChannel.LOCATOR.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentALL() throws Exception {
    Properties properties = configureSSLProperties(SecurableCommunicationChannel.ALL.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentCLUSTER() throws Exception {
    Properties properties = configureSSLProperties(SecurableCommunicationChannel.CLUSTER.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentGATEWAY() throws Exception {
    Properties properties = configureSSLProperties(SecurableCommunicationChannel.GATEWAY.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentHTTP_SERVICE() throws Exception {
    Properties properties = configureSSLProperties(SecurableCommunicationChannel.WEB.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentJMX() throws Exception {
    Properties properties = configureSSLProperties(SecurableCommunicationChannel.JMX.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentSERVER() throws Exception {
    Properties properties = configureSSLProperties(SecurableCommunicationChannel.SERVER.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentCombinations1() throws Exception {
    Properties properties = configureSSLProperties(commaDelimitedString(SecurableCommunicationChannel.CLUSTER.getConstant(), SecurableCommunicationChannel.SERVER.getConstant()));

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentCombinations2() throws Exception {
    Properties properties = configureSSLProperties(commaDelimitedString(SecurableCommunicationChannel.CLUSTER.getConstant(), SecurableCommunicationChannel.SERVER.getConstant(), SecurableCommunicationChannel.WEB.getConstant(), SecurableCommunicationChannel.JMX.getConstant()));

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentAliasWithMultiKeyStore() throws Exception {
    Properties properties = configureSSLProperties(SecurableCommunicationChannel.ALL.getConstant());

    properties.setProperty(SSL_KEYSTORE, TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKey.jks"));
    properties.setProperty(SSL_TRUSTSTORE, TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKeyTrust.jks"));

    properties.setProperty(SSL_CLUSTER_ALIAS, "clusterKey");
    properties.setProperty(SSL_DEFAULT_ALIAS, "serverKey");

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentWithoutAliasWithMultiKeyStore() throws Exception {
    Properties properties = configureSSLProperties(SecurableCommunicationChannel.ALL.getConstant());

    properties.setProperty(SSL_KEYSTORE, TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKey.jks"));
    properties.setProperty(SSL_TRUSTSTORE, TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKeyTrust.jks"));

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  private Properties configureSSLProperties(String sslComponents) throws IOException {
    File jks = findTestJKS();

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(SSL_CIPHERS, "TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384,TLS_DHE_RSA_WITH_AES_256_CBC_SHA256,TLS_DHE_DSS_WITH_AES_256_CBC_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA");
    properties.setProperty(SSL_PROTOCOLS, "TLSv1,TLSv1.1,TLSv1.2");
    properties.setProperty(SSL_KEYSTORE, jks.getCanonicalPath());
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE, jks.getCanonicalPath());
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.setProperty(SSL_ENABLED_COMPONENTS, sslComponents);

    return properties;
  }

  private String commaDelimitedString(final String... sslComponents) {
    StringBuilder stringBuilder = new StringBuilder();
    for (String sslComponent : sslComponents) {
      stringBuilder.append(sslComponent);
      stringBuilder.append(",");
    }
    return stringBuilder.substring(0, stringBuilder.length() - 1);
  }

  @Test
  public void testLegacyServerSSLConfig() throws IOException {
    File jks = findTestJKS();

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(SERVER_SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(SERVER_SSL_ENABLED, "true");
    properties.setProperty(SERVER_SSL_CIPHERS, "TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384,TLS_DHE_RSA_WITH_AES_256_CBC_SHA256,TLS_DHE_DSS_WITH_AES_256_CBC_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA");
    properties.setProperty(SERVER_SSL_PROTOCOLS, "TLSv1,TLSv1.1,TLSv1.2");
    properties.setProperty(SERVER_SSL_KEYSTORE, jks.getCanonicalPath());
    properties.setProperty(SERVER_SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(SERVER_SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SERVER_SSL_TRUSTSTORE, jks.getCanonicalPath());
    properties.setProperty(SERVER_SSL_TRUSTSTORE_PASSWORD, "password");

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  @Test
  public void testLegacyClusterSSLConfig() throws IOException {
    File jks = findTestJKS();

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(CLUSTER_SSL_ENABLED, "true");
    properties.setProperty(CLUSTER_SSL_CIPHERS, "TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384,TLS_DHE_RSA_WITH_AES_256_CBC_SHA256,TLS_DHE_DSS_WITH_AES_256_CBC_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA");
    properties.setProperty(CLUSTER_SSL_PROTOCOLS, "TLSv1,TLSv1.1,TLSv1.2");
    properties.setProperty(CLUSTER_SSL_KEYSTORE, jks.getCanonicalPath());
    properties.setProperty(CLUSTER_SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(CLUSTER_SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(CLUSTER_SSL_TRUSTSTORE, jks.getCanonicalPath());
    properties.setProperty(CLUSTER_SSL_TRUSTSTORE_PASSWORD, "password");

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }
  
  @Test
  public void testLegacyJMXSSLConfig() throws IOException {
    File jks = findTestJKS();

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(JMX_MANAGER_SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(JMX_MANAGER_SSL_ENABLED, "true");
    properties.setProperty(JMX_MANAGER_SSL_CIPHERS, "TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384,TLS_DHE_RSA_WITH_AES_256_CBC_SHA256,TLS_DHE_DSS_WITH_AES_256_CBC_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA");
    properties.setProperty(JMX_MANAGER_SSL_PROTOCOLS, "TLSv1,TLSv1.1,TLSv1.2");
    properties.setProperty(JMX_MANAGER_SSL_KEYSTORE, jks.getCanonicalPath());
    properties.setProperty(JMX_MANAGER_SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(JMX_MANAGER_SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(JMX_MANAGER_SSL_TRUSTSTORE, jks.getCanonicalPath());
    properties.setProperty(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD, "password");

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  @Test
  public void testLegacyGatewaySSLConfig() throws IOException {
    File jks = findTestJKS();

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(GATEWAY_SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(GATEWAY_SSL_ENABLED, "true");
    properties.setProperty(GATEWAY_SSL_CIPHERS, "TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384,TLS_DHE_RSA_WITH_AES_256_CBC_SHA256,TLS_DHE_DSS_WITH_AES_256_CBC_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA");
    properties.setProperty(GATEWAY_SSL_PROTOCOLS, "TLSv1,TLSv1.1,TLSv1.2");
    properties.setProperty(GATEWAY_SSL_KEYSTORE, jks.getCanonicalPath());
    properties.setProperty(GATEWAY_SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(GATEWAY_SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(GATEWAY_SSL_TRUSTSTORE, jks.getCanonicalPath());
    properties.setProperty(GATEWAY_SSL_TRUSTSTORE_PASSWORD, "password");

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  @Test
  public void testLegacyHttpServiceSSLConfig() throws IOException {
    File jks = findTestJKS();

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    properties.setProperty(HTTP_SERVICE_SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
    properties.setProperty(HTTP_SERVICE_SSL_CIPHERS, "TLS_RSA_WITH_AES_256_CBC_SHA256,TLS_ECDH_ECDSA_WITH_AES_256_CBC_SHA384,TLS_DHE_RSA_WITH_AES_256_CBC_SHA256,TLS_DHE_DSS_WITH_AES_256_CBC_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA");
    properties.setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "TLSv1,TLSv1.1,TLSv1.2");
    properties.setProperty(HTTP_SERVICE_SSL_KEYSTORE, jks.getCanonicalPath());
    properties.setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(HTTP_SERVICE_SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(HTTP_SERVICE_SSL_TRUSTSTORE, jks.getCanonicalPath());
    properties.setProperty(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD, "password");

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.GATEWAY).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.JMX).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.SERVER).useSSL());
    assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.WEB).useSSL());
    assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.LOCATOR).useSSL());
  }

  private File findTestJKS() {
    return new File(TestUtil.getResourcePath(getClass(), "/ssl/trusted.keystore"));
  }
}
