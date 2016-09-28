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
import static org.apache.geode.internal.net.SocketCreatorFactory.*;
import static org.apache.geode.internal.security.SecurableCommunicationChannel.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.util.test.TestUtil;

@Category(IntegrationTest.class)
public class SocketCreatorFactoryJUnitTest {

  @After
  public void tearDown() throws Exception {
    close();
  }

  @Test
  public void testNewSSLConfigSSLComponentLocator() throws Exception {
    Properties properties = configureSSLProperties(LOCATOR.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    setDistributionConfig(distributionConfig);

    assertTrue(getSocketCreatorForComponent(LOCATOR).useSSL());
    assertFalse(getSocketCreatorForComponent(CLUSTER).useSSL());
    assertFalse(getSocketCreatorForComponent(GATEWAY).useSSL());
    assertFalse(getSocketCreatorForComponent(JMX).useSSL());
    assertFalse(getSocketCreatorForComponent(SERVER).useSSL());
    assertFalse(getSocketCreatorForComponent(WEB).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentALL() throws Exception {
    Properties properties = configureSSLProperties(ALL.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    setDistributionConfig(distributionConfig);

    assertTrue(getSocketCreatorForComponent(CLUSTER).useSSL());
    assertTrue(getSocketCreatorForComponent(LOCATOR).useSSL());
    assertTrue(getSocketCreatorForComponent(GATEWAY).useSSL());
    assertTrue(getSocketCreatorForComponent(JMX).useSSL());
    assertTrue(getSocketCreatorForComponent(SERVER).useSSL());
    assertTrue(getSocketCreatorForComponent(WEB).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentCLUSTER() throws Exception {
    Properties properties = configureSSLProperties(CLUSTER.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    setDistributionConfig(distributionConfig);

    assertTrue(getSocketCreatorForComponent(CLUSTER).useSSL());
    assertFalse(getSocketCreatorForComponent(GATEWAY).useSSL());
    assertFalse(getSocketCreatorForComponent(JMX).useSSL());
    assertFalse(getSocketCreatorForComponent(SERVER).useSSL());
    assertFalse(getSocketCreatorForComponent(WEB).useSSL());
    assertFalse(getSocketCreatorForComponent(LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentGATEWAY() throws Exception {
    Properties properties = configureSSLProperties(GATEWAY.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    setDistributionConfig(distributionConfig);

    assertFalse(getSocketCreatorForComponent(CLUSTER).useSSL());
    assertTrue(getSocketCreatorForComponent(GATEWAY).useSSL());
    assertFalse(getSocketCreatorForComponent(JMX).useSSL());
    assertFalse(getSocketCreatorForComponent(SERVER).useSSL());
    assertFalse(getSocketCreatorForComponent(WEB).useSSL());
    assertFalse(getSocketCreatorForComponent(LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentHTTP_SERVICE() throws Exception {
    Properties properties = configureSSLProperties(WEB.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    setDistributionConfig(distributionConfig);

    assertFalse(getSocketCreatorForComponent(CLUSTER).useSSL());
    assertFalse(getSocketCreatorForComponent(GATEWAY).useSSL());
    assertFalse(getSocketCreatorForComponent(JMX).useSSL());
    assertFalse(getSocketCreatorForComponent(SERVER).useSSL());
    assertTrue(getSocketCreatorForComponent(WEB).useSSL());
    assertFalse(getSocketCreatorForComponent(LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentJMX() throws Exception {
    Properties properties = configureSSLProperties(JMX.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    setDistributionConfig(distributionConfig);

    assertFalse(getSocketCreatorForComponent(CLUSTER).useSSL());
    assertFalse(getSocketCreatorForComponent(GATEWAY).useSSL());
    assertTrue(getSocketCreatorForComponent(JMX).useSSL());
    assertFalse(getSocketCreatorForComponent(SERVER).useSSL());
    assertFalse(getSocketCreatorForComponent(WEB).useSSL());
    assertFalse(getSocketCreatorForComponent(LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentSERVER() throws Exception {
    Properties properties = configureSSLProperties(SERVER.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    setDistributionConfig(distributionConfig);

    assertFalse(getSocketCreatorForComponent(CLUSTER).useSSL());
    assertFalse(getSocketCreatorForComponent(GATEWAY).useSSL());
    assertFalse(getSocketCreatorForComponent(JMX).useSSL());
    assertTrue(getSocketCreatorForComponent(SERVER).useSSL());
    assertFalse(getSocketCreatorForComponent(WEB).useSSL());
    assertFalse(getSocketCreatorForComponent(LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentCombinations1() throws Exception {
    Properties properties = configureSSLProperties(commaDelimitedString(CLUSTER.getConstant(), SERVER.getConstant()));

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    setDistributionConfig(distributionConfig);

    assertTrue(getSocketCreatorForComponent(CLUSTER).useSSL());
    assertFalse(getSocketCreatorForComponent(GATEWAY).useSSL());
    assertFalse(getSocketCreatorForComponent(JMX).useSSL());
    assertTrue(getSocketCreatorForComponent(SERVER).useSSL());
    assertFalse(getSocketCreatorForComponent(WEB).useSSL());
    assertFalse(getSocketCreatorForComponent(LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentCombinations2() throws Exception {
    Properties properties = configureSSLProperties(commaDelimitedString(CLUSTER.getConstant(), SERVER.getConstant(), WEB.getConstant(), JMX.getConstant()));

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    setDistributionConfig(distributionConfig);

    assertTrue(getSocketCreatorForComponent(CLUSTER).useSSL());
    assertFalse(getSocketCreatorForComponent(GATEWAY).useSSL());
    assertTrue(getSocketCreatorForComponent(JMX).useSSL());
    assertTrue(getSocketCreatorForComponent(SERVER).useSSL());
    assertTrue(getSocketCreatorForComponent(WEB).useSSL());
    assertFalse(getSocketCreatorForComponent(LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentAliasWithMultiKeyStore() throws Exception {
    Properties properties = configureSSLProperties(ALL.getConstant());

    properties.setProperty(SSL_KEYSTORE, TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKey.jks"));
    properties.setProperty(SSL_TRUSTSTORE, TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKeyTrust.jks"));

    properties.setProperty(SSL_CLUSTER_ALIAS, "clusterKey");
    properties.setProperty(SSL_DEFAULT_ALIAS, "serverKey");

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    setDistributionConfig(distributionConfig);

    assertTrue(getSocketCreatorForComponent(CLUSTER).useSSL());
    assertTrue(getSocketCreatorForComponent(GATEWAY).useSSL());
    assertTrue(getSocketCreatorForComponent(JMX).useSSL());
    assertTrue(getSocketCreatorForComponent(SERVER).useSSL());
    assertTrue(getSocketCreatorForComponent(WEB).useSSL());
    assertTrue(getSocketCreatorForComponent(LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentWithoutAliasWithMultiKeyStore() throws Exception {
    Properties properties = configureSSLProperties(ALL.getConstant());

    properties.setProperty(SSL_KEYSTORE, TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKey.jks"));
    properties.setProperty(SSL_TRUSTSTORE, TestUtil.getResourcePath(getClass(), "/org/apache/geode/internal/net/multiKeyTrust.jks"));

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    setDistributionConfig(distributionConfig);

    assertTrue(getSocketCreatorForComponent(CLUSTER).useSSL());
    assertTrue(getSocketCreatorForComponent(GATEWAY).useSSL());
    assertTrue(getSocketCreatorForComponent(JMX).useSSL());
    assertTrue(getSocketCreatorForComponent(SERVER).useSSL());
    assertTrue(getSocketCreatorForComponent(WEB).useSSL());
    assertTrue(getSocketCreatorForComponent(LOCATOR).useSSL());
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

  private File findTestJKS() {
    return new File(TestUtil.getResourcePath(getClass(), "/ssl/trusted.keystore"));
  }
}
