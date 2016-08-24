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
package com.gemstone.gemfire.internal.net;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.internal.security.SecurableComponent;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import com.gemstone.gemfire.util.test.TestUtil;

@Category(UnitTest.class)
public class SocketCreatorFactoryJUnitTest extends JSSESocketJUnitTest {

  @Test
  public void testNewSSLConfigSSLComponentLocator() {
    Properties properties = configureSSLProperties(SecurableComponent.LOCATOR.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.LOCATOR).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.CLUSTER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.GATEWAY).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.JMX).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.SERVER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.HTTP_SERVICE).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentALL() {
    Properties properties = configureSSLProperties(SecurableComponent.ALL.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.CLUSTER).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.LOCATOR).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.GATEWAY).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.JMX).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.SERVER).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.HTTP_SERVICE).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentCLUSTER() {
    Properties properties = configureSSLProperties(SecurableComponent.CLUSTER.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.CLUSTER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.GATEWAY).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.JMX).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.SERVER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.HTTP_SERVICE).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentGATEWAY() {
    Properties properties = configureSSLProperties(SecurableComponent.GATEWAY.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.CLUSTER).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.GATEWAY).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.JMX).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.SERVER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.HTTP_SERVICE).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentHTTP_SERVICE() {
    Properties properties = configureSSLProperties(SecurableComponent.HTTP_SERVICE.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.CLUSTER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.GATEWAY).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.JMX).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.SERVER).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.HTTP_SERVICE).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentJMX() {
    Properties properties = configureSSLProperties(SecurableComponent.JMX.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.CLUSTER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.GATEWAY).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.JMX).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.SERVER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.HTTP_SERVICE).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentSERVER() {
    Properties properties = configureSSLProperties(SecurableComponent.SERVER.getConstant());

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.CLUSTER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.GATEWAY).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.JMX).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.SERVER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.HTTP_SERVICE).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentCombinations1() {
    Properties properties = configureSSLProperties(commaDelimitedString(SecurableComponent.CLUSTER.getConstant(), SecurableComponent.SERVER.getConstant()));

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.CLUSTER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.GATEWAY).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.JMX).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.SERVER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.HTTP_SERVICE).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentCombinations2() {
    Properties properties = configureSSLProperties(commaDelimitedString(SecurableComponent.CLUSTER.getConstant(), SecurableComponent.SERVER.getConstant(), SecurableComponent.HTTP_SERVICE.getConstant(), SecurableComponent.JMX.getConstant()));

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.CLUSTER).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.GATEWAY).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.JMX).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.SERVER).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.HTTP_SERVICE).useSSL());
    Assert.assertFalse(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentAliasWithMultiKeyStore() {
    Properties properties = configureSSLProperties(SecurableComponent.ALL.getConstant());

    properties.setProperty(SSL_KEYSTORE, TestUtil.getResourcePath(getClass(), "/com/gemstone/gemfire/internal/net/multiKey.jks"));
    properties.setProperty(SSL_TRUSTSTORE, TestUtil.getResourcePath(getClass(), "/com/gemstone/gemfire/internal/net/multiKeyTrust.jks"));

    properties.setProperty(SSL_CLUSTER_ALIAS, "clusterKey");
    properties.setProperty(SSL_DEFAULT_ALIAS, "serverKey");

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.CLUSTER).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.GATEWAY).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.JMX).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.SERVER).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.HTTP_SERVICE).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.LOCATOR).useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentWithoutAliasWithMultiKeyStore() {
    Properties properties = configureSSLProperties(SecurableComponent.ALL.getConstant());

    properties.setProperty(SSL_KEYSTORE, TestUtil.getResourcePath(getClass(), "/com/gemstone/gemfire/internal/net/multiKey.jks"));
    properties.setProperty(SSL_TRUSTSTORE, TestUtil.getResourcePath(getClass(), "/com/gemstone/gemfire/internal/net/multiKeyTrust.jks"));

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.CLUSTER).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.GATEWAY).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.JMX).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.SERVER).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.HTTP_SERVICE).useSSL());
    Assert.assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableComponent.LOCATOR).useSSL());
  }

  private Properties configureSSLProperties(String sslComponents) {
    Properties properties = new Properties();
    try {
      File jks = findTestJKS();

      properties.setProperty(MCAST_PORT, "0");
      properties.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
      properties.setProperty(SSL_CIPHERS, "any");
      properties.setProperty(SSL_PROTOCOLS, "TLSv1.2");
      properties.setProperty(SSL_KEYSTORE, jks.getCanonicalPath());
      properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
      properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
      properties.setProperty(SSL_TRUSTSTORE, jks.getCanonicalPath());
      properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
      properties.setProperty(SSL_ENABLED_COMPONENTS, sslComponents);
    } catch (IOException e) {
      Assert.fail("Failed to configure the cluster");
    }
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
  public void testLegacyServerSSLConfig() {
  }

  @Test
  public void testLegacyJMXSSLConfig() {
  }

  @Test
  public void testLegacyGatewaySSLConfig() {
  }

  @Test
  public void testLegacyHttpServiceSSLConfig() {
  }

}
