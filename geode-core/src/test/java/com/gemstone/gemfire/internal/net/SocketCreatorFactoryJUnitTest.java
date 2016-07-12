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

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.SSLEnabledComponents;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SocketCreatorFactoryJUnitTest extends JSSESocketJUnitTest {

  @After
  public void tearDown() {
    SocketCreatorFactory.close();
  }

  @Test
  public void testClusterSSLConfig() {

  }

  @Test
  public void testNewSSLConfigSSLComponentALL() {
    Properties properties = configureSSLProperties(SSLEnabledComponents.ALL);

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertTrue(SocketCreatorFactory.getClusterSSLSocketCreator().useSSL());
    Assert.assertTrue(SocketCreatorFactory.getGatewaySSLSocketCreator().useSSL());
    Assert.assertTrue(SocketCreatorFactory.getJMXManagerSSLSocketCreator().useSSL());
    Assert.assertTrue(SocketCreatorFactory.getServerSSLSocketCreator().useSSL());
    Assert.assertTrue(SocketCreatorFactory.getHTTPServiceSSLSocketCreator().useSSL());

  }

  @Test
  public void testNewSSLConfigSSLComponentCLUSTER() {
    Properties properties = configureSSLProperties(SSLEnabledComponents.CLUSTER);

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertTrue(SocketCreatorFactory.getClusterSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getGatewaySSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getJMXManagerSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getServerSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getHTTPServiceSSLSocketCreator().useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentGATEWAY() {
    Properties properties = configureSSLProperties(SSLEnabledComponents.GATEWAY);

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertFalse(SocketCreatorFactory.getClusterSSLSocketCreator().useSSL());
    Assert.assertTrue(SocketCreatorFactory.getGatewaySSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getJMXManagerSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getServerSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getHTTPServiceSSLSocketCreator().useSSL());

  }

  @Test
  public void testNewSSLConfigSSLComponentHTTP_SERVICE() {
    Properties properties = configureSSLProperties(SSLEnabledComponents.HTTP_SERVICE);

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertFalse(SocketCreatorFactory.getClusterSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getGatewaySSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getJMXManagerSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getServerSSLSocketCreator().useSSL());
    Assert.assertTrue(SocketCreatorFactory.getHTTPServiceSSLSocketCreator().useSSL());

  }

  @Test
  public void testNewSSLConfigSSLComponentJMX() {
    Properties properties = configureSSLProperties(SSLEnabledComponents.JMX);

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertFalse(SocketCreatorFactory.getClusterSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getGatewaySSLSocketCreator().useSSL());
    Assert.assertTrue(SocketCreatorFactory.getJMXManagerSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getServerSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getHTTPServiceSSLSocketCreator().useSSL());

  }

  @Test
  public void testNewSSLConfigSSLComponentSERVER() {
    Properties properties = configureSSLProperties(SSLEnabledComponents.SERVER);

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertFalse(SocketCreatorFactory.getClusterSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getGatewaySSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getJMXManagerSSLSocketCreator().useSSL());
    Assert.assertTrue(SocketCreatorFactory.getServerSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getHTTPServiceSSLSocketCreator().useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentCombinations1() {
    Properties properties = configureSSLProperties(commaDelimetedString(SSLEnabledComponents.CLUSTER, SSLEnabledComponents.SERVER));

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertTrue(SocketCreatorFactory.getClusterSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getGatewaySSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getJMXManagerSSLSocketCreator().useSSL());
    Assert.assertTrue(SocketCreatorFactory.getServerSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getHTTPServiceSSLSocketCreator().useSSL());
  }

  @Test
  public void testNewSSLConfigSSLComponentCombinations2() {
    Properties properties = configureSSLProperties(commaDelimetedString(SSLEnabledComponents.CLUSTER, SSLEnabledComponents.SERVER, SSLEnabledComponents.HTTP_SERVICE, SSLEnabledComponents.JMX));

    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SocketCreatorFactory.setDistributionConfig(distributionConfig);

    Assert.assertTrue(SocketCreatorFactory.getClusterSSLSocketCreator().useSSL());
    Assert.assertFalse(SocketCreatorFactory.getGatewaySSLSocketCreator().useSSL());
    Assert.assertTrue(SocketCreatorFactory.getJMXManagerSSLSocketCreator().useSSL());
    Assert.assertTrue(SocketCreatorFactory.getServerSSLSocketCreator().useSSL());
    Assert.assertTrue(SocketCreatorFactory.getHTTPServiceSSLSocketCreator().useSSL());
  }

  private Properties configureSSLProperties(String sslComponents) {
    Properties properties = new Properties();
    try {
      File jks = findTestJKS();

      properties.setProperty(MCAST_PORT, "0");
      properties.setProperty(CLUSTER_SSL_ENABLED, "true");
      properties.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION, "true");
      properties.setProperty(CLUSTER_SSL_CIPHERS, "any");
      properties.setProperty(CLUSTER_SSL_PROTOCOLS, "TLSv1.2");
      properties.setProperty(CLUSTER_SSL_KEYSTORE, jks.getCanonicalPath());
      properties.setProperty(CLUSTER_SSL_KEYSTORE_PASSWORD, "password");
      properties.setProperty(CLUSTER_SSL_KEYSTORE_TYPE, "JKS");
      properties.setProperty(CLUSTER_SSL_TRUSTSTORE, jks.getCanonicalPath());
      properties.setProperty(CLUSTER_SSL_TRUSTSTORE_PASSWORD, "password");
      properties.setProperty(SSL_ENABLED_COMPONENTS, sslComponents);
    } catch (IOException e) {
      Assert.fail("Failed to configure the cluster");
    }
    return properties;
  }


  private String commaDelimetedString(final String... sslComponents) {
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
