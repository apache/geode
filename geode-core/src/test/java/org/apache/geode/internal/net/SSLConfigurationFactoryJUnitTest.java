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

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SSLConfigurationFactoryJUnitTest {

  @After
  public void tearDownTest() {
    SSLConfigurationFactory.close();
  }

  @Test
  public void getSSLConfigWithCommaDelimitedProtocols() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    properties.setProperty(SSL_KEYSTORE, "someKeyStore");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE, "someKeyStore");
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_DEFAULT_ALIAS, "defaultAlias");
    properties.setProperty(SSL_CIPHERS, "Cipher1,Cipher2");
    properties.setProperty(SSL_PROTOCOLS, "Protocol1,Protocol2");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);
    for (SecurableCommunicationChannel securableComponent : SecurableCommunicationChannel.values()) {
      assertSSLConfig(properties, SSLConfigurationFactory.getSSLConfigForComponent(securableComponent), securableComponent, distributionConfig);
    }
  }

  @Test
  public void getSSLConfigWithCommaDelimitedCiphers() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    properties.setProperty(SSL_KEYSTORE, "someKeyStore");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE, "someKeyStore");
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_DEFAULT_ALIAS, "defaultAlias");
    properties.setProperty(SSL_CIPHERS, "Cipher1,Cipher2");
    properties.setProperty(SSL_PROTOCOLS, "any");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);
    for (SecurableCommunicationChannel securableCommunicationChannel : SecurableCommunicationChannel.values()) {
      assertSSLConfig(properties, SSLConfigurationFactory.getSSLConfigForComponent(securableCommunicationChannel), securableCommunicationChannel, distributionConfig);
    }
  }

  @Test
  public void getSSLConfigForComponentALL() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    properties.setProperty(SSL_KEYSTORE, "someKeyStore");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE, "someKeyStore");
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_DEFAULT_ALIAS, "defaultAlias");
    properties.setProperty(SSL_CIPHERS, "any");
    properties.setProperty(SSL_PROTOCOLS, "any");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);
    for (SecurableCommunicationChannel securableCommunicationChannel : SecurableCommunicationChannel.values()) {
      assertSSLConfig(properties, SSLConfigurationFactory.getSSLConfigForComponent(securableCommunicationChannel), securableCommunicationChannel, distributionConfig);
    }
  }

  @Test
  public void getSSLConfigForComponentHTTPService() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());
    properties.setProperty(SSL_KEYSTORE, "someKeyStore");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE, "someKeyStore");
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_DEFAULT_ALIAS, "defaultAlias");
    properties.setProperty(SSL_CIPHERS, "any");
    properties.setProperty(SSL_PROTOCOLS, "any");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);
    for (SecurableCommunicationChannel securableCommunicationChannel : SecurableCommunicationChannel.values()) {
      assertSSLConfig(properties, SSLConfigurationFactory.getSSLConfigForComponent(securableCommunicationChannel), securableCommunicationChannel, distributionConfig);
    }
  }

  @Test
  public void getSSLConfigForComponentHTTPServiceWithAlias() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());
    properties.setProperty(SSL_KEYSTORE, "someKeyStore");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE, "someKeyStore");
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_DEFAULT_ALIAS, "defaultAlias");
    properties.setProperty(SSL_WEB_ALIAS, "httpAlias");
    properties.setProperty(SSL_CIPHERS, "any");
    properties.setProperty(SSL_PROTOCOLS, "any");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);
    for (SecurableCommunicationChannel securableCommunicationChannel : SecurableCommunicationChannel.values()) {
      assertSSLConfig(properties, SSLConfigurationFactory.getSSLConfigForComponent(securableCommunicationChannel), securableCommunicationChannel, distributionConfig);
    }
  }

  @Test
  public void getSSLConfigForComponentHTTPServiceWithMutualAuth() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());
    properties.setProperty(SSL_KEYSTORE, "someKeyStore");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE, "someKeyStore");
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_DEFAULT_ALIAS, "defaultAlias");
    properties.setProperty(SSL_WEB_ALIAS, "httpAlias");
    properties.setProperty(SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(SSL_CIPHERS, "any");
    properties.setProperty(SSL_PROTOCOLS, "any");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);
    for (SecurableCommunicationChannel securableCommunicationChannel : SecurableCommunicationChannel.values()) {
      assertSSLConfig(properties, SSLConfigurationFactory.getSSLConfigForComponent(securableCommunicationChannel), securableCommunicationChannel, distributionConfig);
    }
  }

  private void assertSSLConfig(final Properties properties,
                               final SSLConfig sslConfig,
                               final SecurableCommunicationChannel expectedSecurableComponent,
                               final DistributionConfigImpl distributionConfig) {
    assertEquals(isSSLComponentEnabled(expectedSecurableComponent, distributionConfig.getSecurableCommunicationChannels()), sslConfig.isEnabled());
    assertEquals(properties.getProperty(SSL_KEYSTORE), sslConfig.getKeystore());
    assertEquals(properties.getProperty(SSL_KEYSTORE_PASSWORD), sslConfig.getKeystorePassword());
    assertEquals(properties.getProperty(SSL_KEYSTORE_TYPE), sslConfig.getKeystoreType());
    assertEquals(properties.getProperty(SSL_TRUSTSTORE), sslConfig.getTruststore());
    assertEquals(properties.getProperty(SSL_TRUSTSTORE_PASSWORD), sslConfig.getTruststorePassword());
    assertEquals(properties.getProperty(SSL_CIPHERS).replace(","," "), sslConfig.getCiphers());
    assertEquals(properties.getProperty(SSL_PROTOCOLS).replace(","," "), sslConfig.getProtocols());
    assertEquals(getCorrectAlias(expectedSecurableComponent, properties), sslConfig.getAlias());
    assertEquals(requiresAuthentication(properties, expectedSecurableComponent), sslConfig.isRequireAuth());
    assertEquals(expectedSecurableComponent, sslConfig.getSecuredCommunicationChannel());
  }

  private boolean requiresAuthentication(final Properties properties, final SecurableCommunicationChannel expectedSecurableComponent) {
    boolean defaultAuthentication = expectedSecurableComponent.equals(SecurableCommunicationChannel.WEB) ? DistributionConfig.DEFAULT_SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION : DistributionConfig.DEFAULT_SSL_REQUIRE_AUTHENTICATION;
    String httpRequiresAuthentication = properties.getProperty(SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION);

    return httpRequiresAuthentication == null ? defaultAuthentication : Boolean.parseBoolean(httpRequiresAuthentication);
  }

  private String getCorrectAlias(final SecurableCommunicationChannel expectedSecurableComponent, final Properties properties) {
    switch (expectedSecurableComponent) {
      case ALL:
        return properties.getProperty(SSL_DEFAULT_ALIAS);
      case CLUSTER:
        return getAliasForComponent(properties, SSL_CLUSTER_ALIAS);
      case GATEWAY:
        return getAliasForComponent(properties, SSL_GATEWAY_ALIAS);
      case WEB:
        return getAliasForComponent(properties, SSL_WEB_ALIAS);
      case JMX:
        return getAliasForComponent(properties, SSL_JMX_ALIAS);
      case LOCATOR:
        return getAliasForComponent(properties, SSL_LOCATOR_ALIAS);
      case SERVER:
        return getAliasForComponent(properties, SSL_SERVER_ALIAS);
      default:
        return properties.getProperty(SSL_DEFAULT_ALIAS);
    }
  }

  private String getAliasForComponent(final Properties properties, final String componentAliasProperty) {
    String aliasProperty = properties.getProperty(componentAliasProperty);
    return !StringUtils.isEmpty(aliasProperty) ? aliasProperty : properties.getProperty(SSL_DEFAULT_ALIAS);
  }

  private boolean isSSLComponentEnabled(final SecurableCommunicationChannel expectedSecurableComponent, final SecurableCommunicationChannel[] securableComponents) {
    for (SecurableCommunicationChannel securableCommunicationChannel : securableComponents) {
      if (SecurableCommunicationChannel.ALL.equals(securableCommunicationChannel) || securableCommunicationChannel.equals(expectedSecurableComponent)) {
        return true;
      }
    }
    return false;
  }

}