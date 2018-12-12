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
package org.apache.geode.internal.net;

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CLUSTER_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_DEFAULT_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_GATEWAY_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_JMX_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_LOCATOR_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_SERVER_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_WEB_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.security.KeyStore;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.junit.categories.MembershipTest;

@Category(MembershipTest.class)
public class SSLConfigurationFactoryJUnitTest {

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @After
  public void tearDownTest() {
    SSLConfigurationFactory.close();
  }

  @Test
  public void getNonSSLConfiguration() {
    Properties properties = new Properties();
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);

    for (SecurableCommunicationChannel securableComponent : SecurableCommunicationChannel
        .values()) {
      assertSSLConfig(properties,
          SSLConfigurationFactory.getSSLConfigForComponent(securableComponent), securableComponent,
          distributionConfig);
    }
  }

  @Test
  public void getSSLConfigForComponentShouldThrowExceptionForUnknownComponents() {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, "none");

    assertThatThrownBy(() -> new DistributionConfigImpl(properties))
        .isInstanceOf(IllegalArgumentException.class)
        .hasCauseInstanceOf(GemFireConfigException.class)
        .hasMessageContaining("There is no registered component for the name: none");
  }

  @Test
  public void getSSLConfigWithCommaDelimitedProtocols() {
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

    assertSSLConfigForChannels(properties, distributionConfig);
  }

  @Test
  public void getSSLConfigWithCommaDelimitedCiphers() {
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

    assertSSLConfigForChannels(properties, distributionConfig);
  }

  @Test
  public void getSSLConfigForComponentALL() {
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

    assertSSLConfigForChannels(properties, distributionConfig);
  }

  @Test
  public void getSSLConfigForComponentHTTPService() {
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

    assertSSLConfigForChannels(properties, distributionConfig);
  }

  @Test
  public void getSSLConfigForComponentHTTPServiceWithAlias() {
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

    assertSSLConfigForChannels(properties, distributionConfig);
  }

  @Test
  public void getSSLConfigForComponentHTTPServiceWithMutualAuth() {
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

    assertSSLConfigForChannels(properties, distributionConfig);
  }

  @Test
  public void getSSLConfigUsingJavaProperties() {
    Properties properties = new Properties();
    properties.setProperty(CLUSTER_SSL_ENABLED, "true");
    properties.setProperty(MCAST_PORT, "0");
    System.setProperty(SSLConfigurationFactory.JAVAX_KEYSTORE, "keystore");
    System.setProperty(SSLConfigurationFactory.JAVAX_KEYSTORE_TYPE, KeyStore.getDefaultType());
    System.setProperty(SSLConfigurationFactory.JAVAX_KEYSTORE_PASSWORD, "keystorePassword");
    System.setProperty(SSLConfigurationFactory.JAVAX_TRUSTSTORE, "truststore");
    System.setProperty(SSLConfigurationFactory.JAVAX_TRUSTSTORE_PASSWORD, "truststorePassword");
    System.setProperty(SSLConfigurationFactory.JAVAX_TRUSTSTORE_TYPE, KeyStore.getDefaultType());
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);
    SSLConfig sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(SecurableCommunicationChannel.CLUSTER);

    assertThat(sslConfig.isEnabled()).isTrue();
    assertThat(sslConfig.getKeystore())
        .isEqualTo(System.getProperty(SSLConfigurationFactory.JAVAX_KEYSTORE));
    assertThat(sslConfig.getKeystorePassword())
        .isEqualTo(System.getProperty(SSLConfigurationFactory.JAVAX_KEYSTORE_PASSWORD));
    assertThat(sslConfig.getKeystoreType())
        .isEqualTo(System.getProperty(SSLConfigurationFactory.JAVAX_KEYSTORE_TYPE));
    assertThat(sslConfig.getTruststore())
        .isEqualTo(System.getProperty(SSLConfigurationFactory.JAVAX_TRUSTSTORE));
    assertThat(sslConfig.getTruststorePassword())
        .isEqualTo(System.getProperty(SSLConfigurationFactory.JAVAX_TRUSTSTORE_PASSWORD));
    assertThat(sslConfig.getTruststoreType())
        .isEqualTo(System.getProperty(SSLConfigurationFactory.JAVAX_TRUSTSTORE_TYPE));
    assertThat(sslConfig.isEnabled()).isTrue();
    assertThat(sslConfig.getKeystore())
        .isEqualTo(System.getProperty(SSLConfigurationFactory.JAVAX_KEYSTORE));
    assertThat(sslConfig.getKeystorePassword())
        .isEqualTo(System.getProperty(SSLConfigurationFactory.JAVAX_KEYSTORE_PASSWORD));
    assertThat(sslConfig.getKeystoreType())
        .isEqualTo(System.getProperty(SSLConfigurationFactory.JAVAX_KEYSTORE_TYPE));
    assertThat(sslConfig.getTruststore())
        .isEqualTo(System.getProperty(SSLConfigurationFactory.JAVAX_TRUSTSTORE));
    assertThat(sslConfig.getTruststorePassword())
        .isEqualTo(System.getProperty(SSLConfigurationFactory.JAVAX_TRUSTSTORE_PASSWORD));
    assertThat(sslConfig.getTruststoreType())
        .isEqualTo(System.getProperty(SSLConfigurationFactory.JAVAX_TRUSTSTORE_TYPE));
  }

  @Test
  public void getSSLHTTPMutualAuthenticationOffWithDefaultConfiguration() {
    Properties properties = new Properties();
    properties.setProperty(CLUSTER_SSL_ENABLED, "true");
    properties.setProperty(MCAST_PORT, "0");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);
    SSLConfig sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(SecurableCommunicationChannel.WEB);
    assertThat(sslConfig.isRequireAuth()).isFalse();
    assertThat(sslConfig.isEnabled()).isTrue();
    sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(SecurableCommunicationChannel.CLUSTER);
    assertThat(sslConfig.isRequireAuth()).isTrue();
    assertThat(sslConfig.isEnabled()).isTrue();
    sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(SecurableCommunicationChannel.GATEWAY);
    assertThat(sslConfig.isRequireAuth()).isTrue();
    assertThat(sslConfig.isEnabled()).isTrue();
    sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(SecurableCommunicationChannel.SERVER);
    assertThat(sslConfig.isRequireAuth()).isTrue();
    assertThat(sslConfig.isEnabled()).isTrue();
    sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(SecurableCommunicationChannel.JMX);
    assertThat(sslConfig.isRequireAuth()).isTrue();
    assertThat(sslConfig.isEnabled()).isTrue();
  }

  @Test
  public void setDistributionConfig() {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    properties.setProperty(SSL_KEYSTORE, "someKeyStore");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);

    SSLConfig sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(SecurableCommunicationChannel.LOCATOR);
    assertThat(sslConfig.isEnabled()).isTrue();
    assertThat(sslConfig.getKeystore()).isEqualTo("someKeyStore");

    properties.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.JMX.getConstant());
    properties.setProperty(SSL_KEYSTORE, "someOtherKeyStore");
    sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(properties,
        SecurableCommunicationChannel.LOCATOR);
    assertThat(sslConfig.isEnabled()).isFalse();
    assertThat(sslConfig.getKeystore()).isEqualTo("someOtherKeyStore");
  }

  private void assertSSLConfig(final Properties properties, final SSLConfig sslConfig,
      final SecurableCommunicationChannel expectedSecurableComponent,
      final DistributionConfigImpl distributionConfig) {
    assertThat(sslConfig.isEnabled()).isEqualTo(isSSLComponentEnabled(expectedSecurableComponent,
        distributionConfig.getSecurableCommunicationChannels()));

    if (sslConfig.isEnabled()) {
      assertThat(sslConfig.getKeystore()).isEqualTo(properties.getProperty(SSL_KEYSTORE));
      assertThat(sslConfig.getKeystorePassword())
          .isEqualTo(properties.getProperty(SSL_KEYSTORE_PASSWORD));
      assertThat(sslConfig.getKeystoreType()).isEqualTo(properties.getProperty(SSL_KEYSTORE_TYPE));
      assertThat(sslConfig.getTruststore()).isEqualTo(properties.getProperty(SSL_TRUSTSTORE));
      assertThat(sslConfig.getTruststorePassword())
          .isEqualTo(properties.getProperty(SSL_TRUSTSTORE_PASSWORD));
      assertThat(sslConfig.getCiphers())
          .isEqualTo(properties.getProperty(SSL_CIPHERS).replace(",", " "));
      assertThat(sslConfig.getProtocols())
          .isEqualTo(properties.getProperty(SSL_PROTOCOLS).replace(",", " "));
      assertThat(sslConfig.getAlias())
          .isEqualTo(getCorrectAlias(expectedSecurableComponent, properties));
      assertThat(sslConfig.isRequireAuth())
          .isEqualTo(requiresAuthentication(properties, expectedSecurableComponent));
      assertThat(sslConfig.getSecuredCommunicationChannel()).isEqualTo(expectedSecurableComponent);
    }
  }

  private void assertSSLConfigForChannels(final Properties distributionConfigProperties,
      final DistributionConfigImpl distributionConfig) {
    for (SecurableCommunicationChannel securableComponent : SecurableCommunicationChannel
        .values()) {
      assertSSLConfig(distributionConfigProperties,
          SSLConfigurationFactory.getSSLConfigForComponent(securableComponent), securableComponent,
          distributionConfig);
    }
  }

  private boolean requiresAuthentication(final Properties properties,
      final SecurableCommunicationChannel expectedSecurableComponent) {
    boolean defaultAuthentication =
        expectedSecurableComponent.equals(SecurableCommunicationChannel.WEB)
            ? DistributionConfig.DEFAULT_SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION
            : DistributionConfig.DEFAULT_SSL_REQUIRE_AUTHENTICATION;
    String httpRequiresAuthentication =
        properties.getProperty(SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION);

    return httpRequiresAuthentication == null ? defaultAuthentication
        : Boolean.parseBoolean(httpRequiresAuthentication);
  }

  private String getCorrectAlias(final SecurableCommunicationChannel expectedSecurableComponent,
      final Properties properties) {
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

  private String getAliasForComponent(final Properties properties,
      final String componentAliasProperty) {
    String aliasProperty = properties.getProperty(componentAliasProperty);
    return !StringUtils.isEmpty(aliasProperty) ? aliasProperty
        : properties.getProperty(SSL_DEFAULT_ALIAS);
  }

  private boolean isSSLComponentEnabled(
      final SecurableCommunicationChannel expectedSecurableComponent,
      final SecurableCommunicationChannel[] securableComponents) {
    for (SecurableCommunicationChannel securableCommunicationChannel : securableComponents) {
      if (SecurableCommunicationChannel.ALL.equals(securableCommunicationChannel)
          || securableCommunicationChannel.equals(expectedSecurableComponent)) {
        return true;
      }
    }

    return false;
  }
}
