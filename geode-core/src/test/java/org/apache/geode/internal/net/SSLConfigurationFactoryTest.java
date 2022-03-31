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

import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.GATEWAY_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CLIENT_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CLUSTER_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_DEFAULT_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_GATEWAY_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_JMX_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_LOCATOR_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_SERVER_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_SERVER_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_WEB_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

@Tag("membership")
public class SSLConfigurationFactoryTest {

  @Test
  public void getNonSSLConfiguration() {
    Properties properties = new Properties();
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    for (SecurableCommunicationChannel securableComponent : SecurableCommunicationChannel
        .values()) {
      assertSSLConfig(properties,
          SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig, securableComponent),
          securableComponent,
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
    final Properties properties =
        createSSLProperties("all", "Cipher1,Cipher2", "Protocol1,Protocol2");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    assertSSLConfigForChannels(properties, distributionConfig);
  }

  @Test
  public void getSSLConfigWithCommaDelimitedCiphers() {
    final Properties properties = createSSLProperties("all", "Cipher1,Cipher2", "any");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    assertSSLConfigForChannels(properties, distributionConfig);
  }

  @Test
  public void getSSLConfigForComponentALL() {
    final Properties properties = createSSLProperties("all", "any", "any");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    assertSSLConfigForChannels(properties, distributionConfig);
  }

  @Test
  public void getSSLConfigForComponentHTTPService() {
    final Properties properties =
        createSSLProperties(SecurableCommunicationChannel.WEB.getConstant(), "any", "any");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    assertSSLConfigForChannels(properties, distributionConfig);
  }

  @Test
  public void getSSLConfigForComponentHTTPServiceWithAlias() {
    final Properties properties =
        createSSLProperties(SecurableCommunicationChannel.WEB.getConstant(), "any", "any");
    properties.setProperty(SSL_WEB_ALIAS, "httpAlias");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    assertSSLConfigForChannels(properties, distributionConfig);
  }

  @Test
  public void getSSLConfigForComponentHTTPServiceWithMutualAuth() {
    final Properties properties =
        createSSLProperties(SecurableCommunicationChannel.WEB.getConstant(), "any", "any");
    properties.setProperty(SSL_WEB_ALIAS, "httpAlias");
    properties.setProperty(SSL_WEB_SERVICE_REQUIRE_AUTHENTICATION, "true");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    assertSSLConfigForChannels(properties, distributionConfig);
  }

  @Test
  public void getSSLHTTPMutualAuthenticationOffWithDefaultConfiguration() {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfig sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig,
            SecurableCommunicationChannel.WEB);
    assertThat(sslConfig.isRequireAuth()).isFalse();
    assertThat(sslConfig.isEnabled()).isTrue();
    sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig,
            SecurableCommunicationChannel.CLUSTER);
    assertThat(sslConfig.isRequireAuth()).isTrue();
    assertThat(sslConfig.isEnabled()).isTrue();
    sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig,
            SecurableCommunicationChannel.GATEWAY);
    assertThat(sslConfig.isRequireAuth()).isTrue();
    assertThat(sslConfig.isEnabled()).isTrue();
    sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig,
            SecurableCommunicationChannel.SERVER);
    assertThat(sslConfig.isRequireAuth()).isTrue();
    assertThat(sslConfig.isEnabled()).isTrue();
    sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig,
        SecurableCommunicationChannel.JMX);
    assertThat(sslConfig.isRequireAuth()).isTrue();
    assertThat(sslConfig.isEnabled()).isTrue();
  }

  @Test
  public void setDistributionConfig() {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    properties.setProperty(SSL_KEYSTORE, "someKeyStore");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    SSLConfig sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig,
            SecurableCommunicationChannel.LOCATOR);
    assertThat(sslConfig.isEnabled()).isTrue();
    assertThat(sslConfig.getKeystore()).isEqualTo("someKeyStore");

    properties.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.JMX.getConstant());
    properties.setProperty(SSL_KEYSTORE, "someOtherKeyStore");
    sslConfig = SSLConfigurationFactory.getSSLConfigForComponent(properties,
        SecurableCommunicationChannel.LOCATOR);
    assertThat(sslConfig.isEnabled()).isFalse();
    assertThat(sslConfig.getKeystore()).isEqualTo("someOtherKeyStore");
  }

  @Test
  void createSSLConfigBuilderSetsSSLClientProtocols() {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    properties.setProperty(SSL_CLIENT_PROTOCOLS, "SomeClientProtocol");
    final DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    final SSLConfig sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig,
            SecurableCommunicationChannel.LOCATOR);

    assertThat(sslConfig.getClientProtocols()).isEqualTo("SomeClientProtocol");
  }

  @Test
  void createSSLConfigBuilderSetsSSLServerProtocols() {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    properties.setProperty(SSL_SERVER_PROTOCOLS, "SomeServerProtocol");
    final DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);

    final SSLConfig sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig,
            SecurableCommunicationChannel.LOCATOR);

    assertThat(sslConfig.getServerProtocols()).isEqualTo("SomeServerProtocol");
  }

  @Test
  void testLegacyGatewaySSLConfig() {
    final Properties properties = createLegacyGatewaySSLProperties();
    final SecurableCommunicationChannel securableComponent = SecurableCommunicationChannel.GATEWAY;

    final SSLConfig sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(new DistributionConfigImpl(properties),
            securableComponent);

    assertLegacyGatewaySSLConfig(properties, sslConfig, securableComponent);
    assertThat(sslConfig.doEndpointIdentification()).isEqualTo(false);
  }

  @Test
  void testLegacyGatewaySSLConfigWithEndpointIdentificationEnabled() {
    final Properties properties = new Properties();
    properties.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");
    final SecurableCommunicationChannel securableComponent = SecurableCommunicationChannel.GATEWAY;

    final SSLConfig sslConfig =
        SSLConfigurationFactory.getSSLConfigForComponent(new DistributionConfigImpl(properties),
            securableComponent);

    assertThat(sslConfig.doEndpointIdentification()).isEqualTo(true);
  }

  private Properties createSSLProperties(String component, String ciphers, String protocols) {
    final Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, component);
    properties.setProperty(SSL_KEYSTORE, "someKeyStore");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE, "someKeyStore");
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_DEFAULT_ALIAS, "defaultAlias");
    properties.setProperty(SSL_CIPHERS, ciphers);
    properties.setProperty(SSL_PROTOCOLS, protocols);
    return properties;
  }

  private Properties createLegacyGatewaySSLProperties() {
    final Properties properties = new Properties();
    properties.setProperty(GATEWAY_SSL_ENABLED, "true");
    properties.setProperty(GATEWAY_SSL_KEYSTORE, "someKeyStore");
    properties.setProperty(GATEWAY_SSL_KEYSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(GATEWAY_SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(GATEWAY_SSL_TRUSTSTORE, "someKeyStore");
    properties.setProperty(GATEWAY_SSL_TRUSTSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(GATEWAY_SSL_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(GATEWAY_SSL_CIPHERS, "any");
    properties.setProperty(GATEWAY_SSL_PROTOCOLS, "any");
    return properties;
  }

  private void assertLegacyGatewaySSLConfig(final Properties properties, final SSLConfig sslConfig,
      final SecurableCommunicationChannel expectedComponent) {
    assertThat(sslConfig.isEnabled())
        .isEqualTo(Boolean.parseBoolean(properties.getProperty(GATEWAY_SSL_ENABLED)));
    assertThat(sslConfig.getKeystore()).isEqualTo(properties.getProperty(GATEWAY_SSL_KEYSTORE));
    assertThat(sslConfig.getKeystorePassword())
        .isEqualTo(properties.getProperty(GATEWAY_SSL_KEYSTORE_PASSWORD));
    assertThat(sslConfig.getKeystoreType())
        .isEqualTo(properties.getProperty(GATEWAY_SSL_KEYSTORE_TYPE));
    assertThat(sslConfig.getTruststore()).isEqualTo(properties.getProperty(GATEWAY_SSL_TRUSTSTORE));
    assertThat(sslConfig.getTruststorePassword())
        .isEqualTo(properties.getProperty(GATEWAY_SSL_TRUSTSTORE_PASSWORD));
    assertThat(sslConfig.getCiphers()).isEqualTo(properties.getProperty(GATEWAY_SSL_CIPHERS));
    assertThat(sslConfig.getClientProtocols())
        .isEqualTo(properties.getProperty(GATEWAY_SSL_PROTOCOLS));
    assertThat(sslConfig.getServerProtocols())
        .isEqualTo(properties.getProperty(GATEWAY_SSL_PROTOCOLS));
    assertThat(sslConfig.isRequireAuth()).isEqualTo(
        Boolean.parseBoolean(properties.getProperty(GATEWAY_SSL_REQUIRE_AUTHENTICATION)));
    assertThat(sslConfig.getSecuredCommunicationChannel()).isEqualTo(expectedComponent);
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
      assertThat(sslConfig.getClientProtocols())
          .isEqualTo(properties.getProperty(SSL_PROTOCOLS).replace(",", " "));
      assertThat(sslConfig.getServerProtocols())
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
          SSLConfigurationFactory.getSSLConfigForComponent(distributionConfig, securableComponent),
          securableComponent,
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
      case ALL:
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
