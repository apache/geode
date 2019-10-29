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

import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

public class SSLConfigurationFactory {
  public static final String JAVAX_KEYSTORE = "javax.net.ssl.keyStore";
  public static final String JAVAX_KEYSTORE_TYPE = "javax.net.ssl.keyStoreType";
  public static final String JAVAX_KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword";
  public static final String JAVAX_TRUSTSTORE = "javax.net.ssl.trustStore";
  public static final String JAVAX_TRUSTSTORE_TYPE = "javax.net.ssl.trustStoreType";
  public static final String JAVAX_TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";
  public static final String GEODE_SSL_CONFIG_PROPERTIES =
      "org.apache.geode.internal.net.ssl.config";

  @MakeNotStatic
  private static SSLConfigurationFactory instance = new SSLConfigurationFactory();

  private SSLConfigurationFactory() {}

  private static synchronized SSLConfigurationFactory getInstance() {
    if (instance == null) {
      instance = new SSLConfigurationFactory();
    }
    return instance;
  }

  private SSLConfig createSSLConfigForComponent(final DistributionConfig distributionConfig,
      final SecurableCommunicationChannel sslEnabledComponent) {
    SSLConfig.Builder sslConfigBuilder =
        createSSLConfigBuilder(distributionConfig, sslEnabledComponent);
    SecurableCommunicationChannel[] sslEnabledComponents =
        distributionConfig.getSecurableCommunicationChannels();
    if (sslEnabledComponents.length == 0) {
      configureLegacyClusterSSL(distributionConfig, sslConfigBuilder);
    }
    sslConfigBuilder.setSecurableCommunicationChannel(sslEnabledComponent);
    switch (sslEnabledComponent) {
      case ALL: {
        break;
      }
      case CLUSTER: {
        if (sslEnabledComponents.length > 0) {
          setAliasForComponent(sslConfigBuilder, distributionConfig.getClusterSSLAlias());
        } else {
          configureLegacyClusterSSL(distributionConfig, sslConfigBuilder);
        }
        break;
      }
      case LOCATOR: {
        if (sslEnabledComponents.length > 0) {
          setAliasForComponent(sslConfigBuilder, distributionConfig.getLocatorSSLAlias());
        }
        break;
      }
      case SERVER: {
        if (sslEnabledComponents.length > 0) {
          setAliasForComponent(sslConfigBuilder, distributionConfig.getServerSSLAlias());
        } else {
          configureLegacyServerSSL(distributionConfig, sslConfigBuilder);
        }
        break;
      }
      case GATEWAY: {
        if (sslEnabledComponents.length > 0) {
          setAliasForComponent(sslConfigBuilder, distributionConfig.getGatewaySSLAlias());
        } else {
          configureLegacyGatewaySSL(distributionConfig, sslConfigBuilder);
        }
        break;
      }
      case WEB: {
        if (sslEnabledComponents.length > 0) {
          setAliasForComponent(sslConfigBuilder, distributionConfig.getHTTPServiceSSLAlias());
          sslConfigBuilder.setRequireAuth(distributionConfig.getSSLWebRequireAuthentication());
        } else {
          configureLegacyHttpServiceSSL(distributionConfig, sslConfigBuilder);
        }
        break;
      }
      case JMX: {
        if (sslEnabledComponents.length > 0) {
          setAliasForComponent(sslConfigBuilder, distributionConfig.getJMXSSLAlias());
        } else {
          configureLegacyJMXSSL(distributionConfig, sslConfigBuilder);
        }
        break;
      }
    }
    configureSSLPropertiesFromSystemProperties(sslConfigBuilder);
    return sslConfigBuilder.build();
  }

  private SSLConfig.Builder setAliasForComponent(SSLConfig.Builder sslConfigBuilder,
      final String clusterSSLAlias) {
    if (!StringUtils.isEmpty(clusterSSLAlias)) {
      sslConfigBuilder.setAlias(clusterSSLAlias);
    }
    return sslConfigBuilder;
  }

  private SSLConfig.Builder createSSLConfigBuilder(final DistributionConfig distributionConfig,
      final SecurableCommunicationChannel sslEnabledComponent) {
    SSLConfig.Builder sslConfigBuilder = new SSLConfig.Builder();
    sslConfigBuilder.setCiphers(distributionConfig.getSSLCiphers());
    sslConfigBuilder
        .setEndpointIdentificationEnabled(distributionConfig.getSSLEndPointIdentificationEnabled());
    sslConfigBuilder
        .setEnabled(determineIfSSLEnabledForSSLComponent(distributionConfig, sslEnabledComponent));
    sslConfigBuilder.setKeystore(distributionConfig.getSSLKeyStore());
    sslConfigBuilder.setKeystorePassword(distributionConfig.getSSLKeyStorePassword());
    sslConfigBuilder.setKeystoreType(distributionConfig.getSSLKeyStoreType());
    sslConfigBuilder.setTruststore(distributionConfig.getSSLTrustStore());
    sslConfigBuilder.setTruststorePassword(distributionConfig.getSSLTrustStorePassword());
    sslConfigBuilder.setTruststoreType(distributionConfig.getSSLTrustStoreType());
    sslConfigBuilder.setProtocols(distributionConfig.getSSLProtocols());
    sslConfigBuilder.setRequireAuth(distributionConfig.getSSLRequireAuthentication());
    sslConfigBuilder.setAlias(distributionConfig.getSSLDefaultAlias());
    sslConfigBuilder.setUseDefaultSSLContext(distributionConfig.getSSLUseDefaultContext());

    return sslConfigBuilder;
  }

  private boolean determineIfSSLEnabledForSSLComponent(final DistributionConfig distributionConfig,
      final SecurableCommunicationChannel sslEnabledComponent) {
    if (ArrayUtils.contains(distributionConfig.getSecurableCommunicationChannels(),
        SecurableCommunicationChannel.ALL)) {
      return true;
    }

    return ArrayUtils.contains(distributionConfig.getSecurableCommunicationChannels(),
        sslEnabledComponent);
  }

  /**
   * Configure a SSLConfig.Builder for the cluster using the legacy configuration
   *
   * @return A SSLConfig.Builder object describing the ssl config for the server component
   * @deprecated as of Geode 1.0
   */
  private SSLConfig.Builder configureLegacyClusterSSL(final DistributionConfig distributionConfig,
      SSLConfig.Builder sslConfigBuilder) {
    sslConfigBuilder.setCiphers(distributionConfig.getClusterSSLCiphers());
    sslConfigBuilder.setEnabled(distributionConfig.getClusterSSLEnabled());
    sslConfigBuilder.setKeystore(distributionConfig.getClusterSSLKeyStore());
    sslConfigBuilder.setKeystorePassword(distributionConfig.getClusterSSLKeyStorePassword());
    sslConfigBuilder.setKeystoreType(distributionConfig.getClusterSSLKeyStoreType());
    sslConfigBuilder.setTruststore(distributionConfig.getClusterSSLTrustStore());
    sslConfigBuilder.setTruststorePassword(distributionConfig.getClusterSSLTrustStorePassword());
    sslConfigBuilder.setTruststoreType(distributionConfig.getClusterSSLKeyStoreType());
    sslConfigBuilder.setProtocols(distributionConfig.getClusterSSLProtocols());
    sslConfigBuilder.setRequireAuth(distributionConfig.getClusterSSLRequireAuthentication());
    return sslConfigBuilder;
  }

  /**
   * Configure a SSLConfig.Builder for the server using the legacy configuration
   *
   * @return A SSLConfig.Builder object describing the ssl config for the server component
   * @deprecated as of Geode 1.0
   */
  private SSLConfig.Builder configureLegacyServerSSL(final DistributionConfig distributionConfig,
      final SSLConfig.Builder sslConfigBuilder) {
    sslConfigBuilder.setCiphers(distributionConfig.getServerSSLCiphers());
    sslConfigBuilder.setEnabled(distributionConfig.getServerSSLEnabled());
    sslConfigBuilder.setKeystore(distributionConfig.getServerSSLKeyStore());
    sslConfigBuilder.setKeystorePassword(distributionConfig.getServerSSLKeyStorePassword());
    sslConfigBuilder.setKeystoreType(distributionConfig.getServerSSLKeyStoreType());
    sslConfigBuilder.setTruststore(distributionConfig.getServerSSLTrustStore());
    sslConfigBuilder.setTruststorePassword(distributionConfig.getServerSSLTrustStorePassword());
    sslConfigBuilder.setTruststoreType(distributionConfig.getServerSSLKeyStoreType());
    sslConfigBuilder.setProtocols(distributionConfig.getServerSSLProtocols());
    sslConfigBuilder.setRequireAuth(distributionConfig.getServerSSLRequireAuthentication());
    return sslConfigBuilder;
  }

  /**
   * Configure a SSLConfig.Builder for the jmx using the legacy configuration
   *
   * @return A SSLConfig.Builder object describing the ssl config for the jmx component
   * @deprecated as of Geode 1.0
   */
  private SSLConfig.Builder configureLegacyJMXSSL(final DistributionConfig distributionConfig,
      SSLConfig.Builder sslConfigBuilder) {
    sslConfigBuilder.setCiphers(distributionConfig.getJmxManagerSSLCiphers());
    sslConfigBuilder.setEnabled(distributionConfig.getJmxManagerSSLEnabled());
    sslConfigBuilder.setKeystore(distributionConfig.getJmxManagerSSLKeyStore());
    sslConfigBuilder.setKeystorePassword(distributionConfig.getJmxManagerSSLKeyStorePassword());
    sslConfigBuilder.setKeystoreType(distributionConfig.getJmxManagerSSLKeyStoreType());
    sslConfigBuilder.setTruststore(distributionConfig.getJmxManagerSSLTrustStore());
    sslConfigBuilder.setTruststorePassword(distributionConfig.getJmxManagerSSLTrustStorePassword());
    sslConfigBuilder.setTruststoreType(distributionConfig.getJmxManagerSSLKeyStoreType());
    sslConfigBuilder.setProtocols(distributionConfig.getJmxManagerSSLProtocols());
    sslConfigBuilder.setRequireAuth(distributionConfig.getJmxManagerSSLRequireAuthentication());
    return sslConfigBuilder;
  }

  /**
   * Configure a SSLConfig.Builder for the gateway using the legacy configuration
   *
   * @return A sslConfig object describing the ssl config for the gateway component
   * @deprecated as of Geode 1.0
   */
  private SSLConfig.Builder configureLegacyGatewaySSL(final DistributionConfig distributionConfig,
      SSLConfig.Builder sslConfigBuilder) {
    sslConfigBuilder.setCiphers(distributionConfig.getGatewaySSLCiphers());
    sslConfigBuilder.setEnabled(distributionConfig.getGatewaySSLEnabled());
    sslConfigBuilder.setKeystore(distributionConfig.getGatewaySSLKeyStore());
    sslConfigBuilder.setKeystorePassword(distributionConfig.getGatewaySSLKeyStorePassword());
    sslConfigBuilder.setKeystoreType(distributionConfig.getGatewaySSLKeyStoreType());
    sslConfigBuilder.setTruststore(distributionConfig.getGatewaySSLTrustStore());
    sslConfigBuilder.setTruststorePassword(distributionConfig.getGatewaySSLTrustStorePassword());
    sslConfigBuilder.setProtocols(distributionConfig.getGatewaySSLProtocols());
    sslConfigBuilder.setRequireAuth(distributionConfig.getGatewaySSLRequireAuthentication());
    return sslConfigBuilder;
  }

  /**
   * Configure a SSLConfig.Builder for the http service using the legacy configuration
   *
   * @return A SSLConfig.Builder object describing the ssl config for the http service component
   * @deprecated as of Geode 1.0
   */
  private SSLConfig.Builder configureLegacyHttpServiceSSL(
      final DistributionConfig distributionConfig,
      SSLConfig.Builder sslConfigBuilder) {
    sslConfigBuilder.setCiphers(distributionConfig.getHttpServiceSSLCiphers());
    sslConfigBuilder.setEnabled(distributionConfig.getHttpServiceSSLEnabled());
    sslConfigBuilder.setKeystore(distributionConfig.getHttpServiceSSLKeyStore());
    sslConfigBuilder.setKeystorePassword(distributionConfig.getHttpServiceSSLKeyStorePassword());
    sslConfigBuilder.setKeystoreType(distributionConfig.getHttpServiceSSLKeyStoreType());
    sslConfigBuilder.setTruststore(distributionConfig.getHttpServiceSSLTrustStore());
    sslConfigBuilder
        .setTruststorePassword(distributionConfig.getHttpServiceSSLTrustStorePassword());
    sslConfigBuilder.setTruststoreType(distributionConfig.getHttpServiceSSLKeyStoreType());
    sslConfigBuilder.setProtocols(distributionConfig.getHttpServiceSSLProtocols());
    sslConfigBuilder.setRequireAuth(distributionConfig.getHttpServiceSSLRequireAuthentication());
    return sslConfigBuilder;
  }

  private SSLConfig.Builder configureSSLPropertiesFromSystemProperties(
      SSLConfig.Builder sslConfigBuilder) {
    return configureSSLPropertiesFromSystemProperties(sslConfigBuilder, null);
  }

  private SSLConfig.Builder configureSSLPropertiesFromSystemProperties(
      SSLConfig.Builder sslConfigBuilder,
      Properties properties) {
    if (StringUtils.isEmpty(sslConfigBuilder.getKeystore())) {
      sslConfigBuilder.setKeystore(getValueFromSystemProperties(properties, JAVAX_KEYSTORE));
    }
    if (StringUtils.isEmpty(sslConfigBuilder.getKeystoreType())) {
      sslConfigBuilder
          .setKeystoreType(getValueFromSystemProperties(properties, JAVAX_KEYSTORE_TYPE));
    }
    if (StringUtils.isEmpty(sslConfigBuilder.getKeystorePassword())) {
      sslConfigBuilder
          .setKeystorePassword(getValueFromSystemProperties(properties, JAVAX_KEYSTORE_PASSWORD));
    }
    if (StringUtils.isEmpty(sslConfigBuilder.getTruststore())) {
      sslConfigBuilder.setTruststore(getValueFromSystemProperties(properties, JAVAX_TRUSTSTORE));
    }
    if (StringUtils.isEmpty(sslConfigBuilder.getTruststorePassword())) {
      sslConfigBuilder.setTruststorePassword(
          getValueFromSystemProperties(properties, JAVAX_TRUSTSTORE_PASSWORD));
    }
    if (StringUtils.isEmpty(sslConfigBuilder.getTruststoreType())) {
      sslConfigBuilder
          .setTruststoreType(getValueFromSystemProperties(properties, JAVAX_TRUSTSTORE_TYPE));
    }
    return sslConfigBuilder;
  }

  private String getValueFromSystemProperties(final Properties properties, String property) {
    String propertyValue = null;
    if (properties != null) {
      propertyValue = properties.getProperty(property);
    }
    if (property != null) {
      propertyValue = System.getProperty(property);
      if (propertyValue != null) {
        if (propertyValue.trim().equals("")) {
          propertyValue = System.getenv(property);
        }
      }
    }
    return propertyValue;
  }

  @Deprecated
  public static SSLConfig getSSLConfigForComponent(final boolean useSSL,
      final boolean needClientAuth, final String protocols, final String ciphers,
      final Properties gfsecurityProps, final String alias) {
    SSLConfig.Builder sslConfigBuilder = new SSLConfig.Builder();
    sslConfigBuilder.setAlias(alias);
    sslConfigBuilder.setCiphers(ciphers);
    sslConfigBuilder.setProtocols(protocols);
    sslConfigBuilder.setRequireAuth(needClientAuth);
    sslConfigBuilder.setEnabled(useSSL);

    getInstance().configureSSLPropertiesFromSystemProperties(sslConfigBuilder, gfsecurityProps);

    return sslConfigBuilder.build();
  }

  public static SSLConfig getSSLConfigForComponent(DistributionConfig distributionConfig,
      SecurableCommunicationChannel sslEnabledComponent) {
    return getInstance().createSSLConfigForComponent(distributionConfig, sslEnabledComponent);
  }

  public static SSLConfig getSSLConfigForComponent(Properties properties,
      SecurableCommunicationChannel sslEnabledComponent) {
    return getInstance().createSSLConfigForComponent(new DistributionConfigImpl(properties),
        sslEnabledComponent);
  }
}
