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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

public class SSLConfigurationFactory {

  public static final String JAVAX_KEYSTORE = "javax.net.ssl.keyStore";
  public static final String JAVAX_KEYSTORE_TYPE = "javax.net.ssl.keyStoreType";
  public static final String JAVAX_KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword";
  public static final String JAVAX_TRUSTSTORE = "javax.net.ssl.trustStore";
  public static final String JAVAX_TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";
  public static final String JAVAX_TRUSTSTORE_TYPE = "javax.net.ssl.trustStoreType";

  private static SSLConfigurationFactory instance = new SSLConfigurationFactory();
  private DistributionConfig distributionConfig = null;
  private Map<SecurableCommunicationChannel, SSLConfig> registeredSSLConfig = new HashMap<>();

  private SSLConfigurationFactory() {}

  private static synchronized SSLConfigurationFactory getInstance() {
    if (instance == null) {
      instance = new SSLConfigurationFactory();
    }
    return instance;
  }

  private DistributionConfig getDistributionConfig() {
    if (distributionConfig == null) {
      throw new GemFireConfigException("SSL Configuration requires a valid distribution config.");
    }
    return distributionConfig;
  }

  public static void setDistributionConfig(final DistributionConfig distributionConfig) {
    if (distributionConfig == null) {
      throw new GemFireConfigException("SSL Configuration requires a valid distribution config.");
    }
    getInstance().distributionConfig = distributionConfig;
  }

  /**
   * @deprecated since GEODE 1.3, use #{getSSLConfigForComponent({@link DistributionConfig} ,
   *             {@link SecurableCommunicationChannel})} instead
   */
  @Deprecated
  public static SSLConfig getSSLConfigForComponent(
      SecurableCommunicationChannel sslEnabledComponent) {
    SSLConfig sslConfig = getInstance().getRegisteredSSLConfigForComponent(sslEnabledComponent);
    if (sslConfig == null) {
      sslConfig = getInstance().createSSLConfigForComponent(sslEnabledComponent);
      getInstance().registeredSSLConfigForComponent(sslEnabledComponent, sslConfig);
    }
    return sslConfig;
  }

  private synchronized void registeredSSLConfigForComponent(
      final SecurableCommunicationChannel sslEnabledComponent, final SSLConfig sslConfig) {
    registeredSSLConfig.put(sslEnabledComponent, sslConfig);
  }

  private SSLConfig createSSLConfigForComponent(
      final SecurableCommunicationChannel sslEnabledComponent) {
    return createSSLConfigForComponent(getDistributionConfig(), sslEnabledComponent);
  }

  private SSLConfig createSSLConfigForComponent(final DistributionConfig distributionConfig,
      final SecurableCommunicationChannel sslEnabledComponent) {
    SSLConfig sslConfig = createSSLConfig(distributionConfig, sslEnabledComponent);
    SecurableCommunicationChannel[] sslEnabledComponents =
        distributionConfig.getSecurableCommunicationChannels();
    if (sslEnabledComponents.length == 0) {
      sslConfig = configureLegacyClusterSSL(distributionConfig, sslConfig);
    }
    sslConfig.setSecurableCommunicationChannel(sslEnabledComponent);
    switch (sslEnabledComponent) {
      case ALL: {
        // Create a SSLConfig separate for HTTP Service. As the require-authentication might differ
        createSSLConfigForComponent(distributionConfig, SecurableCommunicationChannel.WEB);
        break;
      }
      case CLUSTER: {
        if (sslEnabledComponents.length > 0) {
          sslConfig = setAliasForComponent(sslConfig, distributionConfig.getClusterSSLAlias());
        } else {
          sslConfig = configureLegacyClusterSSL(distributionConfig, sslConfig);
        }
        break;
      }
      case LOCATOR: {
        if (sslEnabledComponents.length > 0) {
          sslConfig = setAliasForComponent(sslConfig, distributionConfig.getLocatorSSLAlias());
        }
        break;
      }
      case SERVER: {
        if (sslEnabledComponents.length > 0) {
          sslConfig = setAliasForComponent(sslConfig, distributionConfig.getServerSSLAlias());
        } else {
          sslConfig = configureLegacyServerSSL(distributionConfig, sslConfig);
        }
        break;
      }
      case GATEWAY: {
        if (sslEnabledComponents.length > 0) {
          sslConfig = setAliasForComponent(sslConfig, distributionConfig.getGatewaySSLAlias());
        } else {
          sslConfig = configureLegacyGatewaySSL(distributionConfig, sslConfig);
        }
        break;
      }
      case WEB: {
        if (sslEnabledComponents.length > 0) {
          sslConfig = setAliasForComponent(sslConfig, distributionConfig.getHTTPServiceSSLAlias());
          sslConfig.setRequireAuth(distributionConfig.getSSLWebRequireAuthentication());
        } else {
          sslConfig = configureLegacyHttpServiceSSL(distributionConfig, sslConfig);
        }
        break;
      }
      case JMX: {
        if (sslEnabledComponents.length > 0) {
          sslConfig = setAliasForComponent(sslConfig, distributionConfig.getJMXSSLAlias());
        } else {
          sslConfig = configureLegacyJMXSSL(distributionConfig, sslConfig);
        }
        break;
      }
    }
    configureSSLPropertiesFromSystemProperties(sslConfig);
    return sslConfig;
  }

  private SSLConfig setAliasForComponent(final SSLConfig sslConfig, final String clusterSSLAlias) {
    if (!StringUtils.isEmpty(clusterSSLAlias)) {
      sslConfig.setAlias(clusterSSLAlias);
    }
    return sslConfig;
  }

  private SSLConfig createSSLConfig(final DistributionConfig distributionConfig,
      final SecurableCommunicationChannel sslEnabledComponent) {
    SSLConfig sslConfig = new SSLConfig();
    sslConfig.setCiphers(distributionConfig.getSSLCiphers());
    sslConfig
        .setEndpointIdentificationEnabled(distributionConfig.getSSLEndPointIdentificationEnabled());
    sslConfig
        .setEnabled(determineIfSSLEnabledForSSLComponent(distributionConfig, sslEnabledComponent));
    sslConfig.setKeystore(distributionConfig.getSSLKeyStore());
    sslConfig.setKeystorePassword(distributionConfig.getSSLKeyStorePassword());
    sslConfig.setKeystoreType(distributionConfig.getSSLKeyStoreType());
    sslConfig.setTruststore(distributionConfig.getSSLTrustStore());
    sslConfig.setTruststorePassword(distributionConfig.getSSLTrustStorePassword());
    sslConfig.setTruststoreType(distributionConfig.getSSLTrustStoreType());
    sslConfig.setProtocols(distributionConfig.getSSLProtocols());
    sslConfig.setRequireAuth(distributionConfig.getSSLRequireAuthentication());
    sslConfig.setAlias(distributionConfig.getSSLDefaultAlias());
    sslConfig.setUseDefaultSSLContext(distributionConfig.getSSLUseDefaultContext());

    return sslConfig;
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
   * Configure a sslConfig for the cluster using the legacy configuration
   *
   * @return A sslConfig object describing the ssl config for the server component
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyClusterSSL(final DistributionConfig distributionConfig,
      final SSLConfig sslConfig) {
    sslConfig.setCiphers(distributionConfig.getClusterSSLCiphers());
    sslConfig.setEnabled(distributionConfig.getClusterSSLEnabled());
    sslConfig.setKeystore(distributionConfig.getClusterSSLKeyStore());
    sslConfig.setKeystorePassword(distributionConfig.getClusterSSLKeyStorePassword());
    sslConfig.setKeystoreType(distributionConfig.getClusterSSLKeyStoreType());
    sslConfig.setTruststore(distributionConfig.getClusterSSLTrustStore());
    sslConfig.setTruststorePassword(distributionConfig.getClusterSSLTrustStorePassword());
    sslConfig.setTruststoreType(distributionConfig.getClusterSSLKeyStoreType());
    sslConfig.setProtocols(distributionConfig.getClusterSSLProtocols());
    sslConfig.setRequireAuth(distributionConfig.getClusterSSLRequireAuthentication());
    return sslConfig;
  }

  /**
   * Configure a sslConfig for the server using the legacy configuration
   *
   * @return A sslConfig object describing the ssl config for the server component
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyServerSSL(final DistributionConfig distributionConfig,
      final SSLConfig sslConfig) {
    sslConfig.setCiphers(distributionConfig.getServerSSLCiphers());
    sslConfig.setEnabled(distributionConfig.getServerSSLEnabled());
    sslConfig.setKeystore(distributionConfig.getServerSSLKeyStore());
    sslConfig.setKeystorePassword(distributionConfig.getServerSSLKeyStorePassword());
    sslConfig.setKeystoreType(distributionConfig.getServerSSLKeyStoreType());
    sslConfig.setTruststore(distributionConfig.getServerSSLTrustStore());
    sslConfig.setTruststorePassword(distributionConfig.getServerSSLTrustStorePassword());
    sslConfig.setTruststoreType(distributionConfig.getServerSSLKeyStoreType());
    sslConfig.setProtocols(distributionConfig.getServerSSLProtocols());
    sslConfig.setRequireAuth(distributionConfig.getServerSSLRequireAuthentication());
    return sslConfig;
  }

  /**
   * Configure a sslConfig for the jmx using the legacy configuration
   *
   * @return A sslConfig object describing the ssl config for the jmx component
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyJMXSSL(final DistributionConfig distributionConfig,
      final SSLConfig sslConfig) {
    sslConfig.setCiphers(distributionConfig.getJmxManagerSSLCiphers());
    sslConfig.setEnabled(distributionConfig.getJmxManagerSSLEnabled());
    sslConfig.setKeystore(distributionConfig.getJmxManagerSSLKeyStore());
    sslConfig.setKeystorePassword(distributionConfig.getJmxManagerSSLKeyStorePassword());
    sslConfig.setKeystoreType(distributionConfig.getJmxManagerSSLKeyStoreType());
    sslConfig.setTruststore(distributionConfig.getJmxManagerSSLTrustStore());
    sslConfig.setTruststorePassword(distributionConfig.getJmxManagerSSLTrustStorePassword());
    sslConfig.setTruststoreType(distributionConfig.getJmxManagerSSLKeyStoreType());
    sslConfig.setProtocols(distributionConfig.getJmxManagerSSLProtocols());
    sslConfig.setRequireAuth(distributionConfig.getJmxManagerSSLRequireAuthentication());
    return sslConfig;
  }

  /**
   * Configure a sslConfig for the gateway using the legacy configuration
   *
   * @return A sslConfig object describing the ssl config for the gateway component
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyGatewaySSL(final DistributionConfig distributionConfig,
      final SSLConfig sslConfig) {
    sslConfig.setCiphers(distributionConfig.getGatewaySSLCiphers());
    sslConfig.setEnabled(distributionConfig.getGatewaySSLEnabled());
    sslConfig.setKeystore(distributionConfig.getGatewaySSLKeyStore());
    sslConfig.setKeystorePassword(distributionConfig.getGatewaySSLKeyStorePassword());
    sslConfig.setKeystoreType(distributionConfig.getGatewaySSLKeyStoreType());
    sslConfig.setTruststore(distributionConfig.getGatewaySSLTrustStore());
    sslConfig.setTruststorePassword(distributionConfig.getGatewaySSLTrustStorePassword());
    sslConfig.setProtocols(distributionConfig.getGatewaySSLProtocols());
    sslConfig.setRequireAuth(distributionConfig.getGatewaySSLRequireAuthentication());
    return sslConfig;
  }

  /**
   * Configure a sslConfig for the http service using the legacy configuration
   *
   * @return A sslConfig object describing the ssl config for the http service component
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyHttpServiceSSL(final DistributionConfig distributionConfig,
      final SSLConfig sslConfig) {
    sslConfig.setCiphers(distributionConfig.getHttpServiceSSLCiphers());
    sslConfig.setEnabled(distributionConfig.getHttpServiceSSLEnabled());
    sslConfig.setKeystore(distributionConfig.getHttpServiceSSLKeyStore());
    sslConfig.setKeystorePassword(distributionConfig.getHttpServiceSSLKeyStorePassword());
    sslConfig.setKeystoreType(distributionConfig.getHttpServiceSSLKeyStoreType());
    sslConfig.setTruststore(distributionConfig.getHttpServiceSSLTrustStore());
    sslConfig.setTruststorePassword(distributionConfig.getHttpServiceSSLTrustStorePassword());
    sslConfig.setTruststoreType(distributionConfig.getHttpServiceSSLKeyStoreType());
    sslConfig.setProtocols(distributionConfig.getHttpServiceSSLProtocols());
    sslConfig.setRequireAuth(distributionConfig.getHttpServiceSSLRequireAuthentication());
    return sslConfig;
  }

  private SSLConfig configureSSLPropertiesFromSystemProperties(SSLConfig sslConfig) {
    return configureSSLPropertiesFromSystemProperties(sslConfig, null);
  }

  private SSLConfig configureSSLPropertiesFromSystemProperties(SSLConfig sslConfig,
      Properties properties) {
    if (StringUtils.isEmpty(sslConfig.getKeystore())) {
      sslConfig.setKeystore(getValueFromSystemProperties(properties, JAVAX_KEYSTORE));
    }
    if (StringUtils.isEmpty(sslConfig.getKeystoreType())) {
      sslConfig.setKeystoreType(getValueFromSystemProperties(properties, JAVAX_KEYSTORE_TYPE));
    }
    if (StringUtils.isEmpty(sslConfig.getKeystorePassword())) {
      sslConfig
          .setKeystorePassword(getValueFromSystemProperties(properties, JAVAX_KEYSTORE_PASSWORD));
    }
    if (StringUtils.isEmpty(sslConfig.getTruststore())) {
      sslConfig.setTruststore(getValueFromSystemProperties(properties, JAVAX_TRUSTSTORE));
    }
    if (StringUtils.isEmpty(sslConfig.getTruststorePassword())) {
      sslConfig.setTruststorePassword(
          getValueFromSystemProperties(properties, JAVAX_TRUSTSTORE_PASSWORD));
    }
    if (StringUtils.isEmpty(sslConfig.getTruststoreType())) {
      sslConfig.setTruststoreType(getValueFromSystemProperties(properties, JAVAX_TRUSTSTORE_TYPE));
    }
    return sslConfig;
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

  private SSLConfig getRegisteredSSLConfigForComponent(
      final SecurableCommunicationChannel sslEnabledComponent) {
    return registeredSSLConfig.get(sslEnabledComponent);
  }

  public static void close() {
    getInstance().clearSSLConfigForAllComponents();
    getInstance().distributionConfig = null;
  }

  private void clearSSLConfigForAllComponents() {
    registeredSSLConfig.clear();
  }

  @Deprecated
  public static SSLConfig getSSLConfigForComponent(final boolean useSSL,
      final boolean needClientAuth, final String protocols, final String ciphers,
      final Properties gfsecurityProps, final String alias) {
    SSLConfig sslConfig = new SSLConfig();
    sslConfig.setAlias(alias);
    sslConfig.setCiphers(ciphers);
    sslConfig.setProtocols(protocols);
    sslConfig.setRequireAuth(needClientAuth);
    sslConfig.setEnabled(useSSL);

    sslConfig =
        getInstance().configureSSLPropertiesFromSystemProperties(sslConfig, gfsecurityProps);

    return sslConfig;
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
