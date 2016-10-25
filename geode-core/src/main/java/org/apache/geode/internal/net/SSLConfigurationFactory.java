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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.ArrayUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.geode.GemFireConfigException;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.security.SecurableCommunicationChannel;

public class SSLConfigurationFactory {

  private static SSLConfigurationFactory instance = new SSLConfigurationFactory();
  private DistributionConfig distributionConfig = null;
  private Map<SecurableCommunicationChannel, SSLConfig> registeredSSLConfig = new HashMap<>();

  private SSLConfigurationFactory() {
  }

  private synchronized static SSLConfigurationFactory getInstance() {
    if (instance == null) {
      instance = new SSLConfigurationFactory();
    }
    return instance;
  }

  private DistributionConfig getDistributionConfig() {
    if(distributionConfig == null)
    {
      throw new GemFireConfigException("SSL Configuration requires a valid distribution config.");
    }
    return distributionConfig;
  }

  public static void setDistributionConfig(final DistributionConfig distributionConfig) {
    if(distributionConfig == null)
    {
      throw new GemFireConfigException("SSL Configuration requires a valid distribution config.");
    }
    getInstance().distributionConfig = distributionConfig;
  }

  public static SSLConfig getSSLConfigForComponent(SecurableCommunicationChannel sslEnabledComponent) {
    SSLConfig sslConfig = getInstance().getRegisteredSSLConfigForComponent(sslEnabledComponent);
    if (sslConfig == null) {
      sslConfig = getInstance().createSSLConfigForComponent(sslEnabledComponent);
      getInstance().registeredSSLConfigForComponent(sslEnabledComponent, sslConfig);
    }
    return sslConfig;
  }

  private synchronized void registeredSSLConfigForComponent(final SecurableCommunicationChannel sslEnabledComponent, final SSLConfig sslConfig) {
    registeredSSLConfig.put(sslEnabledComponent, sslConfig);
  }

  private SSLConfig createSSLConfigForComponent(final SecurableCommunicationChannel sslEnabledComponent) {
    SSLConfig sslConfig = createSSLConfig(sslEnabledComponent);
    SecurableCommunicationChannel[] sslEnabledComponents = getDistributionConfig().getSecurableCommunicationChannels();
    if (sslEnabledComponents.length == 0) {
      sslConfig = configureLegacyClusterSSL(sslConfig);
    }
    sslConfig.setSecurableCommunicationChannel(sslEnabledComponent);
    switch (sslEnabledComponent) {
      case ALL: {
        //Create a SSLConfig separate for HTTP Service. As the require-authentication might differ
        createSSLConfigForComponent(SecurableCommunicationChannel.WEB);
        break;
      }
      case CLUSTER: {
        if (sslEnabledComponents.length > 0) {
          sslConfig = setAliasForComponent(sslConfig, getDistributionConfig().getClusterSSLAlias());
        }else {
          sslConfig = configureLegacyClusterSSL(sslConfig);
        }
        break;
      }
      case LOCATOR: {
        if (sslEnabledComponents.length > 0) {
          sslConfig = setAliasForComponent(sslConfig, getDistributionConfig().getLocatorSSLAlias());
        }
        break;
      }
      case SERVER: {
        if (sslEnabledComponents.length > 0) {
          sslConfig = setAliasForComponent(sslConfig, getDistributionConfig().getServerSSLAlias());
        } else {
          sslConfig = configureLegacyServerSSL(sslConfig);
        }
        break;
      }
      case GATEWAY: {
        if (sslEnabledComponents.length > 0) {
          sslConfig = setAliasForComponent(sslConfig, getDistributionConfig().getGatewaySSLAlias());
        } else {
          sslConfig = configureLegacyGatewaySSL(sslConfig);
        }
        break;
      }
      case WEB: {
        if (sslEnabledComponents.length > 0) {
          sslConfig = setAliasForComponent(sslConfig, getDistributionConfig().getHTTPServiceSSLAlias());
          sslConfig.setRequireAuth(getDistributionConfig().getSSLWebRequireAuthentication());
        } else {
          sslConfig = configureLegacyHttpServiceSSL(sslConfig);
        }
        break;
      }
      case JMX: {
        if (sslEnabledComponents.length > 0) {
          sslConfig = setAliasForComponent(sslConfig, getDistributionConfig().getJMXSSLAlias());
        } else {
          sslConfig = configureLegacyJMXSSL(sslConfig);
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

  private SSLConfig createSSLConfig(final SecurableCommunicationChannel sslEnabledComponent) {
    SSLConfig sslConfig = new SSLConfig();
    sslConfig.setCiphers(getDistributionConfig().getSSLCiphers());
    sslConfig.setEnabled(determineIfSSLEnabledForSSLComponent(sslEnabledComponent));
    sslConfig.setKeystore(getDistributionConfig().getSSLKeyStore());
    sslConfig.setKeystorePassword(getDistributionConfig().getSSLKeyStorePassword());
    sslConfig.setKeystoreType(getDistributionConfig().getSSLKeyStoreType());
    sslConfig.setTruststore(getDistributionConfig().getSSLTrustStore());
    sslConfig.setTruststorePassword(getDistributionConfig().getSSLTrustStorePassword());
    sslConfig.setProtocols(getDistributionConfig().getSSLProtocols());
    sslConfig.setRequireAuth(getDistributionConfig().getSSLRequireAuthentication());
    sslConfig.setAlias(getDistributionConfig().getSSLDefaultAlias());
    return sslConfig;
  }

  private boolean determineIfSSLEnabledForSSLComponent(final SecurableCommunicationChannel sslEnabledComponent) {
    if (ArrayUtils.contains(getDistributionConfig().getSecurableCommunicationChannels(), SecurableCommunicationChannel.NONE)) {
      return false;
    }
    if (ArrayUtils.contains(getDistributionConfig().getSecurableCommunicationChannels(), SecurableCommunicationChannel.ALL)) {
      return true;
    }
    return ArrayUtils.contains(getDistributionConfig().getSecurableCommunicationChannels(), sslEnabledComponent) ? true : false;
  }

  /**
   * Configure a sslConfig for the cluster using the legacy configuration
   * @return A sslConfig object describing the ssl config for the server component
   *
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyClusterSSL(SSLConfig sslConfig) {
    sslConfig.setCiphers(getDistributionConfig().getClusterSSLCiphers());
    sslConfig.setEnabled(getDistributionConfig().getClusterSSLEnabled());
    sslConfig.setKeystore(getDistributionConfig().getClusterSSLKeyStore());
    sslConfig.setKeystorePassword(getDistributionConfig().getClusterSSLKeyStorePassword());
    sslConfig.setKeystoreType(getDistributionConfig().getClusterSSLKeyStoreType());
    sslConfig.setTruststore(getDistributionConfig().getClusterSSLTrustStore());
    sslConfig.setTruststorePassword(getDistributionConfig().getClusterSSLTrustStorePassword());
    sslConfig.setProtocols(getDistributionConfig().getClusterSSLProtocols());
    sslConfig.setRequireAuth(getDistributionConfig().getClusterSSLRequireAuthentication());
    return sslConfig;
  }

  /**
   * Configure a sslConfig for the server using the legacy configuration
   * @return A sslConfig object describing the ssl config for the server component
   *
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyServerSSL(SSLConfig sslConfig) {
    sslConfig.setCiphers(getDistributionConfig().getServerSSLCiphers());
    sslConfig.setEnabled(getDistributionConfig().getServerSSLEnabled());
    sslConfig.setKeystore(getDistributionConfig().getServerSSLKeyStore());
    sslConfig.setKeystorePassword(getDistributionConfig().getServerSSLKeyStorePassword());
    sslConfig.setKeystoreType(getDistributionConfig().getServerSSLKeyStoreType());
    sslConfig.setTruststore(getDistributionConfig().getServerSSLTrustStore());
    sslConfig.setTruststorePassword(getDistributionConfig().getServerSSLTrustStorePassword());
    sslConfig.setProtocols(getDistributionConfig().getServerSSLProtocols());
    sslConfig.setRequireAuth(getDistributionConfig().getServerSSLRequireAuthentication());
    return sslConfig;
  }

  /**
   * Configure a sslConfig for the jmx using the legacy configuration
   * @return A sslConfig object describing the ssl config for the jmx component
   *
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyJMXSSL(SSLConfig sslConfig) {
    sslConfig.setCiphers(getDistributionConfig().getJmxManagerSSLCiphers());
    sslConfig.setEnabled(getDistributionConfig().getJmxManagerSSLEnabled());
    sslConfig.setKeystore(getDistributionConfig().getJmxManagerSSLKeyStore());
    sslConfig.setKeystorePassword(getDistributionConfig().getJmxManagerSSLKeyStorePassword());
    sslConfig.setKeystoreType(getDistributionConfig().getJmxManagerSSLKeyStoreType());
    sslConfig.setTruststore(getDistributionConfig().getJmxManagerSSLTrustStore());
    sslConfig.setTruststorePassword(getDistributionConfig().getJmxManagerSSLTrustStorePassword());
    sslConfig.setProtocols(getDistributionConfig().getJmxManagerSSLProtocols());
    sslConfig.setRequireAuth(getDistributionConfig().getJmxManagerSSLRequireAuthentication());
    return sslConfig;
  }

  /**
   * Configure a sslConfig for the gateway using the legacy configuration
   * @return A sslConfig object describing the ssl config for the gateway component
   *
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyGatewaySSL(SSLConfig sslConfig) {
    sslConfig.setCiphers(getDistributionConfig().getGatewaySSLCiphers());
    sslConfig.setEnabled(getDistributionConfig().getGatewaySSLEnabled());
    sslConfig.setKeystore(getDistributionConfig().getGatewaySSLKeyStore());
    sslConfig.setKeystorePassword(getDistributionConfig().getGatewaySSLKeyStorePassword());
    sslConfig.setKeystoreType(getDistributionConfig().getGatewaySSLKeyStoreType());
    sslConfig.setTruststore(getDistributionConfig().getGatewaySSLTrustStore());
    sslConfig.setTruststorePassword(getDistributionConfig().getGatewaySSLTrustStorePassword());
    sslConfig.setProtocols(getDistributionConfig().getGatewaySSLProtocols());
    sslConfig.setRequireAuth(getDistributionConfig().getGatewaySSLRequireAuthentication());
    return sslConfig;
  }

  /**
   * Configure a sslConfig for the http service using the legacy configuration
   * @return A sslConfig object describing the ssl config for the http service component
   *
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyHttpServiceSSL(SSLConfig sslConfig) {
    sslConfig.setCiphers(getDistributionConfig().getHttpServiceSSLCiphers());
    sslConfig.setEnabled(getDistributionConfig().getHttpServiceSSLEnabled());
    sslConfig.setKeystore(getDistributionConfig().getHttpServiceSSLKeyStore());
    sslConfig.setKeystorePassword(getDistributionConfig().getHttpServiceSSLKeyStorePassword());
    sslConfig.setKeystoreType(getDistributionConfig().getHttpServiceSSLKeyStoreType());
    sslConfig.setTruststore(getDistributionConfig().getHttpServiceSSLTrustStore());
    sslConfig.setTruststorePassword(getDistributionConfig().getHttpServiceSSLTrustStorePassword());
    sslConfig.setProtocols(getDistributionConfig().getHttpServiceSSLProtocols());
    sslConfig.setRequireAuth(getDistributionConfig().getHttpServiceSSLRequireAuthentication());
    return sslConfig;
  }

  private SSLConfig configureSSLPropertiesFromSystemProperties(SSLConfig sslConfig) {
    return configureSSLPropertiesFromSystemProperties(sslConfig, null);
  }

  private SSLConfig configureSSLPropertiesFromSystemProperties(SSLConfig sslConfig, Properties properties) {
    if (StringUtils.isEmpty(sslConfig.getKeystore())) {
      sslConfig.setKeystore(getValueFromSystemProperties(properties, "javax.net.ssl.keyStore"));
    }
    if (StringUtils.isEmpty(sslConfig.getKeystoreType())) {
      sslConfig.setKeystoreType(getValueFromSystemProperties(properties, "javax.net.ssl.keyStoreType"));
    }
    if (StringUtils.isEmpty(sslConfig.getKeystorePassword())) {
      sslConfig.setKeystorePassword(getValueFromSystemProperties(properties, "javax.net.ssl.keyStorePassword"));
    }
    if (StringUtils.isEmpty(sslConfig.getTruststore())) {
      sslConfig.setTruststore(getValueFromSystemProperties(properties, "javax.net.ssl.trustStore"));
    }
    if (StringUtils.isEmpty(sslConfig.getTruststorePassword())) {
      sslConfig.setTruststorePassword(getValueFromSystemProperties(properties, "javax.net.ssl.trustStorePassword"));
    }
    if (StringUtils.isEmpty(sslConfig.getTruststoreType())) {
      sslConfig.setTruststoreType(getValueFromSystemProperties(properties, "javax.net.ssl.trustStoreType"));
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

  private SSLConfig getRegisteredSSLConfigForComponent(final SecurableCommunicationChannel sslEnabledComponent) {
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
                                                   final boolean needClientAuth,
                                                   final String protocols,
                                                   final String ciphers,
                                                   final Properties gfsecurityProps,
                                                   final String alias) {
    SSLConfig sslConfig = new SSLConfig();
    sslConfig.setAlias(alias);
    sslConfig.setCiphers(ciphers);
    sslConfig.setProtocols(protocols);
    sslConfig.setRequireAuth(needClientAuth);
    sslConfig.setEnabled(useSSL);

    sslConfig = getInstance().configureSSLPropertiesFromSystemProperties(sslConfig, gfsecurityProps);

    return sslConfig;
  }
}
