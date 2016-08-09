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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.ArrayUtils;
import org.springframework.util.StringUtils;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.internal.admin.SSLConfig;

public class SSLConfigurationFactory {

  private static SSLConfigurationFactory instance = new SSLConfigurationFactory();
  private DistributionConfig distributionConfig = new DistributionConfigImpl(new Properties());
  private Map<SSLEnabledComponent, SSLConfig> registeredSSLConfig = new HashMap<>();

  private SSLConfigurationFactory() {
  }

  private synchronized static SSLConfigurationFactory getInstance() {
    if (instance == null) {
      instance = new SSLConfigurationFactory();
    }
    return instance;
  }

  public static void setDistributionConfig(final DistributionConfig distributionConfig) {
    getInstance().distributionConfig = distributionConfig;
  }

  public static SSLConfig getSSLConfigForComponent(SSLEnabledComponent sslEnabledComponent) {
    SSLConfig sslConfig = getInstance().getRegisteredSSLConfigForComponent(sslEnabledComponent);
    if (sslConfig == null) {
      sslConfig = getInstance().createSSLConfigForComponent(sslEnabledComponent);
      getInstance().registeredSSLConfigForComponent(sslEnabledComponent, sslConfig);
    }
    return sslConfig;
  }

  private synchronized void registeredSSLConfigForComponent(final SSLEnabledComponent sslEnabledComponent, final SSLConfig sslConfig) {
    registeredSSLConfig.put(sslEnabledComponent, sslConfig);
  }

  private SSLConfig createSSLConfigForComponent(final SSLEnabledComponent sslEnabledComponent) {
    SSLConfig sslConfig = new SSLConfig();
    configureClusterSSL(sslConfig, sslEnabledComponent);
    SSLEnabledComponent[] sslEnabledComponents = distributionConfig.getSSLEnabledComponents();
    if(sslEnabledComponents.length == 0)
    {
      sslConfig = configureLegacyClusterSSL(sslConfig);
    }
    sslConfig.setSslEnabledComponent(sslEnabledComponent);
    switch (sslEnabledComponent) {
      case ALL: {

      }
      case CLUSTER: {
        if (sslEnabledComponents.length > 0) {
          sslConfig.setAlias(distributionConfig.getClusterSSLAlias());
        }
        break;
      }
      case LOCATOR: {
        if (sslEnabledComponents.length > 0) {
          sslConfig.setAlias(distributionConfig.getLocatorSSLAlias());
        }
        break;
      }
      case SERVER: {
        if (sslEnabledComponents.length > 0) {
          sslConfig.setAlias(distributionConfig.getServerSSLAlias());
        } else {
          sslConfig = configureLegacyServerSSL(sslConfig);
        }
        break;
      }
      case GATEWAY: {
        if (sslEnabledComponents.length > 0) {
          sslConfig.setAlias(distributionConfig.getGatewaySSLAlias());
        } else {
          sslConfig = configureLegacyGatewaySSL(sslConfig);
        }
        break;
      }
      case HTTP_SERVICE: {
        if (sslEnabledComponents.length > 0) {
          sslConfig.setAlias(distributionConfig.getHTTPServiceSSLAlias());
        } else {
          sslConfig = configureLegacyHttpServiceSSL(sslConfig);
        }
        break;
      }
      case JMX: {
        if (sslEnabledComponents.length > 0) {
          sslConfig.setAlias(distributionConfig.getJMXManagerSSLAlias());
        } else {
          sslConfig = configureLegacyJMXSSL(sslConfig);
        }
        break;
      }
    }
    configureSSLPropertiesFromSystemProperties(sslConfig);
    return sslConfig;
  }

  private void configureClusterSSL(final SSLConfig sslConfig, final SSLEnabledComponent sslEnabledComponent) {
    sslConfig.setCiphers(distributionConfig.getSSLCiphers());
    sslConfig.setEnabled(determineIfSSLEnabledForSSLComponent(sslEnabledComponent));
    sslConfig.setKeystore(distributionConfig.getSSLKeyStore());
    sslConfig.setKeystorePassword(distributionConfig.getSSLKeyStorePassword());
    sslConfig.setKeystoreType(distributionConfig.getSSLKeyStoreType());
    sslConfig.setTruststore(distributionConfig.getSSLTrustStore());
    sslConfig.setTruststorePassword(distributionConfig.getSSLTrustStorePassword());
    sslConfig.setProtocols(distributionConfig.getSSLProtocols());
    sslConfig.setRequireAuth(distributionConfig.getSSLRequireAuthentication());
  }

  private boolean determineIfSSLEnabledForSSLComponent(final SSLEnabledComponent sslEnabledComponent) {
    if (ArrayUtils.contains(distributionConfig.getSSLEnabledComponents(), SSLEnabledComponent.NONE)) {
      return false;
    }
    if (ArrayUtils.contains(distributionConfig.getSSLEnabledComponents(), SSLEnabledComponent.ALL)) {
      return true;
    }
    return ArrayUtils.contains(distributionConfig.getSSLEnabledComponents(), sslEnabledComponent) ? true : false;
  }

  /**
   * Configure a sslConfig for the cluster using the legacy configuration
   * @return A sslConfig object describing the ssl config for the server component
   *
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyClusterSSL(SSLConfig sslConfig) {
    sslConfig.setCiphers(distributionConfig.getClusterSSLCiphers());
    sslConfig.setEnabled(distributionConfig.getClusterSSLEnabled());
    sslConfig.setKeystore(distributionConfig.getClusterSSLKeyStore());
    sslConfig.setKeystorePassword(distributionConfig.getClusterSSLKeyStorePassword());
    sslConfig.setKeystoreType(distributionConfig.getClusterSSLKeyStoreType());
    sslConfig.setTruststore(distributionConfig.getClusterSSLTrustStore());
    sslConfig.setTruststorePassword(distributionConfig.getClusterSSLTrustStorePassword());
    sslConfig.setProtocols(distributionConfig.getClusterSSLProtocols());
    sslConfig.setRequireAuth(distributionConfig.getClusterSSLRequireAuthentication());
    return sslConfig;
  }

  /**
   * Configure a sslConfig for the server using the legacy configuration
   * @return A sslConfig object describing the ssl config for the server component
   *
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyServerSSL(SSLConfig sslConfig) {
    sslConfig.setCiphers(distributionConfig.getServerSSLCiphers());
    sslConfig.setEnabled(distributionConfig.getServerSSLEnabled());
    sslConfig.setKeystore(distributionConfig.getServerSSLKeyStore());
    sslConfig.setKeystorePassword(distributionConfig.getServerSSLKeyStorePassword());
    sslConfig.setKeystoreType(distributionConfig.getServerSSLKeyStoreType());
    sslConfig.setTruststore(distributionConfig.getServerSSLTrustStore());
    sslConfig.setTruststorePassword(distributionConfig.getServerSSLTrustStorePassword());
    sslConfig.setProtocols(distributionConfig.getServerSSLProtocols());
    sslConfig.setRequireAuth(distributionConfig.getServerSSLRequireAuthentication());
    return sslConfig;
  }

  /**
   * Configure a sslConfig for the jmx using the legacy configuration
   * @return A sslConfig object describing the ssl config for the jmx component
   *
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyJMXSSL(SSLConfig sslConfig) {
    sslConfig.setCiphers(distributionConfig.getJmxManagerSSLCiphers());
    sslConfig.setEnabled(distributionConfig.getJmxManagerSSLEnabled());
    sslConfig.setKeystore(distributionConfig.getJmxManagerSSLKeyStore());
    sslConfig.setKeystorePassword(distributionConfig.getJmxManagerSSLKeyStorePassword());
    sslConfig.setKeystoreType(distributionConfig.getJmxManagerSSLKeyStoreType());
    sslConfig.setTruststore(distributionConfig.getJmxManagerSSLTrustStore());
    sslConfig.setTruststorePassword(distributionConfig.getJmxManagerSSLTrustStorePassword());
    sslConfig.setProtocols(distributionConfig.getJmxManagerSSLProtocols());
    sslConfig.setRequireAuth(distributionConfig.getJmxManagerSSLRequireAuthentication());
    return sslConfig;
  }

  /**
   * Configure a sslConfig for the gateway using the legacy configuration
   * @return A sslConfig object describing the ssl config for the gateway component
   *
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyGatewaySSL(SSLConfig sslConfig) {
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
   * @return A sslConfig object describing the ssl config for the http service component
   *
   * @deprecated as of Geode 1.0
   */
  private SSLConfig configureLegacyHttpServiceSSL(SSLConfig sslConfig) {
    sslConfig.setCiphers(distributionConfig.getHttpServiceSSLCiphers());
    sslConfig.setEnabled(distributionConfig.getHttpServiceSSLEnabled());
    sslConfig.setKeystore(distributionConfig.getHttpServiceSSLKeyStore());
    sslConfig.setKeystorePassword(distributionConfig.getHttpServiceSSLKeyStorePassword());
    sslConfig.setKeystoreType(distributionConfig.getHttpServiceSSLKeyStoreType());
    sslConfig.setTruststore(distributionConfig.getHttpServiceSSLTrustStore());
    sslConfig.setTruststorePassword(distributionConfig.getHttpServiceSSLTrustStorePassword());
    sslConfig.setProtocols(distributionConfig.getHttpServiceSSLProtocols());
    sslConfig.setRequireAuth(distributionConfig.getHttpServiceSSLRequireAuthentication());
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

  private SSLConfig getRegisteredSSLConfigForComponent(final SSLEnabledComponent sslEnabledComponent) {
    return registeredSSLConfig.get(sslEnabledComponent);
  }

  public static void close() {
    getInstance().clearSSLConfigForAllComponents();
    getInstance().distributionConfig = null;
    instance = null;
  }

  private void clearSSLConfigForAllComponents() {
    registeredSSLConfig.clear();
  }

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
