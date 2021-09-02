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

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;

import java.security.KeyStore;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.security.CallbackInstantiator;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.net.SSLParameterExtension;
import org.apache.geode.net.internal.SSLParameterExtensionContextImpl;

/**
 * The SSL configuration settings for a GemFire distributed system.
 *
 * This class is immutable and the way to construct it is by means of the
 * {@link SSLConfig.Builder} class.
 */
@Immutable
public class SSLConfig {

  private final boolean endpointIdentification;
  private final boolean useDefaultSSLContext;
  private final boolean enabled;
  private final String protocols;
  private final String ciphers;
  private final boolean requireAuth;
  private final String keystore;
  private final String keystoreType;
  private final String keystorePassword;
  private final String truststore;
  private final String truststorePassword;
  private final String truststoreType;
  private final String alias;
  @Immutable
  private final SecurableCommunicationChannel securableCommunicationChannel;

  /**
   * SSL implementation-specific key-value pairs. Each key should be prefixed with
   * <code>javax.net.ssl.</code>
   */
  @Immutable
  private final Properties properties;

  @Immutable
  private final SSLParameterExtension sslParameterExtension;

  private SSLConfig(boolean endpointIdentification,
      boolean useDefaultSSLContext,
      boolean enabled,
      String protocols,
      String ciphers,
      boolean requireAuth,
      String keystore,
      String keystoreType,
      String keystorePassword,
      String truststore,
      String truststorePassword,
      String truststoreType,
      String alias,
      SecurableCommunicationChannel securableCommunicationChannel,
      Properties properties,
      SSLParameterExtension sslParameterExtension) {
    this.endpointIdentification = endpointIdentification;
    this.useDefaultSSLContext = useDefaultSSLContext;
    this.enabled = enabled;
    this.protocols = protocols;
    this.ciphers = ciphers;
    this.requireAuth = requireAuth;
    this.keystore = keystore;
    this.keystoreType = keystoreType;
    this.keystorePassword = keystorePassword;
    this.truststore = truststore;
    this.truststorePassword = truststorePassword;
    this.truststoreType = truststoreType;
    this.alias = alias;
    this.securableCommunicationChannel = securableCommunicationChannel;
    this.properties = properties;
    this.sslParameterExtension = sslParameterExtension;
  }

  public String getAlias() {
    return alias;
  }

  public boolean doEndpointIdentification() {
    return this.endpointIdentification;
  }

  public String getKeystore() {
    return keystore;
  }

  public String getKeystorePassword() {
    return keystorePassword;
  }

  public String getKeystoreType() {
    return keystoreType;
  }

  public String getTruststore() {
    return truststore;
  }

  public String getTruststorePassword() {
    return truststorePassword;
  }

  public boolean isEnabled() {
    return this.enabled;
  }

  public boolean useDefaultSSLContext() {
    return this.useDefaultSSLContext;
  }

  public String getProtocols() {
    return this.protocols;
  }

  public String[] getProtocolsAsStringArray() {
    return SSLUtil.readArray(this.protocols);
  }

  public String getCiphers() {
    return this.ciphers;
  }

  public String[] getCiphersAsStringArray() {
    return SSLUtil.readArray(this.ciphers);
  }

  public boolean isRequireAuth() {
    return this.requireAuth;
  }

  public String getTruststoreType() {
    return truststoreType;
  }

  public Properties getProperties() {
    return this.properties;
  }

  public SecurableCommunicationChannel getSecuredCommunicationChannel() {
    return securableCommunicationChannel;
  }

  public SSLParameterExtension getSSLParameterExtension() {
    return sslParameterExtension;
  }

  /**
   * Returns true if ciphers is either null, empty or is set to "any" (ignoring case)
   */
  public boolean isAnyCiphers() {
    return StringUtils.isBlank(ciphers) || "any".equalsIgnoreCase(ciphers);
  }

  /**
   * Returns true if protocols is either null, empty or is set to "any" (ignoring case)
   */
  public boolean isAnyProtocols() {
    return StringUtils.isBlank(protocols) || "any".equalsIgnoreCase(protocols);
  }

  @Override
  public String toString() {
    return "SSLConfig{" + "enabled=" + enabled + ", protocols='" + protocols + '\'' + ", ciphers='"
        + ciphers + '\'' + ", requireAuth=" + requireAuth + ", keystore='" + keystore + '\''
        + ", keystoreType='" + keystoreType + '\'' + ", keystorePassword='" + keystorePassword
        + '\'' + ", truststore='" + truststore + '\'' + ", truststorePassword='"
        + truststorePassword + '\'' + ", truststoreType='" + truststoreType + '\'' + ", alias='"
        + alias + '\'' + ", securableCommunicationChannel=" + securableCommunicationChannel
        + ", properties=" + properties + '\'' + ", sslParameterExtension=" + sslParameterExtension
        + '}';
  }

  /**
   * Populates a <code>Properties</code> object with the SSL-related configuration information used
   * by {@link org.apache.geode.distributed.DistributedSystem#connect}.
   *
   * @since GemFire 4.0
   */
  public void toDSProperties(Properties props) {
    props.setProperty(CLUSTER_SSL_ENABLED, String.valueOf(this.enabled));

    if (this.enabled) {
      props.setProperty(CLUSTER_SSL_PROTOCOLS, this.protocols);
      props.setProperty(CLUSTER_SSL_CIPHERS, this.ciphers);
      props.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION, String.valueOf(this.requireAuth));
    }
  }

  /**
   * Builder class to be used to construct SSLConfig instances.
   * In order to build an {@link SSLConfig} instance an instance of this
   * class must be created, then the corresponding setter methods invoked
   * and finally the build() method that returns the {@link SSLConfig} instance
   *
   * Example:
   * SSLConfig sslConfInstance = new SSLConfig.Builder()
   * .setAlias(alias)
   * .setKeystore(keystore)
   * ...
   * .build();
   *
   */
  public static class Builder {
    private boolean endpointIdentification;
    private boolean useDefaultSSLContext = DistributionConfig.DEFAULT_SSL_USE_DEFAULT_CONTEXT;
    private boolean enabled = DistributionConfig.DEFAULT_SSL_ENABLED;
    private String protocols = DistributionConfig.DEFAULT_SSL_PROTOCOLS;
    private String ciphers = DistributionConfig.DEFAULT_SSL_CIPHERS;
    private boolean requireAuth = DistributionConfig.DEFAULT_SSL_REQUIRE_AUTHENTICATION;
    private String keystore = DistributionConfig.DEFAULT_SSL_KEYSTORE;
    private String keystoreType = KeyStore.getDefaultType();
    private String keystorePassword = DistributionConfig.DEFAULT_SSL_KEYSTORE_PASSWORD;
    private String truststore = DistributionConfig.DEFAULT_SSL_TRUSTSTORE;
    private String truststorePassword = DistributionConfig.DEFAULT_SSL_TRUSTSTORE_PASSWORD;
    private String truststoreType = KeyStore.getDefaultType();
    private String alias = null;
    private SecurableCommunicationChannel securableCommunicationChannel = null;
    private Properties properties = new Properties();
    private SSLParameterExtension sslParameterExtension = null;

    public Builder() {}

    public SSLConfig build() {
      return new SSLConfig(endpointIdentification, useDefaultSSLContext, enabled,
          protocols, ciphers, requireAuth, keystore, keystoreType, keystorePassword,
          truststore, truststorePassword, truststoreType, alias, securableCommunicationChannel,
          properties, sslParameterExtension);
    }

    public Builder setAlias(final String alias) {
      this.alias = alias;
      return this;
    }

    public Builder setEndpointIdentificationEnabled(boolean endpointIdentification) {
      this.endpointIdentification = endpointIdentification;
      return this;
    }

    public Builder setKeystore(final String keystore) {
      this.keystore = keystore;
      return this;
    }

    public Builder setKeystorePassword(final String keystorePassword) {
      this.keystorePassword = keystorePassword;
      return this;
    }

    public Builder setKeystoreType(final String keystoreType) {
      this.keystoreType = keystoreType;
      return this;
    }

    public Builder setTruststore(final String truststore) {
      this.truststore = truststore;
      return this;
    }

    public Builder setTruststorePassword(final String truststorePassword) {
      this.truststorePassword = truststorePassword;
      return this;
    }

    public Builder setEnabled(boolean enabled) {
      this.enabled = enabled;
      return this;
    }

    public Builder setUseDefaultSSLContext(boolean useDefaultSSLContext) {
      this.useDefaultSSLContext = useDefaultSSLContext;
      return this;
    }

    public Builder setProtocols(String protocols) {
      this.protocols = protocols;
      return this;
    }

    public Builder setCiphers(String ciphers) {
      this.ciphers = ciphers;
      return this;
    }

    public Builder setRequireAuth(boolean requireAuth) {
      this.requireAuth = requireAuth;
      return this;
    }

    public Builder setTruststoreType(final String truststoreType) {
      this.truststoreType = truststoreType;
      return this;
    }

    public Builder setProperties(Properties newProps) {
      this.properties = new Properties();
      for (Iterator iter = newProps.keySet().iterator(); iter.hasNext();) {
        String key = (String) iter.next();
        this.properties.setProperty(key, newProps.getProperty(key));
      }
      return this;
    }

    public Builder setSecurableCommunicationChannel(
        final SecurableCommunicationChannel securableCommunicationChannel) {
      this.securableCommunicationChannel = securableCommunicationChannel;
      return this;
    }

    public Builder setSSLParameterExtension(
        final String sslParameterExtensionConfig) {
      if (StringUtils.isBlank(sslParameterExtensionConfig)) {
        this.sslParameterExtension = null;
        return this;
      }
      InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();

      if (ids == null) {
        this.sslParameterExtension = null;
        return this;
      }

      SSLParameterExtension sslParameterExtension =
          CallbackInstantiator.getObjectOfTypeFromClassName(sslParameterExtensionConfig,
              SSLParameterExtension.class);
      ids.getConfig().getDistributedSystemId();

      sslParameterExtension.init(
          new SSLParameterExtensionContextImpl(ids.getConfig().getDistributedSystemId()));
      this.sslParameterExtension = sslParameterExtension;
      return this;
    }

    public String getKeystore() {
      return keystore;
    }

    public String getKeystoreType() {
      return keystoreType;
    }

    public String getKeystorePassword() {
      return keystorePassword;
    }

    public String getTruststore() {
      return truststore;
    }

    public String getTruststorePassword() {
      return truststorePassword;
    }

    public String getTruststoreType() {
      return truststoreType;
    }
  }

}
