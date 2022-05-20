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

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;

import java.security.KeyStore;
import java.util.Properties;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
  private final String serverProtocols;
  private final String clientProtocols;
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

  private SSLConfig(final boolean endpointIdentification,
      final boolean useDefaultSSLContext,
      final boolean enabled,
      final @NotNull String protocols,
      final @Nullable String clientProtocols,
      final @Nullable String serverProtocols,
      final String ciphers,
      final boolean requireAuth,
      final String keystore,
      final String keystoreType,
      final String keystorePassword,
      final String truststore,
      final String truststorePassword,
      final String truststoreType,
      final String alias,
      final SecurableCommunicationChannel securableCommunicationChannel,
      final Properties properties,
      final SSLParameterExtension sslParameterExtension) {
    this.endpointIdentification = endpointIdentification;
    this.useDefaultSSLContext = useDefaultSSLContext;
    this.enabled = enabled;
    this.protocols = protocols;
    this.clientProtocols = isEmpty(clientProtocols) ? protocols : clientProtocols;
    this.serverProtocols = isEmpty(serverProtocols) ? protocols : serverProtocols;
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
    return endpointIdentification;
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
    return enabled;
  }

  public boolean useDefaultSSLContext() {
    return useDefaultSSLContext;
  }

  public String getClientProtocols() {
    return clientProtocols;
  }

  public @NotNull String[] getClientProtocolsAsStringArray() {
    return SSLUtil.split(clientProtocols);
  }

  public String getServerProtocols() {
    return serverProtocols;
  }

  public @NotNull String[] getServerProtocolsAsStringArray() {
    return SSLUtil.split(serverProtocols);
  }

  public String getCiphers() {
    return ciphers;
  }

  public @NotNull String[] getCiphersAsStringArray() {
    return SSLUtil.split(ciphers);
  }

  public boolean isRequireAuth() {
    return requireAuth;
  }

  public String getTruststoreType() {
    return truststoreType;
  }

  public Properties getProperties() {
    return properties;
  }

  public SecurableCommunicationChannel getSecuredCommunicationChannel() {
    return securableCommunicationChannel;
  }

  public SSLParameterExtension getSSLParameterExtension() {
    return sslParameterExtension;
  }

  /**
   * Checks if "any" cipher is specified in {@link #getCiphers()}
   *
   * @return {@code true} if ciphers is either {@code null}, empty or is set to "any"
   *         (ignoring case), otherwise {@code false}.
   */
  public boolean isAnyCiphers() {
    return isAnyCiphers(ciphers);
  }

  /**
   * Checks if "any" cipher is specified in {@code ciphers}.
   *
   * @param ciphers Comma or space separated list of cipher names.
   * @return {@code true} if {@code ciphers} is either {@code null}, empty or is set to "any"
   *         (ignoring case), otherwise {@code false}.
   */
  public static boolean isAnyCiphers(final String ciphers) {
    return StringUtils.isBlank(ciphers) || "any".equalsIgnoreCase(ciphers);
  }

  /**
   * Checks if "any" cipher is specified in {@code ciphers}.
   *
   * @param ciphers Array of cipher names.
   * @return {@code true} if {@code ciphers} is either {@code null}, empty or first entry is "any"
   *         (ignoring case), otherwise {@code false}.
   */
  public static boolean isAnyCiphers(final String... ciphers) {
    return ArrayUtils.isEmpty(ciphers) || "any".equalsIgnoreCase(ciphers[0]);
  }

  /**
   * Checks if "any" protocol is specified in {@code protocols}.
   *
   * @param protocols Comma or space separated list of protocol names.
   * @return {@code true} if {@code protocols} is either {@code null}, empty or is set to "any"
   *         (ignoring case), otherwise {@code false}.
   */
  public static boolean isAnyProtocols(final String protocols) {
    return StringUtils.isBlank(protocols) || "any".equalsIgnoreCase(protocols);
  }

  /**
   * Checks if "any" protocol is specified in {@code protocols}.
   *
   * @param protocols Array of protocol names.
   * @return {@code true} if {@code protocols} is either {@code null}, empty or first entry is "any"
   *         (ignoring case), otherwise {@code false}.
   */
  public static boolean isAnyProtocols(final String... protocols) {
    return ArrayUtils.isEmpty(protocols) || "any".equalsIgnoreCase(protocols[0]);
  }

  @Override
  public String toString() {
    return "SSLConfig{" + "enabled=" + enabled + ", protocols='" + protocols + '\''
        + ", clientProtocols='" + clientProtocols + '\'' + ", serverProtocols='" + serverProtocols
        + '\'' + ", ciphers='" + ciphers + '\'' + ", requireAuth=" + requireAuth + ", keystore='"
        + keystore + '\'' + ", keystoreType='" + keystoreType + '\'' + ", keystorePassword='"
        + keystorePassword + '\'' + ", truststore='" + truststore + '\'' + ", truststorePassword='"
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
    props.setProperty(CLUSTER_SSL_ENABLED, String.valueOf(enabled));

    if (enabled) {
      props.setProperty(CLUSTER_SSL_PROTOCOLS, protocols);
      props.setProperty(CLUSTER_SSL_CIPHERS, ciphers);
      props.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION, String.valueOf(requireAuth));
    }
  }

  public static Builder builder() {
    return new Builder();
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
    private String clientProtocols = DistributionConfig.DEFAULT_SSL_CLIENT_PROTOCOLS;
    private String serverProtocols = DistributionConfig.DEFAULT_SSL_SERVER_PROTOCOLS;
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
          protocols, clientProtocols, serverProtocols, ciphers, requireAuth, keystore, keystoreType,
          keystorePassword, truststore, truststorePassword, truststoreType, alias,
          securableCommunicationChannel, properties, sslParameterExtension);
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

    public Builder setClientProtocols(String clientProtocols) {
      this.clientProtocols = clientProtocols;
      return this;
    }

    public Builder setServerProtocols(String serverProtocols) {
      this.serverProtocols = serverProtocols;
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
      properties = new Properties();
      for (final Object o : newProps.keySet()) {
        String key = (String) o;
        properties.setProperty(key, newProps.getProperty(key));
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
        sslParameterExtension = null;
        return this;
      }
      InternalDistributedSystem ids = InternalDistributedSystem.getAnyInstance();

      if (ids == null) {
        sslParameterExtension = null;
        return this;
      }

      SSLParameterExtension sslParameterExtension =
          CallbackInstantiator.getObjectOfTypeFromClassName(sslParameterExtensionConfig,
              SSLParameterExtension.class);

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
