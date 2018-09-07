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
package org.apache.geode.internal.admin;

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;

import java.security.KeyStore;
import java.util.Iterator;
import java.util.Properties;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.internal.SSLUtil;

/**
 * The SSL configuration settings for a GemFire distributed system.
 */
public class SSLConfig {

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

  /**
   * SSL implementation-specific key-value pairs. Each key should be prefixed with
   * <code>javax.net.ssl.</code>
   */
  private Properties properties = new Properties();

  public SSLConfig() {}

  public String getAlias() {
    return alias;
  }

  public void setAlias(final String alias) {
    this.alias = alias;
  }

  public boolean doEndpointIdentification() {
    return this.endpointIdentification;
  }

  public void setEndpointIdentificationEnabled(boolean endpointIdentification) {
    this.endpointIdentification = endpointIdentification;
  }

  public String getKeystore() {
    return keystore;
  }

  public void setKeystore(final String keystore) {
    this.keystore = keystore;
  }

  public String getKeystorePassword() {
    return keystorePassword;
  }

  public void setKeystorePassword(final String keystorePassword) {
    this.keystorePassword = keystorePassword;
  }

  public String getKeystoreType() {
    return keystoreType;
  }

  public void setKeystoreType(final String keystoreType) {
    this.keystoreType = keystoreType;
  }

  public String getTruststore() {
    return truststore;
  }

  public void setTruststore(final String truststore) {
    this.truststore = truststore;
  }

  public String getTruststorePassword() {
    return truststorePassword;
  }

  public void setTruststorePassword(final String truststorePassword) {
    this.truststorePassword = truststorePassword;
  }

  public boolean isEnabled() {
    return this.enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean useDefaultSSLContext() {
    return this.useDefaultSSLContext;
  }

  public void setUseDefaultSSLContext(boolean useDefaultSSLContext) {
    this.useDefaultSSLContext = useDefaultSSLContext;
  }

  public String getProtocols() {
    return this.protocols;
  }

  public String[] getProtocolsAsStringArray() {
    return SSLUtil.readArray(this.protocols);
  }

  public void setProtocols(String protocols) {
    this.protocols = protocols;
  }

  public String getCiphers() {
    return this.ciphers;
  }

  public String[] getCiphersAsStringArray() {
    return SSLUtil.readArray(this.ciphers);
  }

  public void setCiphers(String ciphers) {
    this.ciphers = ciphers;
  }

  public boolean isRequireAuth() {
    return this.requireAuth;
  }

  public void setRequireAuth(boolean requireAuth) {
    this.requireAuth = requireAuth;
  }

  public String getTruststoreType() {
    return truststoreType;
  }

  public void setTruststoreType(final String truststoreType) {
    this.truststoreType = truststoreType;
  }

  public Properties getProperties() {
    return this.properties;
  }

  public void setProperties(Properties newProps) {
    this.properties = new Properties();
    for (Iterator iter = newProps.keySet().iterator(); iter.hasNext();) {
      String key = (String) iter.next();
      // String value = newProps.getProperty(key);
      this.properties.setProperty(key, newProps.getProperty(key));
    }
  }

  public SecurableCommunicationChannel getSecuredCommunicationChannel() {
    return securableCommunicationChannel;
  }

  public void setSecurableCommunicationChannel(
      final SecurableCommunicationChannel securableCommunicationChannel) {
    this.securableCommunicationChannel = securableCommunicationChannel;
  }

  @Override
  public String toString() {
    return "SSLConfig{" + "enabled=" + enabled + ", protocols='" + protocols + '\'' + ", ciphers='"
        + ciphers + '\'' + ", requireAuth=" + requireAuth + ", keystore='" + keystore + '\''
        + ", keystoreType='" + keystoreType + '\'' + ", keystorePassword='" + keystorePassword
        + '\'' + ", truststore='" + truststore + '\'' + ", truststorePassword='"
        + truststorePassword + '\'' + ", truststoreType='" + truststoreType + '\'' + ", alias='"
        + alias + '\'' + ", securableCommunicationChannel=" + securableCommunicationChannel
        + ", properties=" + properties + '}';
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

}
