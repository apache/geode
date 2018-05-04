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
package org.apache.geode.security.generator;

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_REQUIRE_AUTHENTICATION;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.security.AuthenticationFailedException;

public class SSLCredentialGenerator extends CredentialGenerator {

  private static final Logger logger = LogService.getLogger();

  @Override
  protected Properties initialize() throws IllegalArgumentException {
    this.javaProperties = getValidJavaSSLProperties();
    return getSSLProperties();
  }

  @Override
  public ClassCode classCode() {
    return ClassCode.SSL;
  }

  @Override
  public String getAuthInit() {
    return null;
  }

  @Override
  public String getAuthenticator() {
    return null;
  }

  @Override
  public Properties getValidCredentials(int index) {
    this.javaProperties = getValidJavaSSLProperties();
    return getSSLProperties();
  }

  @Override
  public Properties getValidCredentials(final Principal principal) {
    this.javaProperties = getValidJavaSSLProperties();
    return getSSLProperties();
  }

  @Override
  public Properties getInvalidCredentials(final int index) {
    this.javaProperties = getInvalidJavaSSLProperties();
    return getSSLProperties();
  }

  private File findTrustedJKS() {
    final File ssldir = new File(System.getProperty("JTESTS") + "/ssl");
    return new File(ssldir, "trusted.keystore");
  }

  private File findUntrustedJKS() {
    final File ssldir = new File(System.getProperty("JTESTS") + "/ssl");
    return new File(ssldir, "untrusted.keystore");
  }

  private Properties getValidJavaSSLProperties() {
    final File jks = findTrustedJKS();

    try {
      final Properties props = new Properties();
      props.setProperty("javax.net.ssl.trustStore", jks.getCanonicalPath());
      props.setProperty("javax.net.ssl.trustStorePassword", "password");
      props.setProperty("javax.net.ssl.keyStore", jks.getCanonicalPath());
      props.setProperty("javax.net.ssl.keyStorePassword", "password");
      return props;

    } catch (IOException ex) {
      throw new AuthenticationFailedException(
          "SSL: Exception while opening the key store: " + ex.getMessage(), ex);
    }
  }

  private Properties getInvalidJavaSSLProperties() {
    final File jks = findUntrustedJKS();

    try {
      final Properties props = new Properties();
      props.setProperty("javax.net.ssl.trustStore", jks.getCanonicalPath());
      props.setProperty("javax.net.ssl.trustStorePassword", "password");
      props.setProperty("javax.net.ssl.keyStore", jks.getCanonicalPath());
      props.setProperty("javax.net.ssl.keyStorePassword", "password");
      return props;

    } catch (IOException ex) {
      throw new AuthenticationFailedException(
          "SSL: Exception while opening the key store: " + ex.getMessage(), ex);
    }
  }

  private Properties getSSLProperties() {
    Properties props = new Properties();
    props.setProperty(CLUSTER_SSL_CIPHERS, "true");
    props.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION, "true");
    props.setProperty(CLUSTER_SSL_CIPHERS, "SSL_RSA_WITH_3DES_EDE_CBC_SHA");
    props.setProperty(CLUSTER_SSL_PROTOCOLS, "TLSv1");
    return props;
  }
}
