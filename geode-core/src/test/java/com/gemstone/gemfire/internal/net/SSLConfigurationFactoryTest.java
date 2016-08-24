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

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.internal.admin.SSLConfig;
import com.gemstone.gemfire.internal.security.SecurableComponent;
import com.gemstone.gemfire.test.dunit.internal.JUnit4DistributedTestCase;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class SSLConfigurationFactoryTest extends JUnit4DistributedTestCase {

  @After
  public void tearDownTest() {
    SSLConfigurationFactory.close();
  }

  @Test
  public void getSSLConfigForComponentALL() throws Exception {
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
    for (SecurableComponent securableComponent : SecurableComponent.values()) {
      assertSSLConfig(properties, SSLConfigurationFactory.getSSLConfigForComponent(securableComponent), securableComponent, distributionConfig);
    }
  }

  @Test
  public void getSSLConfigForComponentHTTPService() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, SecurableComponent.HTTP_SERVICE.getConstant());
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
    for (SecurableComponent securableComponent : SecurableComponent.values()) {
      assertSSLConfig(properties, SSLConfigurationFactory.getSSLConfigForComponent(securableComponent), securableComponent, distributionConfig);
    }
  }

  @Test
  public void getSSLConfigForComponentHTTPServiceWithAlias() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, SecurableComponent.HTTP_SERVICE.getConstant());
    properties.setProperty(SSL_KEYSTORE, "someKeyStore");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE, "someKeyStore");
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_DEFAULT_ALIAS, "defaultAlias");
    properties.setProperty(SSL_HTTP_SERVICE_ALIAS, "httpAlias");
    properties.setProperty(SSL_CIPHERS, "any");
    properties.setProperty(SSL_PROTOCOLS, "any");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);
    for (SecurableComponent securableComponent : SecurableComponent.values()) {
      assertSSLConfig(properties, SSLConfigurationFactory.getSSLConfigForComponent(securableComponent), securableComponent, distributionConfig);
    }
  }

  @Test
  public void getSSLConfigForComponentHTTPServiceWithMutualAuth() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED_COMPONENTS, SecurableComponent.HTTP_SERVICE.getConstant());
    properties.setProperty(SSL_KEYSTORE, "someKeyStore");
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE, "someKeyStore");
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "keystorePassword");
    properties.setProperty(SSL_DEFAULT_ALIAS, "defaultAlias");
    properties.setProperty(SSL_HTTP_SERVICE_ALIAS, "httpAlias");
    properties.setProperty(SSL_HTTP_SERVICE_REQUIRE_AUTHENTICATION, "true");
    properties.setProperty(SSL_CIPHERS, "any");
    properties.setProperty(SSL_PROTOCOLS, "any");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(properties);
    SSLConfigurationFactory.setDistributionConfig(distributionConfig);
    for (SecurableComponent securableComponent : SecurableComponent.values()) {
      assertSSLConfig(properties, SSLConfigurationFactory.getSSLConfigForComponent(securableComponent), securableComponent, distributionConfig);
    }
  }

  private void assertSSLConfig(final Properties properties,
                               final SSLConfig sslConfig,
                               final SecurableComponent expectedSecurableComponent,
                               final DistributionConfigImpl distributionConfig) {
    assertEquals(isSSLComponentEnabled(expectedSecurableComponent, distributionConfig.getSSLEnabledComponents()), sslConfig.isEnabled());
    assertEquals(properties.getProperty(SSL_KEYSTORE), sslConfig.getKeystore());
    assertEquals(properties.getProperty(SSL_KEYSTORE_PASSWORD), sslConfig.getKeystorePassword());
    assertEquals(properties.getProperty(SSL_KEYSTORE_TYPE), sslConfig.getKeystoreType());
    assertEquals(properties.getProperty(SSL_TRUSTSTORE), sslConfig.getTruststore());
    assertEquals(properties.getProperty(SSL_TRUSTSTORE_PASSWORD), sslConfig.getTruststorePassword());
    assertEquals(properties.getProperty(SSL_CIPHERS), sslConfig.getCiphers());
    assertEquals(properties.getProperty(SSL_PROTOCOLS), sslConfig.getProtocols());
    assertEquals(getCorrectAlias(expectedSecurableComponent, properties), sslConfig.getAlias());
    assertEquals(requiresAuthentication(properties, expectedSecurableComponent), sslConfig.isRequireAuth());
    assertEquals(expectedSecurableComponent, sslConfig.getSecuredComponent());
  }

  private boolean requiresAuthentication(final Properties properties, final SecurableComponent expectedSecurableComponent) {
    boolean defaultAuthentication = expectedSecurableComponent.equals(SecurableComponent.HTTP_SERVICE) ? DistributionConfig.DEFAULT_SSL_HTTP_SERVICE_REQUIRE_AUTHENTICATION : DistributionConfig.DEFAULT_SSL_REQUIRE_AUTHENTICATION;
    String httpRequiresAuthentication = properties.getProperty(SSL_HTTP_SERVICE_REQUIRE_AUTHENTICATION);

    return httpRequiresAuthentication == null ? defaultAuthentication : Boolean.parseBoolean(httpRequiresAuthentication);
  }

  private String getCorrectAlias(final SecurableComponent expectedSecurableComponent, final Properties properties) {
    switch (expectedSecurableComponent) {
      case ALL:
        return properties.getProperty(SSL_DEFAULT_ALIAS);
      case CLUSTER:
        return getAliasForComponent(properties, SSL_CLUSTER_ALIAS);
      case GATEWAY:
        return getAliasForComponent(properties, SSL_GATEWAY_ALIAS);
      case HTTP_SERVICE:
        return getAliasForComponent(properties, SSL_HTTP_SERVICE_ALIAS);
      case JMX:
        return getAliasForComponent(properties, SSL_JMX_MANAGER_ALIAS);
      case LOCATOR:
        return getAliasForComponent(properties, SSL_LOCATOR_ALIAS);
      case SERVER:
        return getAliasForComponent(properties, SSL_SERVER_ALIAS);
      default:
        return properties.getProperty(SSL_DEFAULT_ALIAS);
    }
  }

  private String getAliasForComponent(final Properties properties, final String componentAliasProperty) {
    String aliasProperty = properties.getProperty(componentAliasProperty);
    return !StringUtils.isEmpty(aliasProperty) ? aliasProperty : properties.getProperty(SSL_DEFAULT_ALIAS);
  }

  private boolean isSSLComponentEnabled(final SecurableComponent expectedSecurableComponent, final SecurableComponent[] SecurableComponents) {
    for (SecurableComponent SecurableComponent : SecurableComponents) {
      if (SecurableComponent.ALL.equals(SecurableComponent) || SecurableComponent.equals(expectedSecurableComponent)) {
        return true;
      }
    }
    return false;
  }

}