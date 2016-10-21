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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.util.test.TestUtil;

/**
 * @since GemFire 8.1
 */
@Category(IntegrationTest.class)
public class HTTPServiceSSLSupportJUnitTest {

  private File jks;


  @Before
  public void setUp() throws Exception {
    jks = findTrustedJKS();
  }

  @After
  public void tearDown() throws Exception {
    System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "javax.net.ssl.keyStore");
    System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "javax.net.ssl.keyStorePassword");
    System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "javax.net.ssl.trustStore");
    System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "javax.net.ssl.trustStorePassword");
    System.clearProperty("gemfireSecurityPropertyFile");
  }

  private static File findTrustedJKS() {
    return new File(
        TestUtil.getResourcePath(HTTPServiceSSLSupportJUnitTest.class, "/ssl/trusted.keystore"));
  }

  public static String makePath(String[] strings) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < strings.length; i++) {
      sb.append(strings[i]);
      sb.append(File.separator);
    }
    return sb.toString();
  }

  // @Ignore("disabled for unknown reason")
  @Test
  public void testSSLWithClusterSSL() throws Exception {

    Properties localProps = new Properties();
    localProps.setProperty(MCAST_PORT, "0");
    localProps.setProperty(CLUSTER_SSL_ENABLED, "true");
    localProps.setProperty(CLUSTER_SSL_KEYSTORE, jks.getCanonicalPath());
    localProps.setProperty(CLUSTER_SSL_KEYSTORE_PASSWORD, "password");
    localProps.setProperty(CLUSTER_SSL_KEYSTORE_TYPE, "JKS");
    localProps.setProperty(CLUSTER_SSL_PROTOCOLS, "SSL");
    localProps.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION, "true");
    localProps.setProperty(CLUSTER_SSL_TRUSTSTORE, jks.getCanonicalPath());
    localProps.setProperty(CLUSTER_SSL_TRUSTSTORE_PASSWORD, "password");

    DistributionConfigImpl config = new DistributionConfigImpl(localProps);

    assertEquals(config.getHttpServiceSSLEnabled(), true);
    assertEquals(config.getHttpServiceSSLKeyStore(), jks.getCanonicalPath());
    assertEquals(config.getHttpServiceSSLKeyStorePassword(), "password");
    assertEquals(config.getHttpServiceSSLKeyStoreType(), "JKS");
    assertEquals(config.getHttpServiceSSLProtocols(), "SSL");
    assertEquals(config.getHttpServiceSSLRequireAuthentication(), true);
    assertEquals(config.getHttpServiceSSLTrustStore(), jks.getCanonicalPath());
    assertEquals(config.getHttpServiceSSLTrustStorePassword(), "password");
  }

  @Test
  public void testSSLWithDeprecatedClusterSSL_HTTPService() throws Exception {

    Properties localProps = new Properties();
    localProps.setProperty(MCAST_PORT, "0");
    localProps.setProperty(CLUSTER_SSL_ENABLED, "true");
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "javax.net.ssl.keyStore",
        jks.getCanonicalPath());
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "javax.net.ssl.keyStorePassword",
        "password");

    localProps.setProperty(CLUSTER_SSL_PROTOCOLS, "SSL");
    localProps.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION, "true");
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "javax.net.ssl.trustStore",
        jks.getCanonicalPath());
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "javax.net.ssl.trustStorePassword",
        "password");

    DistributionConfigImpl config = new DistributionConfigImpl(localProps);

    assertEquals(true, config.getHttpServiceSSLEnabled());
    assertEquals("SSL", config.getHttpServiceSSLProtocols());
    assertEquals(true, config.getHttpServiceSSLRequireAuthentication());

    assertEquals(jks.getCanonicalPath(),
        config.getHttpServiceSSLProperties().get("javax.net.ssl.keyStore"));
    assertEquals("password",
        config.getHttpServiceSSLProperties().get("javax.net.ssl.keyStorePassword"));
    // assertIndexDetailsEquals(system.getConfig().getHttpServiceSSLKeyStoreType(),"JKS");
    assertEquals(jks.getCanonicalPath(),
        config.getHttpServiceSSLProperties().get("javax.net.ssl.trustStore"));
    assertEquals("password",
        config.getHttpServiceSSLProperties().get("javax.net.ssl.trustStorePassword"));

  }

  @Test
  public void testSSLWithDeprecatedClusterSSL_HTTPService_WithSSL_Properties() throws Exception {

    Properties localProps = new Properties();
    localProps.setProperty(MCAST_PORT, "0");
    localProps.setProperty(CLUSTER_SSL_ENABLED, "true");

    localProps.setProperty(CLUSTER_SSL_PROTOCOLS, "SSL");
    localProps.setProperty(CLUSTER_SSL_REQUIRE_AUTHENTICATION, "true");

    Properties sslProps = new Properties();
    sslProps.setProperty("javax.net.ssl.keyStore", jks.getCanonicalPath());
    sslProps.setProperty("javax.net.ssl.keyStorePassword", "password");
    sslProps.setProperty("javax.net.ssl.trustStore", jks.getCanonicalPath());
    sslProps.setProperty("javax.net.ssl.trustStorePassword", "password");

    localProps.putAll(sslProps);

    DistributionConfigImpl config = new DistributionConfigImpl(localProps);

    assertEquals(config.getHttpServiceSSLEnabled(), true);
    assertEquals(config.getHttpServiceSSLProtocols(), "SSL");
    assertEquals(config.getHttpServiceSSLRequireAuthentication(), true);

    assertEquals(jks.getCanonicalPath(),
        config.getHttpServiceSSLProperties().get("javax.net.ssl.keyStore"));
    assertEquals("password",
        config.getHttpServiceSSLProperties().get("javax.net.ssl.keyStorePassword"));
    assertEquals(jks.getCanonicalPath(),
        config.getHttpServiceSSLProperties().get("javax.net.ssl.trustStore"));
    assertEquals("password",
        config.getHttpServiceSSLProperties().get("javax.net.ssl.trustStorePassword"));

  }


}
