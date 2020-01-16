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

package org.apache.geode.management.internal.rest;

import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.Properties;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.api.ClusterManagementServiceConnectionConfig;
import org.apache.geode.management.internal.api.GeodeClusterManagementServiceConnectionConfig;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class GeodeClusterManagementServiceConnectionConfigTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(3);

  private static MemberVM locator, server;
  private static ClientVM client;

  @BeforeClass
  public static void beforeClass() throws Exception {
    File keyFile = new File(ClientClusterManagementSSLTest.class.getClassLoader()
        .getResource("ssl/trusted.keystore").getFile());
    Properties sslProps = new Properties();
    sslProps.setProperty(SSL_KEYSTORE, keyFile.getCanonicalPath());
    sslProps.setProperty(SSL_TRUSTSTORE, keyFile.getCanonicalPath());
    sslProps.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    sslProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    sslProps.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    locator = cluster.startLocatorVM(0, l -> l.withHttpService().withProperties(sslProps)
        .withSecurityManager(SimpleSecurityManager.class));

    int locatorPort = locator.getPort();
    server = cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort)
        .withProperties(sslProps)
        .withCredential("cluster", "cluster"));

    client = cluster.startClientVM(2, c -> c.withLocatorConnection(locatorPort)
        .withProperties(sslProps)
        .withCredential("cluster", "cluster"));
  }

  @Test
  public void connectionConfigPopulatesCorrectlyFromInternalCache() {
    int port = locator.getHttpPort();
    server.invoke(() -> {
      ClusterManagementServiceConnectionConfig connectionConfig =
          new GeodeClusterManagementServiceConnectionConfig(ClusterStartupRule.getCache());
      assertThat(connectionConfig.getPort()).isEqualTo(port);
      assertThat(connectionConfig.getHost()).isEqualTo("localhost");
      assertThat(connectionConfig.getSslContext()).isNotNull();
      assertThat(connectionConfig.getHostnameVerifier()).isInstanceOf(NoopHostnameVerifier.class);
      assertThat(connectionConfig.getAuthToken()).isEqualTo(null);
      assertThat(connectionConfig.getUsername()).isEqualTo("cluster");
      assertThat(connectionConfig.getPassword()).isEqualTo("cluster");
    });
  }

  @Test
  public void invokeFromClientCacheWithLocatorPool() {
    int port = locator.getHttpPort();
    client.invoke(() -> {
      ClusterManagementServiceConnectionConfig connectionConfig =
          new GeodeClusterManagementServiceConnectionConfig(ClusterStartupRule.getClientCache());
      assertThat(connectionConfig.getPort()).isEqualTo(port);
      assertThat(connectionConfig.getHost()).isEqualTo("localhost");
      assertThat(connectionConfig.getSslContext()).isNotNull();
      assertThat(connectionConfig.getHostnameVerifier()).isInstanceOf(NoopHostnameVerifier.class);
      assertThat(connectionConfig.getAuthToken()).isEqualTo(null);
      assertThat(connectionConfig.getUsername()).isEqualTo("cluster");
      assertThat(connectionConfig.getPassword()).isEqualTo("cluster");
    });
  }

  @AfterClass
  public static void teardown() {
    client.stop();
    server.stop();
    locator.stop();
  }
}
