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
import static org.apache.geode.distributed.ConfigurationProperties.SSL_USE_DEFAULT_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.Serializable;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.web.client.ResourceAccessException;

import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.builder.GeodeClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class ClusterManagementServiceOnServerTest implements Serializable {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private final File keyFile = new File(ClusterManagementServiceOnServerTest.class.getClassLoader()
      .getResource("ssl/trusted.keystore").getFile());

  private MemberVM locator, server;
  private Properties sslProps;
  private Region regionConfig;

  @Before
  public void before() throws Exception {
    sslProps = new Properties();
    sslProps.setProperty(SSL_KEYSTORE, keyFile.getCanonicalPath());
    sslProps.setProperty(SSL_TRUSTSTORE, keyFile.getCanonicalPath());
    sslProps.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    sslProps.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    sslProps.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannel.WEB.getConstant());

    regionConfig = new Region();
    regionConfig.setName("test");
    regionConfig.setType(RegionType.PARTITION);
  }

  @Test
  public void serverHasNoSslPropertyAndDoNotUseDefaultSSL() {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService().withProperties(sslProps));
    int locatorPort = locator.getPort();
    server = cluster.startServerVM(1, s -> s.withConnectionToLocator(locatorPort));

    server.invoke(() -> {
      assertThatThrownBy(
          () -> new GeodeClusterManagementServiceBuilder()
              .setCache(ClusterStartupRule.getCache())
              .build())
                  .isInstanceOf(IllegalStateException.class);
    });
  }

  @Test
  public void serverHasNoSslPropertyAndDoUseIncorrectDefaultSSL() {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService().withProperties(sslProps));
    int locatorPort = locator.getPort();
    Properties serverProps = new Properties();
    serverProps.setProperty(SSL_USE_DEFAULT_CONTEXT, "true");
    server = cluster.startServerVM(1,
        s -> s.withConnectionToLocator(locatorPort).withProperties(serverProps));

    server.invoke(() -> {
      ClusterManagementService service = new GeodeClusterManagementServiceBuilder()
          .setCache(ClusterStartupRule.getCache())
          .build();
      assertThat(service).isNotNull();
      assertThatThrownBy(() -> service.create(regionConfig))
          .isInstanceOf(ResourceAccessException.class);
    });

    // the default ssl context is static and will probably pollute future tests
    server.getVM().bounce();
  }

  @Test
  public void serverHasNoSslPropertyAndDoUseCorrectDefaultSSL() {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService().withProperties(sslProps));
    int locatorPort = locator.getPort();
    Properties serverProps = new Properties();
    serverProps.setProperty(SSL_USE_DEFAULT_CONTEXT, "true");
    server = cluster.startServerVM(1,
        s -> s.withConnectionToLocator(locatorPort).withProperties(serverProps));

    server.invoke(() -> {
      System.setProperty("javax.net.ssl.keyStore", keyFile.getCanonicalPath());
      System.setProperty("javax.net.ssl.keyStorePassword", "password");
      System.setProperty("javax.net.ssl.keyStoreType", "JKS");
      System.setProperty("javax.net.ssl.trustStore", keyFile.getCanonicalPath());
      System.setProperty("javax.net.ssl.trustStorePassword", "password");
      System.setProperty("javax.net.ssl.trustStoreType", "JKS");

      ClusterManagementService service =
          new GeodeClusterManagementServiceBuilder()
              .setCache(ClusterStartupRule.getCache())
              .build();
      assertThat(service).isNotNull();
      ClusterManagementResult clusterManagementResult =
          service.create(regionConfig);
      assertThat(clusterManagementResult.isSuccessful()).isTrue();
    });

    // the default ssl context is static and will probably pollute future tests
    server.getVM().bounce();
  }

  @Test
  public void useDefaultSSLPropertyTakesPrecedence() {
    locator = cluster.startLocatorVM(0, l -> l.withHttpService().withProperties(sslProps));
    int locatorPort = locator.getPort();
    Properties serverProps = new Properties(sslProps);
    serverProps.setProperty(SSL_USE_DEFAULT_CONTEXT, "true");
    server = cluster.startServerVM(1,
        s -> s.withConnectionToLocator(locatorPort).withProperties(serverProps));

    server.invoke(() -> {
      // default SSL context not set here, and ssl config inside sslProps is ignored because
      // use_default_ssl_context is true
      ClusterManagementService service =
          new GeodeClusterManagementServiceBuilder()
              .setCache(ClusterStartupRule.getCache())
              .build();
      assertThat(service).isNotNull();
      assertThatThrownBy(() -> service.create(regionConfig))
          .isInstanceOf(ResourceAccessException.class);
    });

    // the default ssl context is static and will probably pollute future tests
    server.getVM().bounce();
  }
}
