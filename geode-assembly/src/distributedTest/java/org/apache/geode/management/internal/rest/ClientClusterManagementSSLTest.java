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

import static org.apache.geode.cache.Region.SEPARATOR;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.lang.Identifiable.find;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.web.client.ResourceAccessException;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.RealizationResult;
import org.apache.geode.management.builder.GeodeClusterManagementServiceBuilder;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class ClientClusterManagementSSLTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(3);

  private static MemberVM locator, server;
  private static VM client;

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

    client = cluster.getVM(2);

    client.invoke(() -> {
      System.setProperty("javax.net.ssl.keyStore", keyFile.getCanonicalPath());
      System.setProperty("javax.net.ssl.keyStorePassword", "password");
      System.setProperty("javax.net.ssl.keyStoreType", "JKS");
      System.setProperty("javax.net.ssl.trustStore", keyFile.getCanonicalPath());
      System.setProperty("javax.net.ssl.trustStorePassword", "password");
      System.setProperty("javax.net.ssl.trustStoreType", "JKS");
    });
  }

  @Test
  public void createRegion_Successful() {
    Region region = new Region();
    region.setName("customer");
    region.setType(RegionType.PARTITION);
    int httpPort = locator.getHttpPort();

    client.invoke(() -> {
      SSLContext sslContext = SSLContext.getDefault();
      HostnameVerifier hostnameVerifier = new NoopHostnameVerifier();
      ClusterManagementService cmsClient =
          new ClusterManagementServiceBuilder()
              .setPort(httpPort)
              .setSslContext(sslContext)
              .setUsername("dataManage")
              .setPassword("dataManage")
              .setHostnameVerifier(hostnameVerifier)
              .build();

      ClusterManagementRealizationResult result = cmsClient.create(region);
      assertThat(result.isSuccessful()).isTrue();
      assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
      assertThat(result.getMemberStatuses()).extracting(RealizationResult::getMemberName)
          .containsExactly("server-1");
    });
  }

  @Test
  public void createRegion_NoSsl() {
    Region region = new Region();
    region.setName("customer");
    region.setType(RegionType.PARTITION);
    int httpPort = locator.getHttpPort();

    client.invoke(() -> {
      ClusterManagementService cmsClient =
          new ClusterManagementServiceBuilder()
              .setPort(httpPort)
              .setSslContext(null)
              .setUsername("dataManage")
              .setPassword("dataManage")
              .build();

      assertThatThrownBy(() -> cmsClient.create(region))
          .isInstanceOf(ResourceAccessException.class);
    });
  }

  @Test
  public void createRegion_WrongPassword() {
    Region region = new Region();
    region.setName("customer");
    region.setType(RegionType.PARTITION);
    int httpPort = locator.getHttpPort();

    client.invoke(() -> {
      SSLContext sslContext = SSLContext.getDefault();
      HostnameVerifier hostnameVerifier = new NoopHostnameVerifier();
      ClusterManagementService cmsClient =
          new ClusterManagementServiceBuilder()
              .setPort(httpPort)
              .setSslContext(sslContext)
              .setUsername("dataManage")
              .setPassword("wrongPassword")
              .setHostnameVerifier(hostnameVerifier)
              .build();

      assertThatThrownBy(() -> cmsClient.create(region)).hasMessageContaining("UNAUTHENTICATED");
    });
  }

  @Test
  public void createRegion_NoUser() {
    Region region = new Region();
    region.setName("customer");
    region.setType(RegionType.PARTITION);
    int httpPort = locator.getHttpPort();

    client.invoke(() -> {
      SSLContext sslContext = SSLContext.getDefault();
      HostnameVerifier hostnameVerifier = new NoopHostnameVerifier();

      ClusterManagementService cmsClient =
          new ClusterManagementServiceBuilder()
              .setPort(httpPort)
              .setSslContext(sslContext)
              .setHostnameVerifier(hostnameVerifier)
              .build();

      assertThatThrownBy(() -> cmsClient.create(region)).hasMessageContaining("UNAUTHENTICATED");
    });
  }

  @Test
  public void createRegion_NoPassword() {
    Region region = new Region();
    region.setName("customer");
    region.setType(RegionType.PARTITION);
    int httpPort = locator.getHttpPort();

    client.invoke(() -> {
      SSLContext sslContext = SSLContext.getDefault();
      HostnameVerifier hostnameVerifier = new NoopHostnameVerifier();
      ClusterManagementService cmsClient =
          new ClusterManagementServiceBuilder()
              .setPort(httpPort)
              .setSslContext(sslContext)
              .setUsername("dataManage")
              .setPassword(null)
              .setHostnameVerifier(hostnameVerifier)
              .build();

      assertThatThrownBy(() -> cmsClient.create(region)).hasMessageContaining("UNAUTHENTICATED");
    });
  }

  @Test
  public void createRegion_NoPrivilege() {
    Region region = new Region();
    region.setName("customer");
    region.setType(RegionType.PARTITION);
    int httpPort = locator.getHttpPort();

    client.invoke(() -> {
      SSLContext sslContext = SSLContext.getDefault();
      HostnameVerifier hostnameVerifier = new NoopHostnameVerifier();
      ClusterManagementService cmsClient =
          new ClusterManagementServiceBuilder()
              .setPort(httpPort)
              .setSslContext(sslContext)
              .setUsername("dataRead")
              .setPassword("dataRead")
              .setHostnameVerifier(hostnameVerifier)
              .build();

      assertThatThrownBy(() -> cmsClient.create(region)).hasMessageContaining("UNAUTHORIZED");
    });
  }

  @Test
  public void invokeFromServer() {
    server.invoke(() -> {
      // when getting the service from the server, we don't need to provide the host information
      ClusterManagementService cmsClient =
          new GeodeClusterManagementServiceBuilder()
              .setCache(ClusterStartupRule.getCache())
              .setUsername("dataManage")
              .setPassword("dataManage")
              .build();
      Region region = new Region();
      region.setName("orders");
      region.setType(RegionType.PARTITION);
      cmsClient.create(region);

      // verify that the region is created on the server
      assertThat(ClusterStartupRule.getCache().getRegion(SEPARATOR + "orders")).isNotNull();
    });

    // verify that the configuration is persisted on the locator
    locator.invoke(() -> {
      CacheConfig cacheConfig =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService()
              .getCacheConfig("cluster");
      assertThat(find(cacheConfig.getRegions(), "orders")).isNotNull();
    });
  }
}
