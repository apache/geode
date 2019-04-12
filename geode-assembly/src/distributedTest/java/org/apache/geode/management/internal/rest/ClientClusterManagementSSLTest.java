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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;

import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.springframework.web.client.ResourceAccessException;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.ManagedRegionConfig;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceProvider;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class ClientClusterManagementSSLTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule(2);

  private MemberVM locator, server;
  private ClusterManagementService cmsClient;
  private ManagedRegionConfig region;
  private SSLContext sslContext;
  private HostnameVerifier hostnameVerifier;

  @Before
  public void before() throws Exception {

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

    System.setProperty("javax.net.ssl.keyStore", keyFile.getCanonicalPath());
    System.setProperty("javax.net.ssl.keyStorePassword", "password");
    System.setProperty("javax.net.ssl.keyStoreType", "JKS");
    System.setProperty("javax.net.ssl.trustStore", keyFile.getCanonicalPath());
    System.setProperty("javax.net.ssl.trustStorePassword", "password");
    System.setProperty("javax.net.ssl.trustStoreType", "JKS");

    sslContext = SSLContext.getDefault();
    hostnameVerifier = new NoopHostnameVerifier();

    region = new ManagedRegionConfig();
    region.setName("customer");
  }

  @Test
  public void createRegion_Successful() throws Exception {
    cmsClient = ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort(),
        sslContext, hostnameVerifier, "dataManage", "dataManage");

    ClusterManagementResult result = cmsClient.create(region);
    assertThat(result.isSuccessful()).isTrue();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.OK);
    assertThat(result.getMemberStatuses()).containsKeys("server-1").hasSize(1);
  }

  @Test
  public void createRegion_NoSsl() throws Exception {
    cmsClient = ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort(),
        null, null, "dataManage", "dataManage");
    assertThatThrownBy(() -> cmsClient.create(region)).isInstanceOf(ResourceAccessException.class);
  }

  @Test
  public void createRegion_WrongPassword() throws Exception {
    cmsClient = ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort(),
        sslContext, hostnameVerifier, "dataManage", "wrongPswd");

    ClusterManagementResult result = cmsClient.create(region);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode())
        .isEqualTo(ClusterManagementResult.StatusCode.UNAUTHENTICATED);
  }

  @Test
  public void createRegion_NoUser() throws Exception {
    cmsClient = ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort(),
        sslContext, hostnameVerifier, null, null);

    ClusterManagementResult result = cmsClient.create(region);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode())
        .isEqualTo(ClusterManagementResult.StatusCode.UNAUTHENTICATED);
  }

  @Test
  public void createRegion_NoPassword() throws Exception {
    cmsClient = ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort(),
        sslContext, hostnameVerifier, "dataManage", null);

    ClusterManagementResult result = cmsClient.create(region);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode())
        .isEqualTo(ClusterManagementResult.StatusCode.UNAUTHENTICATED);
  }

  @Test
  public void createRegion_NoPrivilege() throws Exception {
    cmsClient = ClusterManagementServiceProvider.getService("localhost", locator.getHttpPort(),
        sslContext, hostnameVerifier, "dataRead", "dataRead");

    ClusterManagementResult result = cmsClient.create(region);
    assertThat(result.isSuccessful()).isFalse();
    assertThat(result.getStatusCode()).isEqualTo(ClusterManagementResult.StatusCode.UNAUTHORIZED);
  }

  @Test
  public void invokeFromServer() throws Exception {
    server.invoke(() -> {
      // when getting the service from the server, we don't need to provide the host information
      ClusterManagementService cmsClient =
          ClusterManagementServiceProvider.getService("dataManage", "dataManage");
      ManagedRegionConfig region = new ManagedRegionConfig();
      region.setName("orders");
      cmsClient.create(region);

      // verify that the region is created on the server
      assertThat(ClusterStartupRule.getCache().getRegion("/orders")).isNotNull();
    });

    // verify that the configuration is persisted on the locator
    locator.invoke(() -> {
      CacheConfig cacheConfig =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService()
              .getCacheConfig("cluster");
      assertThat(CacheElement.findElement(cacheConfig.getRegions(), "orders")).isNotNull();
    });
  }
}
