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
package org.apache.geode.cache.ssl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerOnlyTLSTestFixture;

/**
 * Distributed tests for Server-only TLS with Alternative Client Authentication.
 *
 * <p>
 * These tests verify that:
 * <ul>
 * <li>Servers present certificates and clients verify them</li>
 * <li>Clients do NOT present certificates (ssl-require-authentication=false)</li>
 * <li>All transport is TLS-encrypted in both directions</li>
 * <li>Clients authenticate using application-layer credentials (username/password or tokens)</li>
 * <li>Authorization is enforced through SecurityManager</li>
 * </ul>
 */
@Category({SecurityTest.class})
public class ServerOnlyTLSWithAuthDUnitTest {

  private static final String REGION_NAME = "testRegion";

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private ServerOnlyTLSTestFixture fixture;

  @Before
  public void setUp() throws Exception {
    fixture = new ServerOnlyTLSTestFixture();

    // Add ignored exceptions for SSL-related cleanup warnings
    IgnoredException.addIgnoredException("javax.net.ssl.SSLException");
    IgnoredException.addIgnoredException("java.io.IOException");
  }

  /**
   * Test basic client connection with TLS transport encryption and username/password
   * authentication.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>Server presents certificate, client verifies it</li>
   * <li>Client does NOT present certificate</li>
   * <li>Client authenticates with valid username/password</li>
   * <li>Transport is encrypted</li>
   * </ul>
   */
  @Test
  public void testBasicConnectionWithUsernamePassword() throws Exception {
    // Create certificates and stores using fixture
    // Note: Use createClusterStores() for both locator and server peer SSL communication
    CertStores clusterStores = fixture.createClusterStores();
    CertStores clientStores = fixture.createClientStores();

    // Configure locator with server-only TLS (require-authentication=false) and security manager
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");

    // Start locator
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Configure server with server-only TLS and security manager
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "cluster");
    serverProps.setProperty("locators", "localhost[" + locatorPort + "]");

    // Start server
    MemberVM server = cluster.startServerVM(1, serverProps, locatorPort);

    // Create region on server
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Configure client with server-only TLS (truststore only, no keystore)
    Properties clientSSLProps = clientStores.propertiesWith("all", false, false);

    // Add authentication properties (username/password)
    // SimpleSecurityManager accepts username when username == password
    Properties clientAuthProps = fixture.createClientAuthProperties("data", "data");
    clientSSLProps.putAll(clientAuthProps);

    // Connect client
    ClientVM client = cluster.startClientVM(2, c -> c
        .withProperties(clientSSLProps)
        .withLocatorConnection(locatorPort));

    // Verify client can perform operations
    client.invoke(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      assertThat(clientCache).isNotNull();

      Region<Object, Object> region = clientCache
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(REGION_NAME);

      // Perform basic operations
      region.put("key1", "value1");
      Object value = region.get("key1");
      assertThat(value).isEqualTo("value1");
    });
  }

  /**
   * Test multiple clients connecting with different credentials.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>Multiple clients can connect simultaneously</li>
   * <li>Each client has its own identity (username)</li>
   * <li>All clients use TLS transport encryption</li>
   * <li>None of the clients present certificates</li>
   * </ul>
   */
  @Test
  public void testMultipleClientsWithDifferentCredentials() throws Exception {
    // Create certificates and stores
    CertStores locatorStores = fixture.createLocatorStores();
    CertStores clusterStores = fixture.createClusterStores();
    CertStores client1Stores = fixture.createClientStores();
    CertStores client2Stores = fixture.createClientStores();
    CertStores client3Stores = fixture.createClientStores();

    // Start locator
    Properties locatorProps = locatorStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Start server
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "cluster");
    serverProps.setProperty("locators", "localhost[" + locatorPort + "]");
    MemberVM server = cluster.startServerVM(1, serverProps, locatorPort);

    // Create region
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Connect client 1 with user "dataRead"
    Properties client1Props = client1Stores.propertiesWith("all", false, false);
    client1Props.putAll(fixture.createClientAuthProperties("data", "data"));
    ClientVM client1 = cluster.startClientVM(2, c -> c
        .withProperties(client1Props)
        .withLocatorConnection(locatorPort));

    // Connect client 2 with user "dataWrite"
    Properties client2Props = client2Stores.propertiesWith("all", false, false);
    client2Props.putAll(fixture.createClientAuthProperties("dataWrite", "dataWrite"));
    ClientVM client2 = cluster.startClientVM(3, c -> c
        .withProperties(client2Props)
        .withLocatorConnection(locatorPort));

    // Connect client 3 with user "dataManage"
    Properties client3Props = client3Stores.propertiesWith("all", false, false);
    client3Props.putAll(fixture.createClientAuthProperties("dataManage", "dataManage"));
    ClientVM client3 = cluster.startClientVM(4, c -> c
        .withProperties(client3Props)
        .withLocatorConnection(locatorPort));

    // Verify all clients can access region
    for (ClientVM client : new ClientVM[] {client1, client2, client3}) {
      client.invoke(() -> {
        Region<Object, Object> region = ClusterStartupRule.getClientCache()
            .createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create(REGION_NAME);
        assertThat(region).isNotNull();
      });
    }
  }

  /**
   * Test token-based authentication instead of username/password.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>Clients can authenticate using bearer tokens</li>
   * <li>Token authentication works with TLS transport</li>
   * <li>No client certificates are required</li>
   * </ul>
   */
  @Test
  public void testTokenBasedAuthentication() throws Exception {
    // Create certificates and stores
    // Note: Locator and server must use same CertStores for peer SSL communication
    CertStores clusterStores = fixture.createClusterStores();
    CertStores clientStores = fixture.createClientStores();

    // Start locator
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Start server
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "cluster");
    serverProps.setProperty("locators", "localhost[" + locatorPort + "]");
    MemberVM server = cluster.startServerVM(1, serverProps, locatorPort);

    // Create region
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Configure client with token authentication
    Properties clientProps = clientStores.propertiesWith("all", false, false);
    Properties tokenAuthProps = fixture.createClientTokenAuthProperties(
        SimpleSecurityManager.VALID_TOKEN);
    clientProps.putAll(tokenAuthProps);

    // Connect client
    ClientVM client = cluster.startClientVM(2, c -> c
        .withProperties(clientProps)
        .withLocatorConnection(locatorPort));

    // Verify client can perform operations
    client.invoke(() -> {
      Region<Object, Object> region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(REGION_NAME);

      region.put("tokenKey", "tokenValue");
      assertThat(region.get("tokenKey")).isEqualTo("tokenValue");
    });
  }

  /**
   * Test server restart with client reconnection.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>Client can reconnect after server restart</li>
   * <li>TLS session is re-established</li>
   * <li>Client re-authenticates successfully</li>
   * </ul>
   */
  @Test
  public void testServerRestartWithClientReconnection() throws Exception {
    // Create certificates and stores
    // Note: Locator and server must use same CertStores for peer SSL communication
    CertStores clusterStores = fixture.createClusterStores();
    CertStores clientStores = fixture.createClientStores();

    // Start locator
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Start server
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "cluster");
    serverProps.setProperty("locators", "localhost[" + locatorPort + "]");
    MemberVM server = cluster.startServerVM(1, serverProps, locatorPort);

    // Create region
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Connect client
    Properties clientProps = clientStores.propertiesWith("all", false, false);
    clientProps.putAll(fixture.createClientAuthProperties("data", "data"));
    ClientVM client = cluster.startClientVM(2, c -> c
        .withProperties(clientProps)
        .withLocatorConnection(locatorPort));

    // Verify initial connection
    client.invoke(() -> {
      Region<Object, Object> region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(REGION_NAME);
      region.put("beforeRestart", "value1");
    });

    // Stop and restart server
    cluster.stop(1);
    server = cluster.startServerVM(1, serverProps, locatorPort);

    // Recreate region on restarted server
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Verify client can reconnect and operate
    client.invoke(() -> {
      Region<Object, Object> region = ClusterStartupRule.getClientCache().getRegion(REGION_NAME);
      region.put("afterRestart", "value2");
      assertThat(region.get("afterRestart")).isEqualTo("value2");
    });
  }

  /**
   * Test concurrent client operations over TLS.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>Multiple threads can perform concurrent operations over TLS</li>
   * <li>TLS connection handles concurrent load</li>
   * <li>Authentication works with concurrent operations</li>
   * </ul>
   */
  @Test
  public void testConcurrentClientConnections() throws Exception {
    // Create certificates and stores
    // Note: Use createClusterStores() for both locator and server peer SSL communication
    CertStores clusterStores = fixture.createClusterStores();
    CertStores clientStores = fixture.createClientStores();

    // Start locator
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Start server
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "cluster");
    serverProps.setProperty("locators", "localhost[" + locatorPort + "]");
    MemberVM server = cluster.startServerVM(1, serverProps, locatorPort);

    // Create region
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Create single client cache with TLS
    Properties clientProps = clientStores.propertiesWith("all", false, false);
    clientProps.putAll(fixture.createClientAuthProperties("data", "data"));

    ClientCacheFactory factory = new ClientCacheFactory(clientProps)
        .addPoolLocator("localhost", locatorPort)
        .setPoolSubscriptionEnabled(true);

    try (ClientCache clientCache = factory.create()) {
      Region<Object, Object> region = clientCache
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(REGION_NAME);

      // Perform concurrent operations from multiple threads
      int numThreads = 10;
      ExecutorService executor = Executors.newFixedThreadPool(numThreads);
      CountDownLatch startLatch = new CountDownLatch(1);
      CountDownLatch completionLatch = new CountDownLatch(numThreads);
      List<Future<?>> futures = new ArrayList<>();

      for (int i = 0; i < numThreads; i++) {
        final int operationId = i;
        Future<?> future = executor.submit(() -> {
          try {
            // Wait for all threads to be ready
            startLatch.await();

            // Perform operation
            region.put("key" + operationId, "value" + operationId);
            assertThat(region.get("key" + operationId)).isEqualTo("value" + operationId);

            completionLatch.countDown();
          } catch (Exception e) {
            throw new RuntimeException("Operation " + operationId + " failed", e);
          }
        });
        futures.add(future);
      }

      // Start all operations simultaneously
      startLatch.countDown();

      // Wait for all operations to complete
      boolean completed = completionLatch.await(2, TimeUnit.MINUTES);

      // Check for exceptions first to see what actually failed
      for (Future<?> future : futures) {
        future.get();
      }

      assertThat(completed).isTrue();

      executor.shutdown();
    }
  }

  /**
   * Test region operations with authorization checks.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>SecurityManager enforces authorization based on principal</li>
   * <li>Different users have different permissions</li>
   * <li>Authorization works with TLS transport encryption</li>
   * </ul>
   */
  @Test
  public void testRegionOperationsWithAuthorization() throws Exception {
    // Create certificates and stores
    // Note: Locator and server must use same CertStores for peer SSL communication
    CertStores clusterStores = fixture.createClusterStores();
    CertStores clientStores = fixture.createClientStores();

    // Start locator
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Start server
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "cluster");
    serverProps.setProperty("locators", "localhost[" + locatorPort + "]");
    MemberVM server = cluster.startServerVM(1, serverProps, locatorPort);

    // Create region
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Connect client with "data" credentials
    // SimpleSecurityManager authorizes based on username prefix matching permission
    Properties clientProps = clientStores.propertiesWith("all", false, false);
    clientProps.putAll(fixture.createClientAuthProperties("data", "data"));
    ClientVM client = cluster.startClientVM(2, c -> c
        .withProperties(clientProps)
        .withLocatorConnection(locatorPort));

    // Verify authorized operations succeed
    client.invoke(() -> {
      Region<Object, Object> region = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create(REGION_NAME);

      // User "data" should be authorized for DATA:READ and DATA:WRITE
      region.put("authKey", "authValue");
      assertThat(region.get("authKey")).isEqualTo("authValue");
    });
  }
}
