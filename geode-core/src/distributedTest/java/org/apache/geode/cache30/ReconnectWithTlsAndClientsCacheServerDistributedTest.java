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

package org.apache.geode.cache30;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.geode.distributed.internal.membership.api.MembershipManagerHelper.crashDistributedSystem;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.awaitility.GeodeAwaitility.getTimeout;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.security.GeneralSecurityException;
import java.util.Properties;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.version.VersionManager;

/*
 * This test creates a three member cluster. One member resides in the test JVM so only
 * two additional JVMs are spawned. The test repeatedly causes the test member to force-
 * disconnect, and verifies that the member is able to complete all its reconnection
 * logic successfully.
 *
 * TLS is enabled for all components. A client-facing CacheServer binds to the same
 * port each time the member reconnects. We want to see if there is a race condition
 * between forced-disconnect closing the server socket, and the newly-created
 * CacheServer binding to the same port.
 */
public class ReconnectWithTlsAndClientsCacheServerDistributedTest {
  @Rule
  public final ClusterStartupRule clusterStartupRule = new ClusterStartupRule(2);
  private CacheFactory cacheFactory;
  private Properties geodeConfigurationProperties;
  private int clientsCachePort;

  @Before
  public void before() throws GeneralSecurityException, IOException {
    final Properties geodeConfig = geodeConfigurationProperties();

    final MemberVM locator =
        clusterStartupRule.startLocatorVM(0, 0, VersionManager.CURRENT_VERSION,
            x -> x.withConnectionToLocator().withProperties(geodeConfig)
                .withoutClusterConfigurationService().withoutManagementRestService());

    /*
     * The only purpose of this extra member is to prevent quorum loss when we force-
     * disconnect the local (test) member. Having this member gives us an initial
     * cluster size of 3.
     */
    clusterStartupRule.startServerVM(1, geodeConfig, locator.getPort());

    // now make the test JVM a cluster member too
    geodeConfig.setProperty("locators", "localhost[" + locator.getPort() + "]");
    cacheFactory = new CacheFactory(geodeConfig);

    clientsCachePort = AvailablePortHelper.getRandomAvailableTCPPort();
  }

  @Test
  public void disconnectAndReconnectTest() throws IOException {
    for (int i = 0; i < 5; ++i) {
      cycle(clientsCachePort);
    }
  }

  /*
   * Experiment to see what happens if I bind port ahead of CacheServer.
   * Doing this before addCacheServer() causes an exception
   */
  @Test
  public void preBindToClientsCacheServerPortTest() throws IOException {
    final ServerSocket serverSocket = new ServerSocket();
    serverSocket.setReuseAddress(true);
    serverSocket.bind(new InetSocketAddress(clientsCachePort));

    // AcceptorImpl constructor will keep trying to bind for two minutes and then it'll give up
    assertThatThrownBy(() -> cycle(clientsCachePort)).isInstanceOf(BindException.class);
  }

  private void cycle(final int clientsCachePort) throws IOException {
    final Cache cache = cacheFactory.create();

    startClientsCacheServer(clientsCachePort, cache);

    final DistributedSystem originalDistributedSystem =
        cache.getDistributedSystem();

    crashDistributedSystem(originalDistributedSystem);
    waitToStartReconnecting(originalDistributedSystem);

    final DistributedMember newDistributedSystem =
        waitUntilReconnected(originalDistributedSystem);

    assertThat(newDistributedSystem).as("failed to reconnect")
        .isNotNull();
    assertThat(newDistributedSystem).as("got same distributed system after reconnect")
        .isNotSameAs(originalDistributedSystem);
  }

  private void startClientsCacheServer(final int clientsCachePort, final Cache cache)
      throws IOException {
    final CacheServer cacheServer = cache.addCacheServer();
    cacheServer.setPort(clientsCachePort);
    cacheServer.start();
  }

  private DistributedMember waitUntilReconnected(final DistributedSystem distributedSystem) {
    await().alias("distributed system failed to begin reconnecting")
        .untilAsserted(() -> assertThat(distributedSystem.isReconnecting()).isTrue());

    boolean failure = true;
    try {
      distributedSystem.waitUntilReconnected(getTimeout().toMillis(), MILLISECONDS);
      final DistributedSystem reconnectedSystem = distributedSystem.getReconnectedSystem();
      assertThat(reconnectedSystem).as("there is no reconnected distributed system").isNotNull();
      assertThat(reconnectedSystem.isConnected())
          .as("reconnected distributed system is not connected")
          .isTrue();
      failure = false;
      return reconnectedSystem.getDistributedMember();
    } catch (final InterruptedException e) {
      System.err.println("interrupted while waiting for reconnect");
      return null;
    } finally {
      if (failure) {
        distributedSystem.disconnect();
      }
    }
  }

  // blocks until the local (test) member has begun reconnecting, or has reconnected
  private void waitToStartReconnecting(final DistributedSystem distributedSystem) {
    await()
        .alias("waiting for local (test) member to start reconnecting")
        .untilAsserted(() -> assertThat(distributedSystem).satisfiesAnyOf(
            ds -> assertTrue(ds.isReconnecting()),
            ds -> assertNotNull(ds.getReconnectedSystem())));
  }

  private @NotNull Properties geodeConfigurationProperties()
      throws GeneralSecurityException, IOException {
    // subsequent calls must return the same value so members agree on credentials
    if (geodeConfigurationProperties == null) {
      final CertificateMaterial ca = new CertificateBuilder()
          .commonName("Test CA")
          .isCA()
          .generate();

      final CertificateMaterial serverCertificate = new CertificateBuilder()
          .commonName("member")
          .issuedBy(ca)
          .generate();

      final CertStores memberStore = new CertStores("member");
      memberStore.withCertificate("member", serverCertificate);
      memberStore.trust("ca", ca);
      // we want to exercise the ByteBufferSharing code paths; we don't care about client auth etc
      final Properties props = memberStore.propertiesWith("all", false, false);
      geodeConfigurationProperties = props;
    }
    return geodeConfigurationProperties;
  }
}
