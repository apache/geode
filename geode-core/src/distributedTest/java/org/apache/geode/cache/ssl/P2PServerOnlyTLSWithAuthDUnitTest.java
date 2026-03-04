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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.ServerOnlyTLSTestFixture;

/**
 * Distributed tests for Peer-to-Peer (P2P) Cache Topology using Server-only TLS with
 * Application-Layer Authentication (Approach 3).
 *
 * <p>
 * This test demonstrates that in a P2P cache configuration (where all members are peers, no
 * client/server distinction), Approach 3 works correctly:
 * <ul>
 * <li><strong>TLS Encryption:</strong> All peer-to-peer connections use TLS for transport
 * encryption</li>
 * <li><strong>No Certificate Authentication:</strong> Peers do NOT exchange certificates during
 * TLS handshake (ssl-require-authentication=false)</li>
 * <li><strong>Application-Layer Authentication:</strong> Peers authenticate using
 * username/password via SecurityManager</li>
 * <li><strong>Authorization:</strong> SecurityManager enforces CLUSTER:MANAGE permission for peer
 * join</li>
 * </ul>
 *
 * <p>
 * <strong>Key Difference from Client/Server:</strong> In P2P topology, all members are equal peers
 * that communicate directly. Each peer presents a server certificate for TLS encryption, but
 * authentication happens at the application layer using credentials validated by SecurityManager.
 *
 * <p>
 * This approach solves the public CA clientAuth EKU sunset problem for P2P topologies by:
 * <ol>
 * <li>Eliminating the need for client certificates entirely</li>
 * <li>Maintaining full TLS encryption for all transport</li>
 * <li>Using existing authentication infrastructure (LDAP, database, tokens)</li>
 * </ol>
 *
 * @see ServerOnlyTLSWithAuthDUnitTest for client/server topology tests
 */
@Category({SecurityTest.class})
public class P2PServerOnlyTLSWithAuthDUnitTest {

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
    IgnoredException.addIgnoredException("Authentication failed");
    IgnoredException.addIgnoredException("Security check failed");
  }

  /**
   * Test basic P2P cluster formation with server-only TLS and application-layer authentication.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>Locator and servers present TLS certificates (server cert from public or private CA)</li>
   * <li>Peers do NOT present client certificates during TLS handshake</li>
   * <li>All peers authenticate using username/password</li>
   * <li>SecurityManager validates credentials and requires CLUSTER:MANAGE permission</li>
   * <li>Cluster forms successfully with encrypted peer-to-peer communication</li>
   * </ul>
   */
  @Test
  public void testP2PClusterFormationWithServerOnlyTLSAndAppAuth() throws Exception {
    // Create certificate stores using fixture
    // All peers use the same certificate for TLS (server cert)
    CertStores clusterStores = fixture.createClusterStores();

    // Configure locator with:
    // - Server-only TLS (ssl-require-authentication=false)
    // - Security manager for application-layer authentication
    // - Peer authentication credentials
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");

    // Start locator - it will authenticate itself when joining
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Configure first server with same setup
    Properties server1Props = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(server1Props);
    fixture.addPeerAuthProperties(server1Props, "cluster", "cluster");

    // Start first server - it joins via application-layer auth, not certificate auth
    MemberVM server1 = cluster.startServerVM(1, server1Props, locatorPort);

    // Verify server1 successfully joined the cluster
    server1.invoke(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      assertThat(ClusterStartupRule.getCache().getDistributedSystem().getAllOtherMembers())
          .hasSize(1); // locator
    });

    // Configure second server with same setup
    Properties server2Props = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(server2Props);
    fixture.addPeerAuthProperties(server2Props, "cluster", "cluster");

    // Start second server
    MemberVM server2 = cluster.startServerVM(2, server2Props, locatorPort);

    // Verify server2 successfully joined and sees all peers
    server2.invoke(() -> {
      assertThat(ClusterStartupRule.getCache()).isNotNull();
      assertThat(ClusterStartupRule.getCache().getDistributedSystem().getAllOtherMembers())
          .hasSize(2); // locator + server1
    });

    // Verify all peers see each other (peer-to-peer mesh formed)
    server1.invoke(() -> {
      assertThat(ClusterStartupRule.getCache().getDistributedSystem().getAllOtherMembers())
          .hasSize(2); // locator + server2
    });
  }

  /**
   * Test P2P data replication across encrypted peer connections without certificate authentication.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>Data replicates across peers using TLS-encrypted connections</li>
   * <li>No certificate authentication is used (application credentials only)</li>
   * <li>All operations succeed over server-only TLS</li>
   * </ul>
   */
  @Test
  public void testP2PDataReplicationOverServerOnlyTLS() throws Exception {
    CertStores clusterStores = fixture.createClusterStores();

    // Configure and start locator
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Start server1 with replicated region
    Properties server1Props = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(server1Props);
    fixture.addPeerAuthProperties(server1Props, "cluster", "cluster");
    MemberVM server1 = cluster.startServerVM(1, server1Props, locatorPort);

    server1.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Start server2 with same replicated region
    Properties server2Props = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(server2Props);
    fixture.addPeerAuthProperties(server2Props, "cluster", "cluster");
    MemberVM server2 = cluster.startServerVM(2, server2Props, locatorPort);

    server2.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create(REGION_NAME);
    });

    // Put data on server1
    server1.invoke(() -> {
      ClusterStartupRule.getCache()
          .getRegion(REGION_NAME)
          .put("key1", "value1");
    });

    // Verify data replicated to server2 over TLS-encrypted peer connection
    server2.invoke(() -> {
      Object value = ClusterStartupRule.getCache()
          .getRegion(REGION_NAME)
          .get("key1");
      assertThat(value).isEqualTo("value1");
    });

    // Put data on server2
    server2.invoke(() -> {
      ClusterStartupRule.getCache()
          .getRegion(REGION_NAME)
          .put("key2", "value2");
    });

    // Verify data replicated to server1
    server1.invoke(() -> {
      Object value = ClusterStartupRule.getCache()
          .getRegion(REGION_NAME)
          .get("key2");
      assertThat(value).isEqualTo("value2");
    });
  }

  /**
   * Test that peer with invalid credentials cannot join P2P cluster.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>TLS handshake succeeds (server cert validation)</li>
   * <li>Application-layer authentication fails with wrong password</li>
   * <li>Peer is rejected and cannot join cluster</li>
   * </ul>
   */
  @Test
  public void testP2PPeerRejectedWithInvalidCredentials() throws Exception {
    // Add ignored exception for authentication failure messages
    IgnoredException.addIgnoredException("Authentication FAILED");
    CertStores clusterStores = fixture.createClusterStores();

    // Configure and start locator
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Try to start server with INVALID credentials
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "cluster", "wrongPassword"); // INVALID

    // Server should fail to join due to authentication failure
    // Note: Root cause is SecurityException, not GemFireSecurityException
    assertThatThrownBy(() -> cluster.startServerVM(1, serverProps, locatorPort))
        .hasRootCauseInstanceOf(SecurityException.class)
        .hasStackTraceContaining("invalid username/password");
  }

  /**
   * Test that peer without CLUSTER:MANAGE permission cannot join P2P cluster.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>TLS handshake succeeds</li>
   * <li>Application-layer authentication succeeds (valid username/password)</li>
   * <li>Authorization fails (lacks CLUSTER:MANAGE permission)</li>
   * <li>Peer is rejected and cannot join cluster</li>
   * </ul>
   */
  @Test
  public void testP2PPeerRejectedWithoutClusterManagePermission() throws Exception {
    CertStores clusterStores = fixture.createClusterStores();

    // Configure and start locator
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Try to start server with user that has valid credentials but no CLUSTER:MANAGE
    // SimpleSecurityManager allows authentication when username == password
    // but "data" user does NOT have CLUSTER:MANAGE permission
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    fixture.addPeerAuthProperties(serverProps, "data", "data"); // Valid creds, insufficient perms

    // Server should fail to join due to authorization failure
    // Note: Root cause is SecurityException, not GemFireSecurityException
    assertThatThrownBy(() -> cluster.startServerVM(1, serverProps, locatorPort))
        .hasRootCauseInstanceOf(SecurityException.class)
        .hasStackTraceContaining("not authorized for CLUSTER:MANAGE");
  }

  /**
   * Test that peer with no credentials cannot join P2P cluster.
   *
   * <p>
   * Verifies:
   * <ul>
   * <li>TLS handshake succeeds (encryption established)</li>
   * <li>Application-layer authentication fails (no credentials provided)</li>
   * <li>Peer is rejected</li>
   * </ul>
   */
  @Test
  public void testP2PPeerRejectedWithNoCredentials() throws Exception {
    CertStores clusterStores = fixture.createClusterStores();

    // Configure and start locator
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Try to start server WITHOUT any authentication credentials
    Properties serverProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(serverProps);
    // NO peer auth properties added - missing credentials

    // Server should fail to join due to missing credentials
    assertThatThrownBy(() -> cluster.startServerVM(1, serverProps, locatorPort))
        .hasRootCauseInstanceOf(GemFireSecurityException.class);
  }

  /**
   * Test multiple peers joining with different valid credentials.
   *
   * <p>
   * Verifies that the cluster supports heterogeneous peer credentials as long as all have
   * CLUSTER:MANAGE permission. This demonstrates flexibility in credential management where
   * different services/teams can use different credentials.
   */
  @Test
  public void testMultiplePeersWithDifferentCredentials() throws Exception {
    CertStores clusterStores = fixture.createClusterStores();

    // Start locator with "cluster" credentials
    Properties locatorProps = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(locatorProps);
    fixture.addPeerAuthProperties(locatorProps, "cluster", "cluster");
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    int locatorPort = locator.getPort();

    // Start server1 with "clusterManage" credentials
    // SimpleSecurityManager grants CLUSTER:MANAGE to "cluster" and "clusterManage" users
    Properties server1Props = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(server1Props);
    fixture.addPeerAuthProperties(server1Props, "clusterManage", "clusterManage");
    MemberVM server1 = cluster.startServerVM(1, server1Props, locatorPort);

    // Start server2 with "cluster" credentials (same as locator)
    Properties server2Props = clusterStores.propertiesWith("all", false, false);
    fixture.addSecurityManagerConfig(server2Props);
    fixture.addPeerAuthProperties(server2Props, "cluster", "cluster");
    MemberVM server2 = cluster.startServerVM(2, server2Props, locatorPort);

    // Verify all peers joined successfully
    server1.invoke(() -> {
      assertThat(ClusterStartupRule.getCache().getDistributedSystem().getAllOtherMembers())
          .hasSize(2); // locator + server2
    });

    server2.invoke(() -> {
      assertThat(ClusterStartupRule.getCache().getDistributedSystem().getAllOtherMembers())
          .hasSize(2); // locator + server1
    });
  }
}
