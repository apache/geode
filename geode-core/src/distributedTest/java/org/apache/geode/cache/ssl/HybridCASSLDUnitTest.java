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

import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.security.SecurableCommunicationChannels.ALL;
import static org.apache.geode.security.SecurableCommunicationChannels.CLUSTER;
import static org.apache.geode.security.SecurableCommunicationChannels.GATEWAY;
import static org.apache.geode.security.SecurableCommunicationChannels.JMX;
import static org.apache.geode.security.SecurableCommunicationChannels.LOCATOR;
import static org.apache.geode.security.SecurableCommunicationChannels.SERVER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.security.AuthenticationFailedException;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Tests hybrid TLS configuration where:
 * - Servers use certificates issued by a public CA (e.g., Let's Encrypt, DigiCert)
 * - Clients use certificates issued by a private/enterprise CA
 * 
 * This configuration mitigates the impact of public CA changes that affect the
 * Client Authentication Extended Key Usage (EKU). See the Apache Geode security
 * documentation for details.
 * 
 * Key requirements validated:
 * - Server certificates must include serverAuth EKU and subjectAltName
 * - Client certificates must include clientAuth EKU
 * - Servers trust the private CA to validate client certificates
 * - Clients trust the public CA to validate server certificates
 */
@Category({ClientServerTest.class})
public class HybridCASSLDUnitTest {
  
  private HybridCATestFixture fixture;
  
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();
  
  @Before
  public void setup() {
    fixture = new HybridCATestFixture();
    fixture.setup();
    
    // Ignore expected exceptions during locator/server shutdown with SSL
    IgnoredException.addIgnoredException("Could not stop Locator");
    IgnoredException.addIgnoredException("ForcedDisconnectException");
  }
  
  /**
   * Tests basic client-server connection with hybrid TLS.
   * Verifies that a client with a private-CA certificate can connect to
   * a server with a public-CA certificate when trust is properly configured.
   * 
   * Note: This test uses SSL only for client-server communication (SERVER component),
   * not for peer-to-peer cluster communication, which simplifies the test setup.
   */
  @Test
  public void testHybridTLSBasicConnection() throws Exception {
    // Start locator without SSL (peer-to-peer communication doesn't require SSL for this test)
    MemberVM locator = cluster.startLocatorVM(0);
    
    // Create server with public-CA certificate, SSL enabled only for SERVER component
    CertStores serverStore = fixture.createServerStores("1");
    Properties serverProps = serverStore.propertiesWith(SERVER, true, true);
    serverProps.setProperty(SSL_ENABLED_COMPONENTS, SERVER);
    
    MemberVM server = cluster.startServerVM(1, serverProps, locator.getPort());
    
    // Create region on server
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });
    
    // Create client with private-CA certificate
    CertStores clientStore = fixture.createClientStores("1");
    Properties clientProps = clientStore.propertiesWith(SERVER, true, true);
    clientProps.setProperty(SSL_ENABLED_COMPONENTS, SERVER);
    
    ClientVM client = cluster.startClientVM(2, clientProps, 
        ccf -> ccf.addPoolLocator("localhost", locator.getPort()));
    
    // Verify client can perform operations
    client.invoke(() -> {
      Region<Object, Object> clientRegion = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create("testRegion");
      
      clientRegion.put("key1", "value1");
      assertThat(clientRegion.get("key1")).isEqualTo("value1");
    });
  }
  
  /**
   * Tests that client authentication is properly enforced in hybrid TLS.
   * A client without a certificate should be rejected.
   */
  @Test
  public void testHybridTLSClientAuthenticationRequired() throws Exception {
    CertStores serverStore = fixture.createServerStores("1");
    Properties serverProps = serverStore.propertiesWith(SERVER, true, true);
    serverProps.setProperty(SSL_ENABLED_COMPONENTS, SERVER);
    
    MemberVM locator = cluster.startLocatorVM(0);
    MemberVM server = cluster.startServerVM(1, serverProps, locator.getPort());
    
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });
    
    // Create client with only truststore (no keystore with client certificate)
    CertStores clientStore = CertStores.clientStore();
    clientStore.trust("publicCA", fixture.getPublicCA());
    Properties clientProps = clientStore.propertiesWith(SERVER, true, true);
    clientProps.setProperty(SSL_ENABLED_COMPONENTS, SERVER);
    
    IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
    IgnoredException.addIgnoredException("java.io.IOException");
    IgnoredException.addIgnoredException("Broken pipe");
    
    // Attempt to create client VM should fail
    assertThatThrownBy(() -> {
      cluster.startClientVM(2, clientProps,
          ccf -> ccf.addPoolLocator("localhost", locator.getPort()));
    }).hasCauseInstanceOf(javax.net.ssl.SSLHandshakeException.class);
    
    IgnoredException.removeAllExpectedExceptions();
  }
  
  /**
   * Tests endpoint identification (hostname verification) with hybrid TLS.
   * Verifies that the server certificate's subjectAltName is validated.
   */
  @Test
  public void testHybridTLSWithEndpointIdentification() throws Exception {
    CertStores serverStore = fixture.createServerStores("1");
    Properties serverProps = serverStore.propertiesWith(SERVER, true, true);
    serverProps.setProperty(SSL_ENABLED_COMPONENTS, SERVER);
    serverProps.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");
    
    MemberVM locator = cluster.startLocatorVM(0, serverProps);
    MemberVM server = cluster.startServerVM(1, serverProps, locator.getPort());
    
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });
    
    CertStores clientStore = fixture.createClientStores("1");
    Properties clientProps = clientStore.propertiesWith(SERVER, true, true);
    clientProps.setProperty(SSL_ENABLED_COMPONENTS, SERVER);
    clientProps.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");
    
    ClientVM client = cluster.startClientVM(2, clientProps,
        ccf -> ccf.addPoolLocator("localhost", locator.getPort()));
    
    client.invoke(() -> {
      Region<Object, Object> clientRegion = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create("testRegion");
      
      clientRegion.put("key1", "value1");
      assertThat(clientRegion.get("key1")).isEqualTo("value1");
    });
  }
  
  /**
   * Tests that peer-to-peer SSL works with hybrid TLS.
   * Multiple servers should be able to communicate using public-CA certificates.
   */
  @Test
  public void testHybridTLSPeerToPeerCommunication() throws Exception {
    CertStores locatorStore = fixture.createLocatorStores("1");
    Properties locatorProps = locatorStore.propertiesWith(CLUSTER, false, true);
    
    MemberVM locator = cluster.startLocatorVM(0, locatorProps);
    
    // Start two servers
    CertStores serverStore1 = fixture.createServerStores("1");
    Properties serverProps1 = serverStore1.propertiesWith(CLUSTER, true, true);
    serverProps1.setProperty(SSL_ENABLED_COMPONENTS, CLUSTER);
    MemberVM server1 = cluster.startServerVM(1, serverProps1, locator.getPort());
    
    CertStores serverStore2 = fixture.createServerStores("2");
    Properties serverProps2 = serverStore2.propertiesWith(CLUSTER, true, true);
   serverProps2.setProperty(SSL_ENABLED_COMPONENTS, CLUSTER);
    MemberVM server2 = cluster.startServerVM(2, serverProps2, locator.getPort());
    
    // Create replicated region on both servers
    server1.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });
    
    server2.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });
    
    // Put data from server1
    server1.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getCache().getRegion("testRegion");
      region.put("key1", "value1");
    });
    
    // Verify data replicated to server2
    server2.invoke(() -> {
      Region<String, String> region = ClusterStartupRule.getCache().getRegion("testRegion");
      assertThat(region.get("key1")).isEqualTo("value1");
    });
  }
  
  /**
   * Tests that clients can connect when only SERVER component has SSL enabled.
   * This validates component-specific SSL configuration.
   */
  @Test
  public void testHybridTLSServerComponentOnly() throws Exception {
    // Server uses hybrid TLS only for SERVER component
    CertStores serverStore = fixture.createServerStores("1");
    Properties serverProps = serverStore.propertiesWith(SERVER, true, true);
    // Locator doesn't need SSL
    serverProps.setProperty(SSL_ENABLED_COMPONENTS, SERVER);
    
    MemberVM locator = cluster.startLocatorVM(0);
    MemberVM server = cluster.startServerVM(1, serverProps, locator.getPort());
    
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });
    
    // Client uses hybrid TLS for SERVER component
    CertStores clientStore = fixture.createClientStores("1");
    Properties clientProps = clientStore.propertiesWith(SERVER, true, true);
    clientProps.setProperty(SSL_ENABLED_COMPONENTS, SERVER);
    
    ClientVM client = cluster.startClientVM(2, clientProps,
        ccf -> ccf.addPoolLocator("localhost", locator.getPort()));
    
    client.invoke(() -> {
      Region<Object, Object> clientRegion = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create("testRegion");
      
      clientRegion.put("key1", "value1");
      assertThat(clientRegion.get("key1")).isEqualTo("value1");
    });
  }
  
  /**
   * Tests multiple clients connecting with different private-CA certificates.
   * Validates that each client can authenticate independently.
   */
  @Test
  public void testHybridTLSMultipleClients() throws Exception {
    CertStores serverStore = fixture.createServerStores("1");
    Properties serverProps = serverStore.propertiesWith(SERVER, true, true);
    serverProps.setProperty(SSL_ENABLED_COMPONENTS, SERVER);
    
    MemberVM locator = cluster.startLocatorVM(0);
    MemberVM server = cluster.startServerVM(1, serverProps, locator.getPort());
    
    server.invoke(() -> {
      ClusterStartupRule.getCache()
          .createRegionFactory(RegionShortcut.REPLICATE)
          .create("testRegion");
    });
    
    // Create first client
    CertStores clientStore1 = fixture.createClientStores("client1");
    Properties clientProps1 = clientStore1.propertiesWith(SERVER, true, true);
    clientProps1.setProperty(SSL_ENABLED_COMPONENTS, SERVER);
    
    ClientVM client1 = cluster.startClientVM(2, clientProps1,
        ccf -> ccf.addPoolLocator("localhost", locator.getPort()));
    
    client1.invoke(() -> {
      Region<Object, Object> clientRegion1 = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create("testRegion");
      
      clientRegion1.put("client1-key", "client1-value");
    });
    
    // Create second client with different certificate
    CertStores clientStore2 = fixture.createClientStores("client2");
    Properties clientProps2 = clientStore2.propertiesWith(SERVER, true, true);
    clientProps2.setProperty(SSL_ENABLED_COMPONENTS, SERVER);
    
    ClientVM client2 = cluster.startClientVM(3, clientProps2,
        ccf -> ccf.addPoolLocator("localhost", locator.getPort()));
    
    client2.invoke(() -> {
      Region<Object, Object> clientRegion2 = ClusterStartupRule.getClientCache()
          .createClientRegionFactory(ClientRegionShortcut.PROXY)
          .create("testRegion");
      
      // Verify second client can see first client's data
      assertThat(clientRegion2.get("client1-key")).isEqualTo("client1-value");
      
      // Verify second client can put its own data
      clientRegion2.put("client2-key", "client2-value");
    });
    
    // Verify first client can see second client's data
    client1.invoke(() -> {
      Region<Object, Object> clientRegion1 = ClusterStartupRule.getClientCache().getRegion("testRegion");
      assertThat(clientRegion1.get("client2-key")).isEqualTo("client2-value");
    });
  }
}
