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
package org.apache.geode.cache.client.internal;

import static org.apache.geode.security.SecurableCommunicationChannels.ALL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.NoAvailableServersException;
import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ClientServerHostNameVerificationDistributedTest {
  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private CertificateMaterial ca;

  @Before
  public void setup() {
    ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();
  }

  private static void createServerRegion() {
    RegionFactory factory =
        ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
    Region r = factory.create("region");
    r.put("serverkey", "servervalue");
  }

  private static void doServerRegionTest() {
    Region<String, String> region = ClusterStartupRule.getCache().getRegion("region");
    assertThat("servervalue").isEqualTo(region.get("serverkey"));
    assertThat("clientvalue").isEqualTo(region.get("clientkey"));
  }

  @Test
  public void connectionSuccessfulWhenHostNameOfLocatorAndServer() throws Exception {
    CertificateMaterial locatorCertificate = new CertificateBuilder()
        .commonName("locator")
        .issuedBy(ca)
        // ClusterStartupRule uses 'localhost' as locator host
        .sanDnsName(InetAddress.getLoopbackAddress().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
        .sanIpAddress(LocalHostUtil.getLocalHost())
        .sanIpAddress(InetAddress.getLocalHost())
        .sanIpAddress(InetAddress.getByName("0.0.0.0")) // to pass on windows
        .generate();

    CertificateMaterial serverCertificate = new CertificateBuilder()
        .commonName("server")
        .issuedBy(ca)
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
        .sanIpAddress(LocalHostUtil.getLocalHost())
        .sanIpAddress(InetAddress.getLocalHost())
        .generate();

    CertificateMaterial clientCertificate = new CertificateBuilder()
        .commonName("client")
        .issuedBy(ca)
        .generate();

    validateClientConnection(locatorCertificate, serverCertificate, clientCertificate, true, true,
        true,
        null);
  }

  @Test
  public void expectConnectionFailureWhenNoHostNameInLocatorKey() throws Exception {

    IgnoredException.addIgnoredException(IllegalStateException.class);
    CertificateMaterial locatorCertificate = new CertificateBuilder()
        .commonName("locator")
        .issuedBy(ca)
        .generate();

    CertificateMaterial serverCertificate = new CertificateBuilder()
        .commonName("server")
        .issuedBy(ca)
        .generate();

    CertificateMaterial clientCertificate = new CertificateBuilder()
        .commonName("client")
        .issuedBy(ca)
        .generate();

    validateClientConnection(locatorCertificate, serverCertificate, clientCertificate, false, false,
        true,
        IllegalStateException.class);
  }

  @Test
  public void expectConnectionFailureWhenWrongHostNameInLocatorKey() throws Exception {
    IgnoredException.addIgnoredException(IllegalStateException.class);

    CertificateMaterial locatorCertificate = new CertificateBuilder()
        .commonName("locator")
        .sanDnsName("example.com")
        .issuedBy(ca)
        .generate();

    CertificateMaterial serverCertificate = new CertificateBuilder()
        .commonName("server")
        .sanDnsName("example.com")
        .issuedBy(ca)
        .generate();

    CertificateMaterial clientCertificate = new CertificateBuilder()
        .commonName("client")
        .issuedBy(ca)
        .generate();

    validateClientConnection(locatorCertificate, serverCertificate, clientCertificate, false, false,
        true,
        IllegalStateException.class);
  }

  @Test
  public void expectConnectionFailureWhenNoHostNameInServerKey() throws Exception {
    CertificateMaterial locatorCertificateWithSan = new CertificateBuilder()
        .commonName("locator")
        .issuedBy(ca)
        .sanDnsName(InetAddress.getLoopbackAddress().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
        .sanIpAddress(LocalHostUtil.getLocalHost())
        .sanIpAddress(InetAddress.getLocalHost())
        .generate();

    CertificateMaterial serverCertificateWithNoSan = new CertificateBuilder()
        .commonName("server")
        .issuedBy(ca)
        .generate();

    CertificateMaterial clientCertificate = new CertificateBuilder()
        .commonName("client")
        .issuedBy(ca)
        .generate();

    validateClientConnection(locatorCertificateWithSan, serverCertificateWithNoSan,
        clientCertificate, false, false, true,
        NoAvailableServersException.class);
  }

  private void validateClientConnection(CertificateMaterial locatorCertificate,
      CertificateMaterial serverCertificate, CertificateMaterial clientCertificate,
      boolean enableHostNameVerficiationForLocator, boolean enableHostNameVerificationForServer,
      boolean enableHostNameVerificationForClient,
      Class<? extends Throwable> expectedExceptionOnClient)
      throws GeneralSecurityException, IOException {
    CertStores locatorStore = CertStores.locatorStore();
    locatorStore.withCertificate("locator", locatorCertificate);
    locatorStore.trust("ca", ca);

    CertStores serverStore = CertStores.serverStore();
    serverStore.withCertificate("server", serverCertificate);
    serverStore.trust("ca", ca);

    CertStores clientStore = CertStores.clientStore();
    clientStore.withCertificate("client", clientCertificate);
    clientStore.trust("ca", ca);

    Properties locatorSSLProps = locatorStore
        .propertiesWith(ALL, true, enableHostNameVerficiationForLocator);

    Properties serverSSLProps = serverStore
        .propertiesWith(ALL, true, enableHostNameVerificationForServer);

    Properties clientSSLProps = clientStore
        .propertiesWith(ALL, true, enableHostNameVerificationForClient);

    // create a cluster
    MemberVM locator = cluster.startLocatorVM(0, locatorSSLProps);
    MemberVM locator2 = cluster.startLocatorVM(1, locatorSSLProps, locator.getPort());
    MemberVM server = cluster.startServerVM(2, serverSSLProps, locator.getPort());
    MemberVM server2 = cluster.startServerVM(3, serverSSLProps, locator.getPort());

    // create region
    server.invoke(ClientServerHostNameVerificationDistributedTest::createServerRegion);
    server2.invoke(ClientServerHostNameVerificationDistributedTest::createServerRegion);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/region", 2);

    // create client and connect
    final int locatorPort = locator.getPort();
    final String locatorHost = locator.getVM().getHost().getHostName();

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory(clientSSLProps);
    clientCacheFactory.setPoolSubscriptionEnabled(true)
        .addPoolLocator(locatorHost, locatorPort);

    // close clientCache when done to stop retries between closing suspect log buffer and closing
    // cache
    try (ClientCache clientCache = clientCacheFactory.create()) {

      ClientRegionFactory<String, String> regionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);

      IgnoredException.addIgnoredException("Connection reset");
      IgnoredException.addIgnoredException("java.io.IOException");
      if (expectedExceptionOnClient != null) {
        IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
        IgnoredException.addIgnoredException("java.net.SocketException");
        IgnoredException.addIgnoredException("java.security.cert.CertificateException");
        IgnoredException.addIgnoredException("java.net.ssl.SSLProtocolException");

        Region<String, String> clientRegion = regionFactory.create("region");
        assertThatExceptionOfType(expectedExceptionOnClient)
            .isThrownBy(() -> clientRegion.put("1", "1"));
      } else {
        // test client can read and write to server
        Region<String, String> clientRegion = regionFactory.create("region");
        assertThat("servervalue").isEqualTo(clientRegion.get("serverkey"));
        clientRegion.put("clientkey", "clientvalue");

        // test server can see data written by client
        server.invoke(ClientServerHostNameVerificationDistributedTest::doServerRegionTest);
      }
    } finally {
      SocketCreatorFactory.close();
    }
  }
}
