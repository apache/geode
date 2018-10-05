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
import org.apache.geode.cache.ssl.TestSSLUtils.CertificateBuilder;
import org.apache.geode.distributed.internal.tcpserver.LocatorCancelException;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ClientServerHostNameVerificationDistributedTest {
  private static MemberVM locator;
  private static MemberVM locator2;
  private static MemberVM server;
  private static MemberVM server2;
  private static ClientVM client;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

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
    CertificateBuilder locatorCertificate = new CertificateBuilder()
        .commonName("locator")
        // ClusterStartupRule uses 'localhost' as locator host
        .sanDnsName(InetAddress.getLoopbackAddress().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
        .sanIpAddress(InetAddress.getLocalHost())
        .sanIpAddress(InetAddress.getByName("0.0.0.0")); // to pass on windows

    CertificateBuilder serverCertificate = new CertificateBuilder()
        .commonName("server")
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
        .sanIpAddress(InetAddress.getLocalHost());

    CertificateBuilder clientCertificate = new CertificateBuilder()
        .commonName("client");

    validateClientConnection(locatorCertificate, serverCertificate, clientCertificate, true, true,
        true,
        null);
  }

  @Test
  public void expectConnectionFailureWhenNoHostNameInLocatorKey() throws Exception {
    CertificateBuilder locatorCertificate = new CertificateBuilder()
        .commonName("locator");

    CertificateBuilder serverCertificate = new CertificateBuilder()
        .commonName("server");

    CertificateBuilder clientCertificate = new CertificateBuilder()
        .commonName("client");

    validateClientConnection(locatorCertificate, serverCertificate, clientCertificate, false, false,
        true,
        LocatorCancelException.class);
  }

  @Test
  public void expectConnectionFailureWhenWrongHostNameInLocatorKey() throws Exception {
    CertificateBuilder locatorCertificate = new CertificateBuilder()
        .commonName("locator")
        .sanDnsName("example.com");;

    CertificateBuilder serverCertificate = new CertificateBuilder()
        .commonName("server")
        .sanDnsName("example.com");;

    CertificateBuilder clientCertificate = new CertificateBuilder()
        .commonName("client");

    validateClientConnection(locatorCertificate, serverCertificate, clientCertificate, false, false,
        true,
        LocatorCancelException.class);
  }

  @Test
  public void expectConnectionFailureWhenNoHostNameInServerKey() throws Exception {
    CertificateBuilder locatorCertificateWithSan = new CertificateBuilder()
        .commonName("locator")
        .sanDnsName(InetAddress.getLoopbackAddress().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
        .sanIpAddress(InetAddress.getLocalHost());

    CertificateBuilder serverCertificateWithNoSan = new CertificateBuilder()
        .commonName("server");

    CertificateBuilder clientCertificate = new CertificateBuilder()
        .commonName("client");

    validateClientConnection(locatorCertificateWithSan, serverCertificateWithNoSan,
        clientCertificate, false, false, true,
        NoAvailableServersException.class);
  }

  private void validateClientConnection(CertificateBuilder locatorCertificate,
      CertificateBuilder serverCertificate, CertificateBuilder clientCertificate,
      boolean enableHostNameVerficiationForLocator, boolean enableHostNameVerificationForServer,
      boolean enableHostNameVerificationForClient, Class expectedExceptionOnClient)
      throws GeneralSecurityException, IOException {
    CertStores locatorStore = CertStores.locatorStore();
    locatorStore.withCertificate(locatorCertificate);

    CertStores serverStore = CertStores.serverStore();
    serverStore.withCertificate(serverCertificate);

    CertStores clientStore = CertStores.clientStore();
    clientStore.withCertificate(clientCertificate);

    Properties locatorSSLProps = locatorStore
        .trustSelf()
        .trust(clientStore.alias(), clientStore.certificate())
        .trust(serverStore.alias(), serverStore.certificate())
        .propertiesWith(ALL, true, enableHostNameVerficiationForLocator);

    Properties serverSSLProps = serverStore
        .trustSelf()
        .trust(locatorStore.alias(), locatorStore.certificate())
        .trust(clientStore.alias(), clientStore.certificate())
        .propertiesWith(ALL, true, enableHostNameVerificationForServer);

    Properties clientSSLProps = clientStore
        .trust(locatorStore.alias(), locatorStore.certificate())
        .trust(serverStore.alias(), serverStore.certificate())
        .propertiesWith(ALL, true, enableHostNameVerificationForClient);

    // create a cluster
    locator = cluster.startLocatorVM(0, locatorSSLProps);
    locator2 = cluster.startLocatorVM(1, locatorSSLProps, locator.getPort());
    server = cluster.startServerVM(2, serverSSLProps, locator.getPort());
    server2 = cluster.startServerVM(3, serverSSLProps, locator.getPort());

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

    ClientCache clientCache = clientCacheFactory.create();

    ClientRegionFactory<String, String> regionFactory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);

    if (expectedExceptionOnClient != null) {
      IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
      IgnoredException.addIgnoredException("java.net.SocketException");

      Region<String, String> clientRegion = regionFactory.create("region");
      assertThatExceptionOfType(expectedExceptionOnClient)
          .isThrownBy(() -> clientRegion.put("1", "1"));

      // Close the client cache so the pool does not retry, CSRule tearDown
      // closes cache which eventually close pool. But pool can keep
      // retrying and fill logs between closing suspect log buffer
      // and closing cache
      clientCache.close();
      IgnoredException.removeAllExpectedExceptions();
    } else {
      // test client can read and write to server
      Region<String, String> clientRegion = regionFactory.create("region");
      assertThat("servervalue").isEqualTo(clientRegion.get("serverkey"));
      clientRegion.put("clientkey", "clientvalue");

      // test server can see data written by client
      server.invoke(ClientServerHostNameVerificationDistributedTest::doServerRegionTest);
    }
    SocketCreatorFactory.close();
  }
}
