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

import java.net.InetAddress;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.TestSSLUtils.CertificateBuilder;
import org.apache.geode.test.dunit.SerializableConsumerIF;
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

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @BeforeClass
  public static void setupCluster() throws Exception {
    CertificateBuilder locatorCertificate = new CertificateBuilder()
        .commonName("locator")
        // ClusterStartupRule uses 'localhost' as locator host
        .sanDnsName(InetAddress.getLoopbackAddress().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanIpAddress(InetAddress.getLocalHost());

    CertificateBuilder serverCertificate = new CertificateBuilder()
        .commonName("server")
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanIpAddress(InetAddress.getLocalHost());

    CertificateBuilder clientCertificate = new CertificateBuilder()
        .commonName("client");

    CertStores locatorStore = CertStores.locatorStore();
    locatorStore.withCertificate(locatorCertificate);

    CertStores serverStore = CertStores.serverStore();
    serverStore.withCertificate(serverCertificate);

    CertStores clientStore = CertStores.clientStore();
    clientStore.withCertificate(clientCertificate);

    Properties locatorSSLProps = locatorStore
        .trustSelf()
        .trust(serverStore.alias(), serverStore.certificate())
        .trust(clientStore.alias(), clientStore.certificate())
        .propertiesWith(ALL);

    Properties serverSSLProps = serverStore
        .trustSelf()
        .trust(locatorStore.alias(), locatorStore.certificate())
        .trust(clientStore.alias(), clientStore.certificate())
        .propertiesWith(ALL);

    Properties clientSSLProps = clientStore
        .trust(locatorStore.alias(), locatorStore.certificate())
        .trust(serverStore.alias(), serverStore.certificate())
        .propertiesWith(ALL);

    // create a cluster
    locator = cluster.startLocatorVM(0, locatorSSLProps);
    locator2 = cluster.startLocatorVM(1, locatorSSLProps, locator.getPort());
    server = cluster.startServerVM(2, serverSSLProps, locator.getPort());
    server2 = cluster.startServerVM(3, serverSSLProps, locator.getPort());

    // create region
    server.invoke(ClientServerHostNameVerificationDistributedTest::createServerRegion);
    server2.invoke(ClientServerHostNameVerificationDistributedTest::createServerRegion);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/region", 2);

    // setup client
    setupClient(clientSSLProps, locator.getPort(), locator.getVM().getHost().getHostName());
  }

  private static void createServerRegion() {
    RegionFactory factory =
        ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
    Region r = factory.create("region");
    r.put("serverkey", "servervalue");
  }

  private static void setupClient(Properties clientSSLProps, int locatorPort,
      String locatorHost) throws Exception {
    SerializableConsumerIF<ClientCacheFactory> clientSetup =
        cf -> cf.addPoolLocator(locatorHost, locatorPort);
    client = cluster.startClientVM(4, clientSSLProps, clientSetup);

    // create a client region
    client.invoke(ClientServerHostNameVerificationDistributedTest::createClientRegion);
  }

  private static void createClientRegion() {
    ClientRegionFactory<String, String> regionFactory =
        ClusterStartupRule.getClientCache().createClientRegionFactory(ClientRegionShortcut.PROXY);
    Region<String, String> region = regionFactory.create("region");
    assertThat(region).isNotNull();
  }

  @Test
  public void clientValidatesHostNameOfLocatorAndServerDuringSSLConnection() {
    client.invoke(ClientServerHostNameVerificationDistributedTest::doClientRegionTest);
    server.invoke(ClientServerHostNameVerificationDistributedTest::doServerRegionTest);
  }

  private static void doClientRegionTest() {
    Region<String, String> region = ClusterStartupRule.getClientCache().getRegion("region");
    assertThat("servervalue").isEqualTo(region.get("serverkey"));

    region.put("clientkey", "clientvalue");
    assertThat("clientvalue").isEqualTo(region.get("clientkey"));
  }

  private static void doServerRegionTest() {
    Region<String, String> region = ClusterStartupRule.getCache().getRegion("region");
    assertThat("servervalue").isEqualTo(region.get("serverkey"));
    assertThat("clientvalue").isEqualTo(region.get("clientkey"));
  }
}
