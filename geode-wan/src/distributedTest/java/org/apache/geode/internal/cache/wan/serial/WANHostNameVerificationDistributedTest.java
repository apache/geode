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
package org.apache.geode.internal.cache.wan.serial;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.security.SecurableCommunicationChannels.ALL;
import static org.apache.geode.security.SecurableCommunicationChannels.GATEWAY;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.rules.ClusterStartupRule.getCache;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.wan.AbstractGatewaySender;
import org.apache.geode.internal.cache.wan.GatewaySenderEventRemoteDispatcher;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class WANHostNameVerificationDistributedTest {

  private static MemberVM locator_ln;
  private static MemberVM server_ln;

  private static MemberVM locator_ny;
  private static MemberVM server_ny;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void setupCluster() throws Exception {
    IgnoredException.addIgnoredException("Connection reset");
    IgnoredException.addIgnoredException("Broken pipe");
    IgnoredException.addIgnoredException("Connection refused");
    IgnoredException.addIgnoredException("could not get remote locator information");
    IgnoredException.addIgnoredException("Unexpected IOException");
  }

  private void setupWanSites(CertificateMaterial ca, CertificateMaterial locator_ln_cert,
      CertificateMaterial server_ln_cert,
      CertificateMaterial locator_ny_cert, CertificateMaterial server_ny_cert)
      throws GeneralSecurityException, IOException {
    CertStores locator_ln_store = new CertStores("ln_locator");
    locator_ln_store.withCertificate("ln_locator", locator_ln_cert);
    locator_ln_store.trust("ca", ca);

    CertStores server_ln_store = new CertStores("ln_server");
    server_ln_store.withCertificate("ln_server", server_ln_cert);
    server_ln_store.trust("ca", ca);

    CertStores locator_ny_store = new CertStores("ny_locator");
    locator_ny_store.withCertificate("ny_locator", locator_ny_cert);
    locator_ny_store.trust("ca", ca);

    CertStores server_ny_store = new CertStores("ny_server");
    server_ny_store.withCertificate("ny_server", server_ny_cert);
    server_ny_store.trust("ca", ca);

    int site1Port =
        setupWanSite1(locator_ln_store, server_ln_store);
    setupWanSite2(site1Port, locator_ny_store, server_ny_store);
  }

  private int setupWanSite1(CertStores locator_ln_store, CertStores server_ln_store)
      throws GeneralSecurityException, IOException {

    Properties locatorSSLProps = locator_ln_store
        .propertiesWith(ALL, true, true);

    Properties serverSSLProps = server_ln_store
        .propertiesWith(ALL, true, true);

    // create a cluster
    locatorSSLProps.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    locator_ln = cluster.startLocatorVM(0, locatorSSLProps);
    server_ln = cluster.startServerVM(1, serverSSLProps, locator_ln.getPort());

    // create a region
    server_ln.invoke(WANHostNameVerificationDistributedTest::createServerRegion);
    locator_ln.waitUntilRegionIsReadyOnExactlyThisManyServers("/region", 1);

    // create gateway sender
    server_ln.invoke(() -> {
      GeodeAwaitility.await().until(() -> {
        WANHostNameVerificationDistributedTest.createGatewaySender();
        return true;
      });
    });

    return locator_ln.getPort();
  }

  private void setupWanSite2(int site1Port, CertStores locator_ny_store,
      CertStores server_ny_store)
      throws GeneralSecurityException, IOException {

    Properties locator_ny_props = locator_ny_store
        .propertiesWith(ALL, true, true);

    locator_ny_props.setProperty(MCAST_PORT, "0");
    locator_ny_props.setProperty(DISTRIBUTED_SYSTEM_ID, "2");
    locator_ny_props.setProperty(REMOTE_LOCATORS, "localhost[" + site1Port + "]");

    Properties server_ny_props = server_ny_store
        .propertiesWith(ALL, true, true);

    // create a cluster
    locator_ny_props.setProperty(DISTRIBUTED_SYSTEM_ID, "2");
    locator_ny = cluster.startLocatorVM(2, locator_ny_props);
    server_ny = cluster.startServerVM(3, server_ny_props, locator_ny.getPort());

    // create a region
    server_ny.invoke(WANHostNameVerificationDistributedTest::createServerRegion);
    locator_ny.waitUntilRegionIsReadyOnExactlyThisManyServers("/region", 1);

    // create gateway sender
    server_ny.invoke(() -> {
      GeodeAwaitility.await().until(() -> {
        WANHostNameVerificationDistributedTest.createGatewayReceiver();
        return true;
      });
    });
  }

  private static void createGatewayReceiver() {
    int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    GatewayReceiverFactory gwReceiver = getCache().createGatewayReceiverFactory();
    gwReceiver.setStartPort(port);
    gwReceiver.setEndPort(port);
    gwReceiver.create();
  }

  private static void createGatewaySender() {
    GatewaySenderFactory gwSender = getCache().createGatewaySenderFactory();
    gwSender.setBatchSize(1);
    gwSender.create("ln", 2);
  }

  private static void createServerRegion() {
    RegionFactory factory =
        ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
    factory.addGatewaySenderId("ln");
    factory.create("region");
  }

  private static void doPutOnSite1() {
    Region<String, String> region = ClusterStartupRule.getCache().getRegion("region");
    region.put("serverkey", "servervalue");
    assertThat("servervalue").isEqualTo(region.get("serverkey"));
  }

  private static void verifySite2Received() {
    Region<String, String> region = ClusterStartupRule.getCache().getRegion("region");
    await()
        .untilAsserted(() -> assertThat("servervalue").isEqualTo(region.get("serverkey")));
  }

  private static void verifySite2DidNotReceived() {
    Region<String, String> region = ClusterStartupRule.getCache().getRegion("region");
    await()
        .untilAsserted(() -> assertThat(region.keySet().size()).isEqualTo(0));
  }

  @Test
  public void enableHostNameValidationAcrossAllComponents() throws Exception {
    // this test enables hostname validation across all components in both sites
    // to test tcp clients validating hostname in servers identity across
    // locator-ln -> locator-ny
    // server-ln -> locator-ln
    // server-ln -> locator-ny
    // server-ln -> server-ny

    CertificateMaterial ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();

    CertificateMaterial locator_ln_cert = new CertificateBuilder()
        .commonName("locator_ln")
        .issuedBy(ca)
        // ClusterStartupRule uses 'localhost' as locator host
        .sanDnsName(InetAddress.getLoopbackAddress().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getHostAddress())
        .sanDnsName(LocalHostUtil.getLocalHost().getCanonicalHostName())
        .sanIpAddress(InetAddress.getLocalHost())
        .sanIpAddress(InetAddress.getByName("0.0.0.0")) // to pass on windows
        .sanIpAddress(LocalHostUtil.getLocalHost())
        .generate();

    CertificateMaterial server_ln_cert = new CertificateBuilder()
        .commonName("server_ln")
        .issuedBy(ca)
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanDnsName(LocalHostUtil.getLocalHostName())
        .sanDnsName(LocalHostUtil.getLocalHost().getCanonicalHostName())
        .sanDnsName(InetAddress.getLocalHost().getHostAddress())
        .sanIpAddress(InetAddress.getLocalHost())
        .sanIpAddress(LocalHostUtil.getLocalHost())
        .generate();

    CertificateMaterial locator_ny_cert = new CertificateBuilder()
        .commonName("locator_ny")
        .issuedBy(ca)
        // ClusterStartupRule uses 'localhost' as locator host
        .sanDnsName(InetAddress.getLoopbackAddress().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
        .sanDnsName(LocalHostUtil.getLocalHost().getCanonicalHostName())
        .sanDnsName(LocalHostUtil.getLocalHostName())
        .sanDnsName(InetAddress.getLocalHost().getHostAddress())
        .sanIpAddress(InetAddress.getLocalHost())
        .sanIpAddress(InetAddress.getByName("0.0.0.0")) // to pass on windows
        .sanIpAddress(LocalHostUtil.getLocalHost())
        .generate();

    CertificateMaterial server_ny_cert = new CertificateBuilder()
        .commonName("server_ny")
        .issuedBy(ca)
        .sanDnsName(InetAddress.getLocalHost().getHostName())
        .sanDnsName(InetAddress.getLocalHost().getCanonicalHostName())
        .sanDnsName(LocalHostUtil.getLocalHostName())
        .sanDnsName(LocalHostUtil.getLocalHost().getCanonicalHostName())
        .sanDnsName(InetAddress.getLocalHost().getHostAddress())
        .sanIpAddress(InetAddress.getLocalHost())
        .sanIpAddress(LocalHostUtil.getLocalHost())
        .generate();

    setupWanSites(ca, locator_ln_cert, server_ln_cert, locator_ny_cert, server_ny_cert);

    server_ln.invoke(WANHostNameVerificationDistributedTest::doPutOnSite1);
    server_ny.invoke(WANHostNameVerificationDistributedTest::verifySite2Received);
  }

  @Test
  public void gwsenderFailsToConnectIfGWReceiverHasNoHostName() throws Exception {
    CertificateMaterial ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();

    CertificateMaterial gwSenderCertificate = new CertificateBuilder()
        .commonName("gwsender-ln")
        .issuedBy(ca)
        .generate();

    CertificateMaterial gwReceiverCertificate = new CertificateBuilder()
        .commonName("gwreceiver-ny")
        .issuedBy(ca)
        .generate();

    CertStores gwSender = new CertStores("gwsender");
    gwSender.withCertificate("gwsender", gwSenderCertificate);
    gwSender.trust("ca", ca);

    CertStores gwReceiver = new CertStores("gwreceiver");
    gwReceiver.withCertificate("gwreceiver", gwReceiverCertificate);
    gwReceiver.trust("ca", ca);

    Properties ln_SSLProps = gwSender
        .propertiesWith(GATEWAY, true, true);

    Properties ny_SSLProps = gwReceiver
        .propertiesWith(GATEWAY, true, true);

    // create a ln cluster
    ln_SSLProps.setProperty(DISTRIBUTED_SYSTEM_ID, "1");
    locator_ln = cluster.startLocatorVM(0, ln_SSLProps);
    server_ln = cluster.startServerVM(1, ln_SSLProps, locator_ln.getPort());

    // create a region in ln
    server_ln.invoke(WANHostNameVerificationDistributedTest::createServerRegion);
    locator_ln.waitUntilRegionIsReadyOnExactlyThisManyServers("/region", 1);

    // create a gateway sender ln
    server_ln.invoke(WANHostNameVerificationDistributedTest::createGatewaySender);

    // create a ny cluster
    ny_SSLProps.setProperty(MCAST_PORT, "0");
    ny_SSLProps.setProperty(DISTRIBUTED_SYSTEM_ID, "2");
    ny_SSLProps.setProperty(REMOTE_LOCATORS, "localhost[" + locator_ln.getPort() + "]");
    locator_ny = cluster.startLocatorVM(2, ny_SSLProps);
    server_ny = cluster.startServerVM(3, ny_SSLProps, locator_ny.getPort());

    // create a region in ny
    server_ny.invoke(WANHostNameVerificationDistributedTest::createServerRegion);
    locator_ny.waitUntilRegionIsReadyOnExactlyThisManyServers("/region", 1);

    // create a gateway sender in ny
    server_ny.invoke(WANHostNameVerificationDistributedTest::createGatewayReceiver);

    IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
    server_ln.invoke(WANHostNameVerificationDistributedTest::doPutOnSite1);
    server_ny.invoke(WANHostNameVerificationDistributedTest::verifySite2DidNotReceived);

    server_ln.invoke(() -> {
      AbstractGatewaySender gatewaySender =
          (AbstractGatewaySender) getCache().getGatewaySender("ln");
      GatewaySenderEventRemoteDispatcher dispatcher =
          (GatewaySenderEventRemoteDispatcher) gatewaySender.getEventProcessor().getDispatcher();
      assertThat(dispatcher.isConnectedToRemote()).isFalse();

      // Close the processor so its does not retry, CSRule tearDown
      // closes cache which eventually close processor. But processor
      // can keep retrying between closing suspect log buffer and cache
      // can continue to log connection exceptions which will pollute
      // next test
      gatewaySender.getEventProcessor().closeProcessor();
    });
    IgnoredException.removeAllExpectedExceptions();
  }
}
