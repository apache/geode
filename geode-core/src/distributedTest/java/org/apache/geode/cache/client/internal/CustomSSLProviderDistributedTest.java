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

import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_USE_DEFAULT_CONTEXT;
import static org.apache.geode.security.SecurableCommunicationChannels.ALL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.io.IOException;
import java.net.InetAddress;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Properties;

import javax.net.ssl.SSLContext;

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
import org.apache.geode.cache.client.internal.provider.CustomKeyManagerFactory;
import org.apache.geode.cache.client.internal.provider.CustomTrustManagerFactory;
import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.TestSSLUtils.CertificateBuilder;
import org.apache.geode.distributed.internal.tcpserver.LocatorCancelException;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class CustomSSLProviderDistributedTest {
  private static MemberVM locator;
  private static MemberVM server;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  private CustomKeyManagerFactory.PKIXFactory keyManagerFactory;
  private CustomTrustManagerFactory.PKIXFactory trustManagerFactory;

  private void setupCluster(Properties locatorSSLProps, Properties serverSSLProps) {
    // create a cluster
    locator = cluster.startLocatorVM(0, locatorSSLProps);
    server = cluster.startServerVM(1, serverSSLProps, locator.getPort());

    // create region
    server.invoke(CustomSSLProviderDistributedTest::createServerRegion);
    locator.waitUntilRegionIsReadyOnExactlyThisManyServers("/region", 1);
  }

  private static void createServerRegion() {
    RegionFactory factory =
        ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.REPLICATE);
    Region r = factory.create("region");
    r.put("serverkey", "servervalue");
  }

  @Test
  public void hostNameIsValidatedWhenUsingDefaultContext() throws Exception {
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

    validateClientSSLConnection(locatorCertificate, serverCertificate, clientCertificate, true,
        true, false, null);
  }

  @Test
  public void clientCanChooseNotToValidateHostName() throws Exception {
    CertificateBuilder locatorCertificate = new CertificateBuilder()
        .commonName("locator");

    CertificateBuilder serverCertificate = new CertificateBuilder()
        .commonName("server");

    CertificateBuilder clientCertificate = new CertificateBuilder()
        .commonName("client");

    validateClientSSLConnection(locatorCertificate, serverCertificate, clientCertificate, false,
        false, true, null);
  }

  @Test
  public void clientConnectionFailsIfNoHostNameInLocatorKey() throws Exception {
    CertificateBuilder locatorCertificate = new CertificateBuilder()
        .commonName("locator");

    CertificateBuilder serverCertificate = new CertificateBuilder()
        .commonName("server");

    CertificateBuilder clientCertificate = new CertificateBuilder()
        .commonName("client");

    validateClientSSLConnection(locatorCertificate, serverCertificate, clientCertificate, false,
        false, false, LocatorCancelException.class);
  }

  @Test
  public void clientConnectionFailsWhenWrongHostNameInLocatorKey() throws Exception {
    CertificateBuilder locatorCertificate = new CertificateBuilder()
        .commonName("locator")
        .sanDnsName("example.com");;

    CertificateBuilder serverCertificate = new CertificateBuilder()
        .commonName("server")
        .sanDnsName("example.com");;

    CertificateBuilder clientCertificate = new CertificateBuilder()
        .commonName("client");

    validateClientSSLConnection(locatorCertificate, serverCertificate, clientCertificate, false,
        false,
        false,
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

    validateClientSSLConnection(locatorCertificateWithSan, serverCertificateWithNoSan,
        clientCertificate, false, false, false,
        NoAvailableServersException.class);
  }

  private void validateClientSSLConnection(CertificateBuilder locatorCertificate,
      CertificateBuilder serverCertificate, CertificateBuilder clientCertificate,
      boolean enableHostNameVerficationForLocator, boolean enableHostNameVerificationForServer,
      boolean disableHostNameVerificationForClient,
      Class expectedExceptionOnClient)
      throws GeneralSecurityException, IOException {

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
        .propertiesWith(ALL, false, enableHostNameVerficationForLocator);

    Properties serverSSLProps = serverStore
        .trustSelf()
        .trust(locatorStore.alias(), locatorStore.certificate())
        .trust(clientStore.alias(), clientStore.certificate())
        .propertiesWith(ALL, true, enableHostNameVerificationForServer);

    // this props is only to create temp keystore and truststore and get paths
    Properties clientSSLProps = clientStore
        .trust(locatorStore.alias(), locatorStore.certificate())
        .trust(serverStore.alias(), serverStore.certificate())
        .propertiesWith(ALL, true, true);

    setupCluster(locatorSSLProps, serverSSLProps);

    // setup client
    keyManagerFactory =
        new CustomKeyManagerFactory.PKIXFactory(clientSSLProps.getProperty(SSL_KEYSTORE));
    keyManagerFactory.engineInit(null, null);

    trustManagerFactory =
        new CustomTrustManagerFactory.PKIXFactory(clientSSLProps.getProperty(SSL_TRUSTSTORE));
    trustManagerFactory.engineInit((KeyStore) null);

    SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
    sslContext.init(keyManagerFactory.engineGetKeyManagers(),
        trustManagerFactory.engineGetTrustManagers(), null);
    // set default context
    SSLContext.setDefault(sslContext);

    Properties clientSSLProperties = new Properties();
    clientSSLProperties.setProperty(SSL_ENABLED_COMPONENTS, ALL);
    clientSSLProperties.setProperty(SSL_REQUIRE_AUTHENTICATION, String.valueOf("true"));
    clientSSLProperties.setProperty(SSL_USE_DEFAULT_CONTEXT, String.valueOf("true"));

    if (disableHostNameVerificationForClient) {
      // client chose to override default
      clientSSLProperties.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, String.valueOf("false"));
    }

    ClientCacheFactory clientCacheFactory = new ClientCacheFactory(clientSSLProperties);
    clientCacheFactory.addPoolLocator(locator.getVM().getHost().getHostName(), locator.getPort());
    ClientCache clientCache = clientCacheFactory.create();

    ClientRegionFactory<String, String> regionFactory =
        clientCache.createClientRegionFactory(ClientRegionShortcut.PROXY);

    if (expectedExceptionOnClient != null) {
      IgnoredException.addIgnoredException("javax.net.ssl.SSLHandshakeException");
      IgnoredException.addIgnoredException("java.net.SocketException");

      Region<String, String> clientRegion = regionFactory.create("region");
      assertThatExceptionOfType(expectedExceptionOnClient)
          .isThrownBy(() -> clientRegion.put("clientkey", "clientvalue"));
    } else {
      // test client can read and write to server
      Region<String, String> clientRegion = regionFactory.create("region");
      assertThat("servervalue").isEqualTo(clientRegion.get("serverkey"));
      clientRegion.put("clientkey", "clientvalue");

      // test server can see data written by client
      server.invoke(CustomSSLProviderDistributedTest::doServerRegionTest);
    }

    SocketCreatorFactory.close();
  }

  private static void doServerRegionTest() {
    Region<String, String> region = ClusterStartupRule.getCache().getRegion("region");
    assertThat("servervalue").isEqualTo(region.get("serverkey"));
    assertThat("clientvalue").isEqualTo(region.get("clientkey"));
  }
}
