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

package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_HOSTNAME_FOR_CLIENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_DEFAULT_ALIAS;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.ssl.CertStores;
import org.apache.geode.cache.ssl.CertificateBuilder;
import org.apache.geode.cache.ssl.CertificateMaterial;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(GfshTest.class)
public class ConnectCommandWithSSLMultiKeyTest {
  private static MemberVM locator;
  private static Properties sslProperties;
  private OutputStream out = null;
  private File sslConfigFile = null;

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @ClassRule
  public static ClusterStartupRule clusterRule = new ClusterStartupRule();

  @BeforeClass
  public static void beforeClass() throws Exception {

    CertificateMaterial ca = new CertificateBuilder()
        .commonName("Test CA")
        .isCA()
        .generate();

    CertificateMaterial memberMaterial = new CertificateBuilder()
        .commonName("member")
        .sanDnsName("localhost")
        .issuedBy(ca)
        .generate();

    CertStores memberStore = new CertStores("member");
    memberStore.withCertificate("member", memberMaterial);
    memberStore.trust("ca", ca);

    final Properties memberProperties = memberStore.propertiesWith("all", true, false);

    locator = clusterRule.startLocatorVM(0,
        l -> l.withProperties(memberProperties)
            .withProperty(JMX_MANAGER_HOSTNAME_FOR_CLIENTS, "localhost")
            .withHttpService());

    CertificateMaterial clientMaterial = new CertificateBuilder()
        .commonName("client")
        .issuedBy(ca)
        .generate();

    CertificateMaterial selfSigned = new CertificateBuilder()
        .commonName("bad-self-signed")
        .generate();

    CertStores clientStore = new CertStores("client");
    clientStore.withCertificate("bad-self-signed", selfSigned);
    clientStore.withCertificate("client", clientMaterial);
    clientStore.trust("ca", ca);

    sslProperties = clientStore.propertiesWith("all", true, true);
  }

  @Before
  public void before() throws Exception {
    sslConfigFile = temporaryFolder.newFile("ssl.properties");
    out = new FileOutputStream(sslConfigFile);
  }

  private void connectWithSSLSucceeds(int port, GfshCommandRule.PortType locator) throws Exception {
    gfsh.connect(port, locator, "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isTrue();
  }

  private void connectWithSSLFails(int port, GfshCommandRule.PortType locator) throws Exception {
    gfsh.connect(port, locator, "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isFalse();
  }

  private void failToConnectWithoutSSLTo(int port, GfshCommandRule.PortType locator)
      throws Exception {
    gfsh.connect(port, locator);
    assertThat(gfsh.isConnected()).isFalse();
  }

  @Test
  public void connectWithNoSSLShouldFail() throws Exception {
    failToConnectWithoutSSLTo(locator.getPort(), GfshCommandRule.PortType.locator);
    // should fail at connecting to locator stage
    assertThat(gfsh.getGfshOutput()).doesNotContain("Connecting to Manager at");
    assertThat(gfsh.getGfshOutput()).containsPattern(
        "trying to connect a non-SSL-enabled client to an SSL-enabled locator|Broken pipe \\(Write failed\\)|Connection reset");

    failToConnectWithoutSSLTo(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    assertThat(gfsh.getGfshOutput()).contains("non-JRMP server at remote endpoint");

    failToConnectWithoutSSLTo(locator.getHttpPort(), GfshCommandRule.PortType.http);
    assertThat(gfsh.getGfshOutput()).contains("Unexpected end of file from server");
  }

  @Test
  public void connectWithSSLShouldSucceed() throws Exception {
    sslProperties.setProperty(SSL_DEFAULT_ALIAS, "client");
    sslProperties.store(out, null);

    connectWithSSLSucceeds(locator.getPort(), GfshCommandRule.PortType.locator);
    gfsh.disconnect();

    connectWithSSLSucceeds(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfsh.disconnect();
  }

  @Test
  public void connectWithSSLAndBadCertificateShouldFail() throws Exception {
    IgnoredException.addIgnoredException(
        "javax.net.ssl.SSLHandshakeException: Received fatal alert: certificate_unknown");
    sslProperties.setProperty(SSL_DEFAULT_ALIAS, "bad-self-signed");
    sslProperties.store(out, null);

    connectWithSSLFails(locator.getPort(), GfshCommandRule.PortType.locator);

    connectWithSSLFails(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);

    IgnoredException.removeAllExpectedExceptions();
  }

  @Test
  public void connectWithSSLShouldSucceedAndSubsequentConnectWithNoSSLShouldFail()
      throws Exception {
    sslProperties.setProperty(SSL_DEFAULT_ALIAS, "client");
    sslProperties.store(out, null);

    // can connect to both locator and jmx
    connectWithSSLSucceeds(locator.getPort(), GfshCommandRule.PortType.locator);
    gfsh.disconnect();

    // reconnect again with no SSL should fail
    failToConnectWithoutSSLTo(locator.getPort(), GfshCommandRule.PortType.locator);
    // it should fail at connecting to locator, not connecting to manager
    assertThat(gfsh.getGfshOutput()).doesNotContain("Connecting to Manager at");
  }
}
