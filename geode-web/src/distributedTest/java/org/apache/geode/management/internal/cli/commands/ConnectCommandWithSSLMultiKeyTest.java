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

import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_DEFAULT_ALIAS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_TYPE;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Properties;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.security.SecurableCommunicationChannels;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.CleanupDUnitVMsRule;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category(GfshTest.class)
public class ConnectCommandWithSSLMultiKeyTest {

  @ClassRule
  public static CleanupDUnitVMsRule cleanupDUnitVMsRule = new CleanupDUnitVMsRule();

  private static final String multiKeyTrustStore;
  private static final String multiKeyClientKeyStore;
  private static final String multiKeyServerKeyStore;

  static {
    // The files were generated using the following commands.
    // The final step import the certificates in the "wrong" order (SSL implementation uses by
    // default the first one found, ignoring the alias).
    //
    // openssl genrsa -out ca.key 2048
    // # It will be necessary to provide a keystore password and the CN for each.
    // # Use "client" for the CN of the client keystore and "server" for the CN of the server key.
    // keytool -genkeypair -alias client -keyalg RSA -keystore client.jks
    // keytool -genkeypair -alias server -keyalg RSA -keystore server.jks
    // # Export CSRs
    // keytool -certreq -alias client -keystore client.jks -file client.csr
    // keytool -certreq -alias server -keystore server.jks -file server.csr
    // # Create a self-signed cert for the CA
    // openssl req -new -x509 -key ca.key -out ca.crt
    // # Sign them
    // openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -out client.crt -CAcreateserial
    // openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -out server.crt -CAcreateserial
    // # Import both of these leaf certs into a trust store but don't import CA
    // keytool -importcert -keystore trusted.jks -file client.crt -alias client
    // keytool -importcert -keystore trusted.jks -file server.crt -alias server
    // # Import the CA cert and the signed csrs back into the server and client keystores
    // keytool -importcert -file ca.crt -keystore server.jks -storepass passw0rd -alias ca
    // keytool -importcert -file server.crt -keystore server.jks -storepass passw0rd -alias server
    // keytool -importcert -file ca.crt -keystore client.jks -storepass passw0rd -alias ca
    // keytool -importcert -file client.crt -keystore client.jks -storepass passw0rd -alias client

    multiKeyTrustStore = createTempFileFromResource(ConnectCommandWithSSLMultiKeyTest.class,
        "/org/apache/geode/management/internal/cli/commands/multiKeyTrustStore.jks")
            .getAbsolutePath();
    multiKeyClientKeyStore = createTempFileFromResource(ConnectCommandWithSSLMultiKeyTest.class,
        "/org/apache/geode/management/internal/cli/commands/multiKeyClientKeyStore.jks")
            .getAbsolutePath();
    multiKeyServerKeyStore = createTempFileFromResource(ConnectCommandWithSSLMultiKeyTest.class,
        "/org/apache/geode/management/internal/cli/commands/multiKeyServerKeyStore.jks")
            .getAbsolutePath();
  }

  private static final Properties sslProperties = new Properties() {
    {
      setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannels.ALL);
      setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
      setProperty(SSL_DEFAULT_ALIAS, "server");
      setProperty(SSL_KEYSTORE, multiKeyServerKeyStore);
      setProperty(SSL_KEYSTORE_PASSWORD, "passw0rd");
      setProperty(SSL_KEYSTORE_TYPE, "JKS");
      setProperty(SSL_CIPHERS, "any");
      setProperty(SSL_PROTOCOLS, "any");
      setProperty(SSL_TRUSTSTORE, multiKeyTrustStore);
      setProperty(SSL_TRUSTSTORE_PASSWORD, "passw0rd");
      setProperty(SSL_TRUSTSTORE_TYPE, "JKS");
    }
  };

  private static final Properties gfshSslProperties = new Properties() {
    {
      setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannels.ALL);
      setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
      setProperty(SSL_DEFAULT_ALIAS, "client");
      setProperty(SSL_KEYSTORE, multiKeyClientKeyStore);
      setProperty(SSL_KEYSTORE_PASSWORD, "passw0rd");
      setProperty(SSL_KEYSTORE_TYPE, "JKS");
      setProperty(SSL_CIPHERS, "any");
      setProperty(SSL_PROTOCOLS, "any");
      setProperty(SSL_TRUSTSTORE, multiKeyTrustStore);
      setProperty(SSL_TRUSTSTORE_PASSWORD, "passw0rd");
      setProperty(SSL_TRUSTSTORE_TYPE, "JKS");
    }
  };

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  private static MemberVM locator;
  private OutputStream out = null;
  private File sslConfigFile = null;

  @Before
  public void before() throws Exception {
    locator = lsRule.startLocatorVM(0, l -> l.withProperties(sslProperties).withHttpService());
    IgnoredException.addIgnoredException("javax.net.ssl.SSLException: Unrecognized SSL message");
    sslConfigFile = temporaryFolder.newFile("ssl.properties");
    out = new FileOutputStream(sslConfigFile);
  }

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private void connectWithSSLTo(int port, GfshCommandRule.PortType locator) throws Exception {
    gfsh.connect(port, locator, "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isTrue();
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
    gfshSslProperties.store(out, null);

    connectWithSSLTo(locator.getPort(), GfshCommandRule.PortType.locator);
    gfsh.disconnect();

    connectWithSSLTo(locator.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    gfsh.disconnect();
  }

  @Test
  public void connectWithSSLShouldSucceedAndSubsequentConnectWithNoSSLShouldFail()
      throws Exception {
    sslProperties.store(out, null);

    // can connect to both locator and jmx
    connectWithSSLTo(locator.getPort(), GfshCommandRule.PortType.locator);
    gfsh.disconnect();

    // reconnect again with no SSL should fail
    failToConnectWithoutSSLTo(locator.getPort(), GfshCommandRule.PortType.locator);
    // it should fail at connecting to locator, not connecting to manager
    assertThat(gfsh.getGfshOutput()).doesNotContain("Connecting to Manager at");
  }
}
