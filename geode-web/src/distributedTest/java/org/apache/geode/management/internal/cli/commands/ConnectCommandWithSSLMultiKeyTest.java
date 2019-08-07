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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.security.SecurableCommunicationChannels;
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
  public static void beforeClass() {
    // The files were generated using the following commands.
    // echo "Add two certs to keystore to test using a specific alias."
    // keytool -genkey -keyalg RSA -validity 3650 -keysize 2048 -alias server -keystore
    // multiKeyStore.jks -storepass password -keypass password -dname "CN=127.0.0.1, O=Apache"
    // keytool -genkey -keyalg RSA -validity 3650 -keysize 2048 -alias client -keystore
    // multiKeyStore.jks -storepass password -keypass password -dname "CN=127.0.0.1, O=Apache"
    // echo "Export public cert..."
    // keytool -export -alias client -keystore multiKeyStore.jks -rfc -file client.cer -storepass
    // password
    // echo "Setup trustStore..."
    // keytool -import -alias client -file client.cer -keystore truststore.jks -storepass password
    // -noprompt
    // echo "Remove exported certs..."
    // rm client.cer
    String multiKeyTrustStore = createTempFileFromResource(ConnectCommandWithSSLMultiKeyTest.class,
        "/org/apache/geode/management/internal/cli/commands/multiKey_TrustStore.jks")
            .getAbsolutePath();
    String multiKeyKeyStore =
        createTempFileFromResource(ConnectCommandWithSSLMultiKeyTest.class,
            "/org/apache/geode/management/internal/cli/commands/multiKey_KeyStore.jks")
                .getAbsolutePath();

    sslProperties = new Properties();
    sslProperties.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannels.ALL);
    sslProperties.setProperty(SSL_REQUIRE_AUTHENTICATION, "true");
    sslProperties.setProperty(SSL_DEFAULT_ALIAS, "client");
    sslProperties.setProperty(SSL_KEYSTORE, multiKeyKeyStore);
    sslProperties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    sslProperties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    sslProperties.setProperty(SSL_CIPHERS, "any");
    sslProperties.setProperty(SSL_PROTOCOLS, "any");
    sslProperties.setProperty(SSL_TRUSTSTORE, multiKeyTrustStore);
    sslProperties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    sslProperties.setProperty(SSL_TRUSTSTORE_TYPE, "JKS");

    final Properties serializableProperties = new Properties();
    sslProperties
        .forEach((key, value) -> serializableProperties.setProperty((String) key, (String) value));
    locator = clusterRule.startLocatorVM(0,
        l -> l.withProperties(serializableProperties).withHttpService());
  }

  @Before
  public void before() throws Exception {
    IgnoredException.addIgnoredException("javax.net.ssl.SSLException: Unrecognized SSL message");
    sslConfigFile = temporaryFolder.newFile("ssl.properties");
    out = new FileOutputStream(sslConfigFile);
  }

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
    sslProperties.store(out, null);

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
