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

import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.CLUSTER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.util.Properties;

import javax.net.ssl.HttpsURLConnection;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.security.SecurableCommunicationChannels;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class ConnectCommandWithSSLTest {

  private static File jks;

  static {
    try {
      jks = new File(ConnectCommandWithSSLTest.class.getClassLoader()
          .getResource("ssl/trusted.keystore").toURI());
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
  }

  private static Properties sslProperties = new Properties() {
    {
      setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannels.ALL);
      setProperty(SSL_KEYSTORE, jks.getAbsolutePath());
      setProperty(SSL_KEYSTORE_PASSWORD, "password");
      setProperty(SSL_KEYSTORE_TYPE, "JKS");
      setProperty(SSL_TRUSTSTORE, jks.getAbsolutePath());
      setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
      setProperty(SSL_TRUSTSTORE_TYPE, "JKS");
      setProperty(SSL_CIPHERS, "any");
      setProperty(SSL_PROTOCOLS, "any");
    }
  };

  private static Properties jmxSslProperties = new Properties() {
    {
      setProperty(JMX_MANAGER_SSL_ENABLED, "true");
      setProperty(JMX_MANAGER_SSL_KEYSTORE, jks.getAbsolutePath());
      setProperty(JMX_MANAGER_SSL_KEYSTORE_PASSWORD, "password");
      setProperty(JMX_MANAGER_SSL_KEYSTORE_TYPE, "JKS");
      setProperty(JMX_MANAGER_SSL_TRUSTSTORE, jks.getAbsolutePath());
      setProperty(JMX_MANAGER_SSL_TRUSTSTORE_PASSWORD, "password");
      setProperty(JMX_MANAGER_SSL_CIPHERS, "any");
      setProperty(JMX_MANAGER_SSL_PROTOCOLS, "any");
    }
  };

  private static Properties clusterSslProperties = new Properties() {
    {
      setProperty(CLUSTER_SSL_ENABLED, "true");
      setProperty(CLUSTER_SSL_KEYSTORE, jks.getAbsolutePath());
      setProperty(CLUSTER_SSL_KEYSTORE_PASSWORD, "password");
      setProperty(CLUSTER_SSL_KEYSTORE_TYPE, "JKS");
      setProperty(CLUSTER_SSL_TRUSTSTORE, jks.getAbsolutePath());
      setProperty(CLUSTER_SSL_TRUSTSTORE_PASSWORD, "password");
      setProperty(CLUSTER_SSL_CIPHERS, "any");
      setProperty(CLUSTER_SSL_PROTOCOLS, "any");
    }
  };

  private static Properties httpSslProperties = new Properties() {
    {
      setProperty(HTTP_SERVICE_SSL_ENABLED, "true");
      setProperty(HTTP_SERVICE_SSL_KEYSTORE, jks.getAbsolutePath());
      setProperty(HTTP_SERVICE_SSL_KEYSTORE_PASSWORD, "password");
      setProperty(HTTP_SERVICE_SSL_KEYSTORE_TYPE, "JKS");
      setProperty(HTTP_SERVICE_SSL_TRUSTSTORE, jks.getAbsolutePath());
      setProperty(HTTP_SERVICE_SSL_TRUSTSTORE_PASSWORD, "password");
      setProperty(HTTP_SERVICE_SSL_CIPHERS, "any");
      setProperty(HTTP_SERVICE_SSL_PROTOCOLS, "any");
    }
  };

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private static MemberVM locator;
  private OutputStream out = null;
  private File sslConfigFile = null;

  @Before
  public void before() throws Exception {
    locator = lsRule.startLocatorVM(0, sslProperties);
    HttpsURLConnection.setDefaultHostnameVerifier((hostname, session) -> true);
    IgnoredException.addIgnoredException("javax.net.ssl.SSLException: Unrecognized SSL message");
    sslConfigFile = temporaryFolder.newFile("ssl.properties");
    out = new FileOutputStream(sslConfigFile);
  }

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @Test
  public void connectWithNoSSL() throws Exception {
    gfsh.connect(locator.getPort(), GfshShellConnectionRule.PortType.locator);
    assertThat(gfsh.isConnected()).isFalse();
    // should fail at connecting to locator stage
    assertThat(gfsh.getGfshOutput()).doesNotContain("Connecting to Manager at");
    assertThat(gfsh.getGfshOutput())
        .contains("trying to connect a non-SSL-enabled client to an SSL-enabled locator");

    gfsh.connect(locator.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger);
    assertThat(gfsh.isConnected()).isFalse();
    assertThat(gfsh.getGfshOutput()).contains("non-JRMP server at remote endpoint");

    gfsh.connect(locator.getHttpPort(), GfshShellConnectionRule.PortType.http);
    assertThat(gfsh.isConnected()).isFalse();
    assertThat(gfsh.getGfshOutput()).contains("Unexpected end of file from server");
  }

  @Test
  public void connectWithSSL() throws Exception {
    sslProperties.store(out, null);

    gfsh.connect(locator.getPort(), GfshShellConnectionRule.PortType.locator,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isTrue();
    gfsh.disconnect();

    gfsh.connect(locator.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isTrue();
    gfsh.disconnect();

    gfsh.connect(locator.getHttpPort(), GfshShellConnectionRule.PortType.http,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isTrue();
  }

  @Test
  public void connectWithJmxSSL() throws Exception {
    jmxSslProperties.store(out, null);
    // can't connect locator
    gfsh.connect(locator.getPort(), GfshShellConnectionRule.PortType.locator,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isFalse();
    assertThat(gfsh.getGfshOutput()).doesNotContain("Connecting to Manager at");
    assertThat(gfsh.getGfshOutput())
        .contains("trying to connect a non-SSL-enabled client to an SSL-enabled locator");

    // can connect to jmx
    gfsh.connect(locator.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isTrue();
    gfsh.disconnect();

    // cannot conect to http
    gfsh.connect(locator.getHttpPort(), GfshShellConnectionRule.PortType.http,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isFalse();
  }

  @Test
  /*
   * apparently cluster-ssl-* configurations are copied to jmx-ssl-* and http-server-ssl-*, so
   * connection to other channels will succeed as well. see DistributionConfigImpl around line 986:
   * if (!isConnected) { copySSLPropsToServerSSLProps(); copySSLPropsToJMXSSLProps();
   * copyClusterSSLPropsToGatewaySSLProps(); copySSLPropsToHTTPSSLProps(); }
   */
  public void connectWithClusterSSL() throws Exception {
    clusterSslProperties.store(out, null);
    // can connect to locator and jmx
    gfsh.connect(locator.getPort(), GfshShellConnectionRule.PortType.locator,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isTrue();
    gfsh.disconnect();

    // can connect to jmx
    gfsh.connect(locator.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isTrue();
    gfsh.disconnect();

    // can conect to http
    gfsh.connect(locator.getHttpPort(), GfshShellConnectionRule.PortType.http,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isTrue();
  }

  @Test
  public void connectWithHttpSSL() throws Exception {
    httpSslProperties.store(out, null);
    // can connect to locator and jmx
    gfsh.connect(locator.getPort(), GfshShellConnectionRule.PortType.locator,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isFalse();

    // can connect to jmx
    gfsh.connect(locator.getJmxPort(), GfshShellConnectionRule.PortType.jmxManger,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isFalse();

    // cannot conect to http
    gfsh.connect(locator.getHttpPort(), GfshShellConnectionRule.PortType.http,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isTrue();
  }

  @Test
  public void connectWithClusterAndJmxSSL() throws Exception {
    Properties combined = new Properties();
    combined.putAll(jmxSslProperties);
    combined.putAll(clusterSslProperties);
    combined.store(out, null);

    // can connect to both locator and jmx
    gfsh.connect(locator.getPort(), GfshShellConnectionRule.PortType.locator,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isTrue();
  }


  @Test
  public void connectWithSSLAndThenWithNoSSL() throws Exception {
    sslProperties.store(out, null);

    // can connect to both locator and jmx
    gfsh.connect(locator.getPort(), GfshShellConnectionRule.PortType.locator,
        "security-properties-file", sslConfigFile.getAbsolutePath());
    assertThat(gfsh.isConnected()).isTrue();
    gfsh.disconnect();

    // reconnect again with no SSL should fail
    gfsh.connect(locator.getPort(), GfshShellConnectionRule.PortType.locator);
    assertThat(gfsh.isConnected()).isFalse();
    // it should fail at connecting to locator, not connecting to manager
    assertThat(gfsh.getGfshOutput()).doesNotContain("Connecting to Manager at");
  }
}
