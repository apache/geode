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

package org.apache.geode.management.internal.configuration;

import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_TYPE;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.security.SecurableCommunicationChannels;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({DistributedTest.class})
public class DeployJarWithSSLDUnitTest {

  private static File jks;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule(3);

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator;

  private File sslConfigFile = null;

  static {
    try {
      /*
       * This file was generated with the following command:
       * keytool -genkey -dname "CN=localhost" -alias self -validity 3650 -keyalg EC \
       * -keystore trusted.keystore -keypass password -storepass password \
       * -ext san=ip:127.0.0.1 -storetype jks
       */
      jks = new File(DeployJarWithSSLDUnitTest.class.getClassLoader()
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

  @Before
  public void before() throws Exception {
    locator = lsRule.startLocatorVM(0, sslProperties);

    sslConfigFile = temporaryFolder.newFile("ssl.properties");
    FileOutputStream out = new FileOutputStream(sslConfigFile);
    sslProperties.store(out, null);
  }

  @Test
  public void deployJarToCluster() throws Exception {
    lsRule.startServerVM(1, sslProperties, locator.getPort());

    gfsh.connectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator,
        "security-properties-file", sslConfigFile.getAbsolutePath());

    String clusterJar = createJarFileWithClass("Cluster", "cluster.jar", temporaryFolder.getRoot());
    gfsh.executeAndAssertThat("deploy --jar=" + clusterJar).statusIsSuccess();
  }

  @Test
  public void startServerAfterDeployJar() throws Exception {
    gfsh.connectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator,
        "security-properties-file", sslConfigFile.getAbsolutePath());

    String clusterJar = createJarFileWithClass("Cluster", "cluster.jar", temporaryFolder.getRoot());
    gfsh.executeAndAssertThat("deploy --jar=" + clusterJar).statusIsSuccess();

    lsRule.startServerVM(1, sslProperties, locator.getPort());
  }

  @Test
  public void deployJarWithMultipleLocators() throws Exception {
    MemberVM locator2 = lsRule.startLocatorVM(1, sslProperties, locator.getPort());
    lsRule.startServerVM(2, sslProperties, locator2.getPort());

    gfsh.connectAndVerify(locator.getPort(), GfshCommandRule.PortType.locator,
        "security-properties-file", sslConfigFile.getAbsolutePath());

    String clusterJar = createJarFileWithClass("Cluster", "cluster.jar", temporaryFolder.getRoot());
    gfsh.executeAndAssertThat("deploy --jar=" + clusterJar).statusIsSuccess();
  }

  protected String createJarFileWithClass(String className, String jarName, File dir)
      throws IOException {
    File jarFile = new File(dir, jarName);
    new ClassBuilder().writeJarFromName(className, jarFile);
    return jarFile.getCanonicalPath();
  }
}
