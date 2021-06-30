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
package org.apache.geode.management;

import static org.apache.geode.distributed.ConfigurationProperties.BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_VERIFY_MEMBER_TIMEOUT;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENDPOINT_IDENTIFICATION_ENABLED;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_TYPE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_REQUIRE_AUTHENTICATION;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_TYPE;
import static org.apache.geode.test.junit.rules.gfsh.GfshRule.startLocatorCommand;
import static org.apache.geode.test.junit.rules.gfsh.GfshRule.startServerCommand;
import static org.apache.geode.test.util.ResourceUtils.createFileFromResource;
import static org.apache.geode.test.util.ResourceUtils.getResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.internal.UniquePortSupplier;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.rules.gfsh.GfshExecution;
import org.apache.geode.test.junit.rules.gfsh.GfshRule;
import org.apache.geode.test.junit.rules.gfsh.GfshScript;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;
import org.apache.geode.test.version.TestVersion;
import org.apache.geode.test.version.VersionManager;

/**
 * This test iterates through the versions of Geode and executes client compatibility with
 * the current version of Geode.
 */
@Category({BackwardCompatibilityTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class RollingUpgradeWithGfshDUnitTest {
  private final UniquePortSupplier portSupplier = new UniquePortSupplier();
  private final String oldVersion;
  private File propertiesFile;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<String> data() {
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(s -> TestVersion.compare(s, "1.10.0") < 0);

    // just upgrading from 1.13.2 for now
    result = Arrays.asList("1.13.2");
    return result;
  }

  @Rule
  public GfshRule oldGfsh;

  @Rule
  public GfshRule currentGfsh;

  // @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void before() throws IOException {
    // TODO: delete this line and uncomment @Rule
    tempFolder.create();

    ClassBuilder classBuilder = new ClassBuilder();
    String jarName1 = "DeployCommandsDUnit1.jar";
    File jar1 = new File(tempFolder.getRoot(), jarName1);
    String class1 = "DeployCommandsDUnitA";
    classBuilder.writeJarFromName(class1, jar1);

    final File keystoreFile = createFileFromResource(
        getResource("foo-keystore.jks"), tempFolder.getRoot(),
        "foo-keystore.jks");
    final File truststoreFile = createFileFromResource(
        getResource("foo-truststore.jks"), tempFolder.getRoot(),
        "foo-truststore.jks");

    /*
     * We must use absolute paths for truststore and keystore in properties file and
     * since we don't know those at coding-time, we must generate the file.
     * Since GfshRule provides no way to pass along Properties object to start server etc,
     * we must write the properties to an actual file.
     */
    Properties properties = new Properties();

    properties.setProperty(SECURITY_LOG_LEVEL, "info");
    properties.setProperty(SECURITY_PEER_VERIFY_MEMBER_TIMEOUT, "1000");

    properties.setProperty(SSL_REQUIRE_AUTHENTICATION, "false");
    // properties.setProperty(SSL_ENABLED_COMPONENTS, "all");
    properties.setProperty(SSL_ENABLED_COMPONENTS, "cluster,server");
    properties.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "true");
    // properties.setProperty(SSL_ENDPOINT_IDENTIFICATION_ENABLED, "false");
    properties.setProperty(SSL_KEYSTORE_TYPE, "jks");
    properties.setProperty(SSL_TRUSTSTORE_TYPE, "jks");

    properties.setProperty(SSL_KEYSTORE_PASSWORD, "geode");
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "geode");

    properties.setProperty(SSL_KEYSTORE, keystoreFile.getAbsolutePath());
    properties.setProperty(SSL_TRUSTSTORE, truststoreFile.getAbsolutePath());

    properties.setProperty(BIND_ADDRESS, "bburcham-a01.vmware.com");

    propertiesFile = tempFolder.newFile("gemfire.properties");
    final FileOutputStream fileOutputStream =
        new FileOutputStream(propertiesFile.getAbsolutePath());
    properties.store(fileOutputStream, "");

    // propertiesFile = createFileFromResource(
    // getResource("gemfire.properties"), tempFolder.getRoot(),
    // "gemfire.properties");
  }

  public RollingUpgradeWithGfshDUnitTest(String version) throws IOException {
    oldVersion = version;
    oldGfsh = new GfshRule(oldVersion);
    // can try "1.13.3" here (want locally-built one with reverted GEODE-9139 fix)
    currentGfsh = new GfshRule();
  }

  @Test
  public void testRollingUpgradeWithDeployment() throws Exception {
    int locatorPort = portSupplier.getAvailablePort();
    int locatorJmxPort = portSupplier.getAvailablePort();
    int locator2Port = portSupplier.getAvailablePort();
    int locator2JmxPort = portSupplier.getAvailablePort();
    int server1Port = portSupplier.getAvailablePort();
    int server2Port = portSupplier.getAvailablePort();

    GfshExecution startupExecution =
        GfshScript.of(startLocatorCommandWithConfig("loc1", locatorPort, locatorJmxPort, -1))
            .and(startLocatorCommandWithConfig("loc2", locator2Port, locator2JmxPort, locatorPort))
            .and(startServerCommandWithConfig("server1", server1Port, locatorPort))
            .and(startServerCommandWithConfig("server2", server2Port, locatorPort))
            .and(deployDirCommand())
            .execute(oldGfsh, tempFolder.getRoot());

    initializeRegion(locatorPort);
    causeP2PTraffic(locatorPort);

    // doing rolling upgrades
    oldGfsh.stopLocator(startupExecution, "loc1");
    GfshScript.of(startLocatorCommandWithConfig("loc1", locatorPort, locatorJmxPort, locator2Port))
        .execute(currentGfsh, tempFolder.getRoot());
    verifyListDeployed(locatorPort);

    causeP2PTraffic(locatorPort);

    oldGfsh.stopLocator(startupExecution, "loc2");
    GfshScript.of(startLocatorCommandWithConfig("loc2", locator2Port, locator2JmxPort, locatorPort))
        .execute(currentGfsh, tempFolder.getRoot());
    verifyListDeployed(locator2Port);

    causeP2PTraffic(locatorPort);

    // make sure servers can do rolling upgrade too
    oldGfsh.stopServer(startupExecution, "server1");
    GfshScript.of(startServerCommandWithConfig("server1", server1Port, locatorPort))
        .execute(currentGfsh, tempFolder.getRoot());

    causeP2PTraffic(locatorPort);

    oldGfsh.stopServer(startupExecution, "server2");
    GfshScript.of(startServerCommandWithConfig("server2", server2Port, locatorPort))
        .execute(currentGfsh, tempFolder.getRoot());

    causeP2PTraffic(locatorPort);

  }

  private void verifyListDeployed(int locatorPort) {
    GfshExecution list_deployed =
        GfshScript.of("connect --locator=bburcham-a01.vmware.com[" + locatorPort + "]")
            .and("list deployed").execute(currentGfsh, tempFolder.getRoot());
    assertThat(list_deployed.getOutputText()).contains("DeployCommandsDUnit1.jar")
        .contains("server1").contains("server2");
    GfshScript.of("disconnect").execute(currentGfsh, tempFolder.getRoot());
  }

  private String deployDirCommand() throws IOException {
    ClassBuilder classBuilder = new ClassBuilder();
    File jarsDir = tempFolder.getRoot();
    String jarName1 = "DeployCommandsDUnit1.jar";
    File jar1 = new File(jarsDir, jarName1);
    String class1 = "DeployCommandsDUnitA";
    classBuilder.writeJarFromName(class1, jar1);
    return "deploy --dir=" + jarsDir.getAbsolutePath();
  }

  private String startServerCommandWithConfig(final String server, final int serverPort,
      final int locatorPort) {
    return startServerCommand(server, serverPort, locatorPort) + propertiesFileParameter();
  }

  private String startLocatorCommandWithConfig(final String name, final int locatorPort,
      final int locatorJmxPort,
      final int connectedLocatorPort) {
    return startLocatorCommand(name, locatorPort, locatorJmxPort, 0, connectedLocatorPort) +
        propertiesFileParameter();
  }

  private String propertiesFileParameter() {
    return " --properties-file=" + propertiesFile.getAbsolutePath() +
        " --J=-Dgemfire.forceDnsUse=true --J=-Djdk.tls.trustNameService=true";
  }

  private void initializeRegion(int locatorPort) {
    GfshExecution getResponse =
        GfshScript.of("connect --locator=bburcham-a01.vmware.com[" + locatorPort + "]")
            .and("create region --name=region1 --type=REPLICATE")
            .and("list regions")
            .execute(currentGfsh, tempFolder.getRoot());

    assertThat(getResponse.getOutputText()).contains("region1");

    GfshScript.of("disconnect").execute(currentGfsh, tempFolder.getRoot());
  }

  private void causeP2PTraffic(int locatorPort) {
    GfshExecution getResponse =
        GfshScript.of("connect --locator=bburcham-a01.vmware.com[" + locatorPort + "]")
            .and("put --key='123abc' --value='Hello World!!' --region=region1")
            .and("get --key='123abc' --region=region1")
            .execute(currentGfsh, tempFolder.getRoot());

    assertThat(getResponse.getOutputText()).contains("Hello World!!");

    GfshScript.of("disconnect").execute(currentGfsh, tempFolder.getRoot());
  }
}
