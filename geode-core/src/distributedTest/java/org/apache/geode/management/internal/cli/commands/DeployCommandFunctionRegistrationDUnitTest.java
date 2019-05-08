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

import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.URISyntaxException;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.compiler.JarBuilder;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.FunctionServiceTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category({FunctionServiceTest.class})
public class DeployCommandFunctionRegistrationDUnitTest {
  private MemberVM locator;
  private MemberVM server;

  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Rule
  public ClusterStartupRule lsRule = new ClusterStartupRule();

  @Rule
  public transient GfshCommandRule gfshConnector = new GfshCommandRule();

  @Before
  public void setup() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server = lsRule.startServerVM(1, locator.getPort());

    gfshConnector.connectAndVerify(locator);
  }

  @Test
  public void deployImplements() throws Exception {
    JarBuilder jarBuilder = new JarBuilder();
    File source = loadTestResource(
        "/org/apache/geode/management/internal/deployment/ImplementsFunction.java");

    File outputJar = new File(temporaryFolder.getRoot(), "output.jar");
    jarBuilder.buildJar(outputJar, source);

    gfshConnector.executeAndAssertThat("deploy --jar=" + outputJar.getCanonicalPath())
        .statusIsSuccess();
    server.invoke(() -> assertThatCanLoad(
        "org.apache.geode.management.internal.deployment.ImplementsFunction"));
    server.invoke(() -> assertThatFunctionHasVersion("myTestFunction", "ImplementsFunctionResult"));
  }

  @Test
  public void deployExtends() throws Exception {
    JarBuilder jarBuilder = new JarBuilder();
    File source = loadTestResource(
        "/org/apache/geode/management/internal/deployment/ExtendsFunctionAdapter.java");

    File outputJar = new File(temporaryFolder.getRoot(), "output.jar");
    jarBuilder.buildJar(outputJar, source);

    gfshConnector.executeAndAssertThat("deploy --jar=" + outputJar.getCanonicalPath())
        .statusIsSuccess();
    server.invoke(() -> assertThatCanLoad(
        "org.apache.geode.management.internal.deployment.ExtendsFunctionAdapter"));
    server.invoke(() -> assertThatFunctionHasVersion(
        "org.apache.geode.management.internal.deployment.ExtendsFunctionAdapter",
        "ExtendsFunctionAdapterResult"));
  }

  @Test
  public void testCreateJndiBindingWithJarOptionDoesNotThrowDriverErrorAfterDeployingJarAsDriver()
      throws URISyntaxException {
    String URL = "jdbc:mysql://localhost/";
    IgnoredException.addIgnoredException(
        "An Exception was caught while trying to load the driver");
    IgnoredException.addIgnoredException(
        "create jndi-binding failed");
    IgnoredException.addIgnoredException(
        "java.lang.ClassNotFoundException: com.mysql.cj.jdbc.Driver");
    IgnoredException.addIgnoredException(
        "Access denied for user 'mysqlUser'@'localhost'");
    IgnoredException.addIgnoredException(
        "com.mysql.cj.jdbc.exceptions.CommunicationsException: Communications link failure");

    // create the data-source
    gfshConnector.executeAndAssertThat(
        "create jndi-binding --name=mysqlDataSource --type=simple --username=mysqlUser --password=mysqlPass --jdbc-driver-class=\"com.mysql.cj.jdbc.Driver\" --connection-url=\""
            + URL + "\"")
        .statusIsError().containsOutput("An Exception was caught while trying to load the driver");

    final String jdbcJarName = "mysql-connector-java-8.0.15.jar";
    File mysqlDriverFile = loadTestResource("/" + jdbcJarName);
    assertThat(mysqlDriverFile).exists();
    String jarFile = mysqlDriverFile.getAbsolutePath();

    IgnoredException.removeAllExpectedExceptions();

    gfshConnector.executeAndAssertThat("deploy --jar=" + jarFile).statusIsSuccess();
    gfshConnector.executeAndAssertThat(
        "create jndi-binding --name=mysqlDataSource --type=simple --username=mysqlUser --password=mysqlPass --jdbc-driver-class=\"com.mysql.cj.jdbc.Driver\" --connection-url=\""
            + URL + "\"")
        .containsOutput("Failed to connect to \"mysqlDataSource\". See log for details");
  }

  @Test
  public void testCreateJndiBindingWithJarOptionDoesNotThrowDriverErrorAfterDeployingJarAsDriverOnNewServer()
      throws Exception {
    String URL = "jdbc:mysql://localhost/";
    IgnoredException.addIgnoredException(
        "An Exception was caught while trying to load the driver");
    IgnoredException.addIgnoredException(
        "create jndi-binding failed");
    IgnoredException.addIgnoredException(
        "java.lang.ClassNotFoundException: com.mysql.cj.jdbc.Driver");
    IgnoredException.addIgnoredException(
        "Access denied for user 'mysqlUser'@'localhost'");
    IgnoredException.addIgnoredException(
        "com.mysql.cj.jdbc.exceptions.CommunicationsException: Communications link failure");

    // create the data-source
    gfshConnector.executeAndAssertThat(
        "create jndi-binding --name=mysqlDataSource --type=simple --username=mysqlUser --password=mysqlPass --jdbc-driver-class=\"com.mysql.cj.jdbc.Driver\" --connection-url=\""
            + URL + "\"")
        .statusIsError().containsOutput("An Exception was caught while trying to load the driver");

    final String jdbcJarName = "mysql-connector-java-8.0.15.jar";
    File mysqlDriverFile = loadTestResource("/" + jdbcJarName);
    assertThat(mysqlDriverFile).exists();
    String jarFile = mysqlDriverFile.getAbsolutePath();

    IgnoredException.removeAllExpectedExceptions();

    gfshConnector.executeAndAssertThat("deploy --jar=" + jarFile).statusIsSuccess();
    gfshConnector.executeAndAssertThat(
        "create jndi-binding --name=mysqlDataSource --type=simple --username=mysqlUser --password=mysqlPass --jdbc-driver-class=\"com.mysql.cj.jdbc.Driver\" --connection-url=\""
            + URL + "\"")
        .containsOutput("Failed to connect to \"mysqlDataSource\". See log for details");

    server.stop();
    locator.stop();
    locator = lsRule.startLocatorVM(0);
    MemberVM server1 = lsRule.startServerVM(2, locator.getPort());
    gfshConnector.connectAndVerify(locator);


    gfshConnector.executeAndAssertThat(
        "create jndi-binding --name=mysqlDataSource --type=simple --username=mysqlUser --password=mysqlPass --jdbc-driver-class=\"com.mysql.cj.jdbc.Driver\" --connection-url=\""
            + URL + "\"")
        .containsOutput("Failed to connect to \"mysqlDataSource\". See log for details");
  }

  private File loadTestResource(String fileName) throws URISyntaxException {
    String filePath =
        createTempFileFromResource(this.getClass(), fileName).getAbsolutePath();
    assertThat(filePath).isNotNull();

    return new File(filePath);
  }

  private static void assertThatFunctionHasVersion(String functionId, String version) {
    GemFireCacheImpl gemFireCache = GemFireCacheImpl.getInstance();
    DistributedSystem distributedSystem = gemFireCache.getDistributedSystem();
    Execution execution = FunctionService.onMember(distributedSystem.getDistributedMember());
    List<String> result = (List<String>) execution.execute(functionId).getResult();
    assertThat(result.get(0)).isEqualTo(version);
  }

  private static void assertThatCanLoad(String className) throws ClassNotFoundException {
    assertThat(ClassPathLoader.getLatest().forName(className)).isNotNull();
  }
}
