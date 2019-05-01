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
import java.util.List;

import org.apache.logging.log4j.Logger;
import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.logging.LogService;
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
  public void testCreateDataSourceWithJarOptionDoesNotThrowDriverError() {
    Logger logger = LogService.getLogger();
    logger.info("testCreateDataSourceWithJarOptionDoesNotThrowDriverError");
    System.out.println("testCreateDataSourceWithJarOptionDoesNotThrowDriverError");
    String URL = "jdbc:mysql://localhost/";
    IgnoredException.addIgnoredException(
        "An Exception was caught while trying to load the driver");
    logger.info("check point 1");
    System.out.println("check point 1");
    IgnoredException.addIgnoredException(
        "java.lang.ClassNotFoundException: com.mysql.cj.jdbc.Driver");
    logger.info("check point 2");
    System.out.println("check point 2");
    IgnoredException.addIgnoredException(
        "Access denied for user 'mysqlUser'@'localhost'");
    logger.info("check point 3");
    System.out.println("check point 3");
    IgnoredException.addIgnoredException(
        "Failed to connect to \"mysqlDataSource\". See log for details");
    logger.info("check point 4");
    System.out.println("check point 4");
    gfshConnector.executeAndAssertThat("set variable --name=APP_QUIET_EXECUTION --value=true")
        .statusIsSuccess();
    // create the data-source
    gfshConnector.executeAndAssertThat(
        "create jndi-binding --name=mysqlDataSource --type=simple --username=mysqlUser --password=mysqlPass --jdbc-driver-class=\"com.mysql.cj.jdbc.Driver\" --connection-url=\""
            + URL + "\"")
        .statusIsError().containsOutput("An Exception was caught while trying to load the driver");
    logger.info("check point 5");
    System.out.println("check point 5");
    final String jdbcJarName = "mysql-connector-java-8.0.15.jar";
    logger.info("check point 6");
    System.out.println("check point 6");
    File mysqlDriverFile = loadTestResource("/" + jdbcJarName);
    logger.info("check point 7");
    System.out.println("check point 7");
    AssertionsForClassTypes.assertThat(mysqlDriverFile).exists();
    logger.info("check point 8");
    System.out.println("check point 8");
    String jarFile = mysqlDriverFile.getAbsolutePath();
    logger.info("check point 9");
    System.out.println("check point 9");
    gfshConnector.executeAndAssertThat("set variable --name=APP_QUIET_EXECUTION --value=true")
        .statusIsSuccess();
    gfshConnector.executeAndAssertThat("deploy --jar=" + jarFile).statusIsSuccess();
    logger.info("check point 10");
    System.out.println("check point 10");
//    gfshConnector.executeAndAssertThat("set variable --name=APP_QUIET_EXECUTION --value=true")
//        .statusIsSuccess();
//    gfshConnector.executeAndAssertThat(
//        "create jndi-binding --name=mysqlDataSource --type=simple --username=mysqlUser --password=mysqlPass --jdbc-driver-class=\"com.mysql.cj.jdbc.Driver\" --connection-url=\""
//            + URL + "\"")
//        .containsOutput("Failed to connect to \"mysqlDataSource\". See log for details");
//    logger.info("check point 11");
//    System.out.println("check point 11");
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
