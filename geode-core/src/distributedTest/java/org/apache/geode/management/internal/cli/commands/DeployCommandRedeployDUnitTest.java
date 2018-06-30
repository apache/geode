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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.Serializable;
import java.net.URL;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.io.FileUtils;
import org.awaitility.Awaitility;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.FunctionServiceTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category({DistributedTest.class, FunctionServiceTest.class})
public class DeployCommandRedeployDUnitTest {
  private static final String VERSION1 = "Version1";
  private static final String VERSION2 = "Version2";

  private static final String JAR_NAME_A = "DeployCommandRedeployDUnitTestA.jar";
  private static final String FUNCTION_A = "DeployCommandRedeployDUnitFunctionA";
  private File jarAVersion1;
  private File jarAVersion2;

  private static final String JAR_NAME_B = "DeployCommandRedeployDUnitTestB.jar";
  private static final String FUNCTION_B = "DeployCommandRedeployDUnitFunctionB";
  private static final String PACKAGE_B = "jddunit.function";
  private static final String FULLY_QUALIFIED_FUNCTION_B = PACKAGE_B + "." + FUNCTION_B;
  private File jarBVersion1;
  private File jarBVersion2;

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
    jarAVersion1 = createJarWithFunctionA(VERSION1);
    jarAVersion2 = createJarWithFunctionA(VERSION2);

    jarBVersion1 = createJarWithFunctionB(VERSION1);
    jarBVersion2 = createJarWithFunctionB(VERSION2);

    locator = lsRule.startLocatorVM(0);
    server = lsRule.startServerVM(1, locator.getPort());

    gfshConnector.connectAndVerify(locator);
  }

  @Test
  public void redeployJarsWithNewVersionsOfFunctions() throws Exception {
    gfshConnector.executeAndAssertThat("deploy --jar=" + jarAVersion1.getCanonicalPath())
        .statusIsSuccess();
    server.invoke(() -> assertThatCanLoad(JAR_NAME_A, FUNCTION_A));
    server.invoke(() -> assertThatFunctionHasVersion(FUNCTION_A, VERSION1));

    gfshConnector.executeAndAssertThat("deploy --jar=" + jarBVersion1.getCanonicalPath())
        .statusIsSuccess();
    server.invoke(() -> assertThatCanLoad(JAR_NAME_A, FUNCTION_A));
    server.invoke(() -> assertThatCanLoad(JAR_NAME_B, FULLY_QUALIFIED_FUNCTION_B));
    server.invoke(() -> assertThatFunctionHasVersion(FUNCTION_A, VERSION1));
    server.invoke(() -> assertThatFunctionHasVersion(FUNCTION_B, VERSION1));

    gfshConnector.executeAndAssertThat("deploy --jar=" + jarBVersion2.getCanonicalPath())
        .statusIsSuccess();
    server.invoke(() -> assertThatCanLoad(JAR_NAME_A, FUNCTION_A));
    server.invoke(() -> assertThatCanLoad(JAR_NAME_B, FULLY_QUALIFIED_FUNCTION_B));
    server.invoke(() -> assertThatFunctionHasVersion(FUNCTION_A, VERSION1));
    server.invoke(() -> assertThatFunctionHasVersion(FUNCTION_B, VERSION2));

    gfshConnector.executeAndAssertThat("deploy --jar=" + jarAVersion2.getCanonicalPath())
        .statusIsSuccess();
    server.invoke(() -> assertThatCanLoad(JAR_NAME_A, FUNCTION_A));
    server.invoke(() -> assertThatCanLoad(JAR_NAME_B, FULLY_QUALIFIED_FUNCTION_B));
    server.invoke(() -> assertThatFunctionHasVersion(FUNCTION_A, VERSION2));
    server.invoke(() -> assertThatFunctionHasVersion(FUNCTION_B, VERSION2));
  }

  @Test
  public void redeployJarsWithNewVersionsOfFunctionsAndMultipleLocators() throws Exception {
    Properties props = new Properties();
    props.setProperty("locators", "localhost[" + locator.getPort() + "]");
    MemberVM locator2 = lsRule.startLocatorVM(2, props);

    gfshConnector.executeAndAssertThat("deploy --jar=" + jarAVersion1.getCanonicalPath())
        .statusIsSuccess();
    server.invoke(() -> assertThatCanLoad(JAR_NAME_A, FUNCTION_A));
    server.invoke(() -> assertThatFunctionHasVersion(FUNCTION_A, VERSION1));


    gfshConnector.executeAndAssertThat("deploy --jar=" + jarAVersion2.getCanonicalPath())
        .statusIsSuccess();
    server.invoke(() -> assertThatCanLoad(JAR_NAME_A, FUNCTION_A));
    server.invoke(() -> assertThatFunctionHasVersion(FUNCTION_A, VERSION2));

    server.stopMember(false);

    lsRule.startServerVM(1, locator.getPort());

    server.invoke(() -> assertThatCanLoad(JAR_NAME_A, FUNCTION_A));
    server.invoke(() -> assertThatFunctionHasVersion(FUNCTION_A, VERSION2));
  }

  @Test
  public void hotDeployShouldNotResultInAnyFailedFunctionExecutions() throws Exception {
    gfshConnector.executeAndAssertThat("deploy --jar=" + jarAVersion1.getCanonicalPath())
        .statusIsSuccess();
    server.invoke(() -> assertThatCanLoad(JAR_NAME_A, FUNCTION_A));
    server.invoke(() -> assertThatFunctionHasVersion(FUNCTION_A, VERSION1));

    server.invoke(() -> LoopingFunctionExecutor.startExecuting(FUNCTION_A));
    server.invoke(() -> LoopingFunctionExecutor.waitForExecutions(100));

    gfshConnector.executeAndAssertThat("deploy --jar=" + jarAVersion2.getCanonicalPath())
        .statusIsSuccess();
    server.invoke(() -> assertThatCanLoad(JAR_NAME_A, FUNCTION_A));
    server.invoke(() -> assertThatFunctionHasVersion(FUNCTION_A, VERSION2));

    server.invoke(() -> LoopingFunctionExecutor.waitForExecutions(100));
    server.invoke(LoopingFunctionExecutor::stopExecutionAndThrowAnyException);
  }

  // Note that jar A is a Declarable Function, while jar B is only a Function.
  // Also, the function for jar A resides in the default package, whereas jar B specifies a package.
  // This ensures that this test has identical coverage to some tests that it replaced.
  private File createJarWithFunctionA(String version) throws Exception {
    URL classTemplateUrl = DeployCommandRedeployDUnitTest.class
        .getResource("DeployCommandRedeployDUnitTest_FunctionATemplate");
    assertThat(classTemplateUrl).isNotNull();

    String classContents = FileUtils.readFileToString(new File(classTemplateUrl.toURI()), "UTF-8");
    classContents = classContents.replaceAll("FUNCTION_A", FUNCTION_A);
    classContents = classContents.replaceAll("VERSION", version);

    File jar = new File(temporaryFolder.newFolder(JAR_NAME_A + version), this.JAR_NAME_A);
    ClassBuilder functionClassBuilder = new ClassBuilder();
    functionClassBuilder.writeJarFromContent(FUNCTION_A, classContents, jar);

    return jar;
  }

  private File createJarWithFunctionB(String version) throws Exception {
    URL classTemplateUrl = DeployCommandRedeployDUnitTest.class
        .getResource("DeployCommandRedeployDUnitTest_FunctionBTemplate");
    assertThat(classTemplateUrl).isNotNull();

    String classContents = FileUtils.readFileToString(new File(classTemplateUrl.toURI()), "UTF-8");
    classContents = classContents.replaceAll("PACKAGE_B", PACKAGE_B);
    classContents = classContents.replaceAll("FUNCTION_B", FUNCTION_B);
    classContents = classContents.replaceAll("VERSION", version);

    File jar = new File(temporaryFolder.newFolder(JAR_NAME_B + version), this.JAR_NAME_B);
    ClassBuilder functionClassBuilder = new ClassBuilder();
    functionClassBuilder.writeJarFromContent("jddunit/function/" + FUNCTION_B, classContents, jar);

    return jar;
  }

  private static void assertThatFunctionHasVersion(String functionId, String version) {
    GemFireCacheImpl gemFireCache = GemFireCacheImpl.getInstance();
    DistributedSystem distributedSystem = gemFireCache.getDistributedSystem();
    Execution execution = FunctionService.onMember(distributedSystem.getDistributedMember());
    List<String> result = (List<String>) execution.execute(functionId).getResult();
    assertThat(result.get(0)).isEqualTo(version);
  }

  private static void assertThatCanLoad(String jarName, String className)
      throws ClassNotFoundException {
    assertThat(ClassPathLoader.getLatest().getJarDeployer().getDeployedJar(jarName)).isNotNull();
    assertThat(ClassPathLoader.getLatest().forName(className)).isNotNull();
  }

  private static class LoopingFunctionExecutor implements Serializable {
    private static final AtomicInteger COUNT_OF_EXECUTIONS = new AtomicInteger();
    private static final AtomicReference<Exception> EXCEPTION = new AtomicReference<>();
    private static final ExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadExecutor();

    public static void startExecuting(String functionId) {
      EXECUTOR_SERVICE.submit(() -> {
        GemFireCacheImpl gemFireCache = GemFireCacheImpl.getInstance();
        DistributedSystem distributedSystem = gemFireCache.getDistributedSystem();

        while (!Thread.currentThread().isInterrupted()) {
          try {
            COUNT_OF_EXECUTIONS.incrementAndGet();

            FunctionService.onMember(distributedSystem.getDistributedMember()).execute(functionId)
                .getResult();
          } catch (Exception e) {
            EXCEPTION.set(e);
          }
        }
      });
    }

    public static void waitForExecutions(int numberOfExecutions) {
      int initialCount = COUNT_OF_EXECUTIONS.get();
      int countToWaitFor = initialCount + numberOfExecutions;
      Callable<Boolean> doneWaiting = () -> COUNT_OF_EXECUTIONS.get() >= countToWaitFor;

      Awaitility.await().atMost(3, TimeUnit.MINUTES).until(doneWaiting);
    }

    public static void stopExecutionAndThrowAnyException() throws Exception {
      EXECUTOR_SERVICE.shutdownNow();
      Exception e = EXCEPTION.get();
      if (e != null) {
        throw e;
      }
    }


  }
}
