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

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.Locator;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.dunit.rules.Server;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

@Category(DistributedTest.class)
public class DeployCommandRedeployDUnitTest implements Serializable {
  private static final String VERSION1 = "Version1";
  private static final String VERSION2 = "Version2";

  private static final String jarNameA = "DeployCommandRedeployDUnitTestA.jar";
  private static final String functionA = "DeployCommandRedeployDUnitFunctionA";
  private File jarAVersion1;
  private File jarAVersion2;

  private static final String jarNameB = "DeployCommandRedeployDUnitTestB.jar";
  private static final String functionB = "DeployCommandRedeployDUnitFunctionB";
  private static final String packageB = "jddunit.function";
  private static final String fullyQualifiedFunctionB = packageB + "." + functionB;
  private File jarBVersion1;
  private File jarBVersion2;

  private MemberVM locator;
  private MemberVM server;

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();

  @Rule
  public transient GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();

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
    gfshConnector.executeAndVerifyCommand("deploy --jar=" + jarAVersion1.getCanonicalPath());
    server.invoke(() -> assertThatCanLoad(jarNameA, functionA));
    server.invoke(() -> assertThatFunctionHasVersion(functionA, VERSION1));


    gfshConnector.executeAndVerifyCommand("deploy --jar=" + jarBVersion1.getCanonicalPath());
    server.invoke(() -> assertThatCanLoad(jarNameA, functionA));
    server.invoke(() -> assertThatCanLoad(jarNameB, fullyQualifiedFunctionB));
    server.invoke(() -> assertThatFunctionHasVersion(functionA, VERSION1));
    server.invoke(() -> assertThatFunctionHasVersion(functionB, VERSION1));

    gfshConnector.executeAndVerifyCommand("deploy --jar=" + jarBVersion2.getCanonicalPath());
    server.invoke(() -> assertThatCanLoad(jarNameA, functionA));
    server.invoke(() -> assertThatCanLoad(jarNameB, fullyQualifiedFunctionB));
    server.invoke(() -> assertThatFunctionHasVersion(functionA, VERSION1));
    server.invoke(() -> assertThatFunctionHasVersion(functionB, VERSION2));

    gfshConnector.executeAndVerifyCommand("deploy --jar=" + jarAVersion2.getCanonicalPath());
    server.invoke(() -> assertThatCanLoad(jarNameA, functionA));
    server.invoke(() -> assertThatCanLoad(jarNameB, fullyQualifiedFunctionB));
    server.invoke(() -> assertThatFunctionHasVersion(functionA, VERSION2));
    server.invoke(() -> assertThatFunctionHasVersion(functionB, VERSION2));
  }

  // Note that jar A is a Declarable Function, while jar B is only a Function.
  // Also, the function for jar A resides in the default package, whereas jar B specifies a package.
  // This ensures that this test has identical coverage to some tests that it replaced.
  private File createJarWithFunctionA(String version) throws Exception {
    String classContents =
        "import java.util.Properties;" + "import org.apache.geode.cache.Declarable;"
            + "import org.apache.geode.cache.execute.Function;"
            + "import org.apache.geode.cache.execute.FunctionContext;" + "public class " + functionA
            + " implements Function, Declarable {" + "public String getId() {return \"" + functionA
            + "\";}" + "public void init(Properties props) {}"
            + "public void execute(FunctionContext context) {context.getResultSender().lastResult(\""
            + version + "\");}" + "public boolean hasResult() {return true;}"
            + "public boolean optimizeForWrite() {return false;}"
            + "public boolean isHA() {return false;}}";

    File jar = new File(lsRule.getTempFolder().newFolder(jarNameA + version), this.jarNameA);
    ClassBuilder functionClassBuilder = new ClassBuilder();
    functionClassBuilder.writeJarFromContent(functionA, classContents, jar);

    return jar;
  }

  private File createJarWithFunctionB(String version) throws IOException {
    String classContents =
        "package " + packageB + ";" + "import org.apache.geode.cache.execute.Function;"
            + "import org.apache.geode.cache.execute.FunctionContext;" + "public class " + functionB
            + " implements Function {" + "public boolean hasResult() {return true;}"
            + "public void execute(FunctionContext context) {context.getResultSender().lastResult(\""
            + version + "\");}" + "public String getId() {return \"" + functionB + "\";}"
            + "public boolean optimizeForWrite() {return false;}"
            + "public boolean isHA() {return false;}}";

    File jar = new File(lsRule.getTempFolder().newFolder(jarNameB + version), this.jarNameB);
    ClassBuilder functionClassBuilder = new ClassBuilder();
    functionClassBuilder.writeJarFromContent("jddunit/function/" + functionB, classContents, jar);

    return jar;
  }

  private void assertThatFunctionHasVersion(String functionId, String version) {
    GemFireCacheImpl gemFireCache = GemFireCacheImpl.getInstance();
    DistributedSystem distributedSystem = gemFireCache.getDistributedSystem();
    Execution execution =
        FunctionService.onMember(distributedSystem, distributedSystem.getDistributedMember());
    List<String> result = (List<String>) execution.execute(functionId).getResult();
    assertThat(result.get(0)).isEqualTo(version);
  }

  private void assertThatCanLoad(String jarName, String className) throws ClassNotFoundException {
    assertThat(ClassPathLoader.getLatest().getJarDeployer().findDeployedJar(jarName)).isNotNull();
    assertThat(ClassPathLoader.getLatest().forName(className)).isNotNull();
  }
}
