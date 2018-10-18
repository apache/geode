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
package org.apache.geode.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * Integration tests for {@link ClassPathLoader}.
 */
public class ClassPathLoaderDeployTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public ServerStarterRule server = new ServerStarterRule();

  @Test
  public void testDeployWithExistingDependentJars() throws Exception {
    ClassBuilder classBuilder = new ClassBuilder();
    final File parentJarFile =
        new File("JarDeployerDUnitAParent.v1.jar");
    final File usesJarFile = new File("JarDeployerDUnitUses.v1.jar");
    final File functionJarFile =
        new File("JarDeployerDUnitFunction.v1.jar");

    // Write out a JAR files.
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("package jddunit.parent;");
    stringBuffer.append("public class JarDeployerDUnitParent {");
    stringBuffer.append("public String getValueParent() {");
    stringBuffer.append("return \"PARENT\";}}");

    byte[] jarBytes = classBuilder.createJarFromClassContent(
        "jddunit/parent/JarDeployerDUnitParent", stringBuffer.toString());
    FileOutputStream outStream = new FileOutputStream(parentJarFile);
    outStream.write(jarBytes);
    outStream.close();

    stringBuffer = new StringBuffer();
    stringBuffer.append("package jddunit.uses;");
    stringBuffer.append("public class JarDeployerDUnitUses {");
    stringBuffer.append("public String getValueUses() {");
    stringBuffer.append("return \"USES\";}}");

    jarBytes = classBuilder.createJarFromClassContent("jddunit/uses/JarDeployerDUnitUses",
        stringBuffer.toString());
    outStream = new FileOutputStream(usesJarFile);
    outStream.write(jarBytes);
    outStream.close();

    stringBuffer = new StringBuffer();
    stringBuffer.append("package jddunit.function;");
    stringBuffer.append("import jddunit.parent.JarDeployerDUnitParent;");
    stringBuffer.append("import jddunit.uses.JarDeployerDUnitUses;");
    stringBuffer.append("import org.apache.geode.cache.execute.Function;");
    stringBuffer.append("import org.apache.geode.cache.execute.FunctionContext;");
    stringBuffer.append(
        "public class JarDeployerDUnitFunction  extends JarDeployerDUnitParent implements Function {");
    stringBuffer.append("private JarDeployerDUnitUses uses = new JarDeployerDUnitUses();");
    stringBuffer.append("public boolean hasResult() {return true;}");
    stringBuffer.append(
        "public void execute(FunctionContext context) {context.getResultSender().lastResult(getValueParent() + \":\" + uses.getValueUses());}");
    stringBuffer.append("public String getId() {return \"JarDeployerDUnitFunction\";}");
    stringBuffer.append("public boolean optimizeForWrite() {return false;}");
    stringBuffer.append("public boolean isHA() {return false;}}");

    ClassBuilder functionClassBuilder = new ClassBuilder();
    functionClassBuilder.addToClassPath(parentJarFile.getAbsolutePath());
    functionClassBuilder.addToClassPath(usesJarFile.getAbsolutePath());
    jarBytes = functionClassBuilder.createJarFromClassContent(
        "jddunit/function/JarDeployerDUnitFunction", stringBuffer.toString());
    outStream = new FileOutputStream(functionJarFile);
    outStream.write(jarBytes);
    outStream.close();

    server.startServer();

    GemFireCacheImpl gemFireCache = GemFireCacheImpl.getInstance();
    DistributedSystem distributedSystem = gemFireCache.getDistributedSystem();
    Execution execution = FunctionService.onMember(distributedSystem.getDistributedMember());
    ResultCollector resultCollector = execution.execute("JarDeployerDUnitFunction");
    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) resultCollector.getResult();
    assertEquals("PARENT:USES", result.get(0));
  }

  @Test
  public void deployNewVersionOfFunctionOverOldVersion() throws Exception {
    File jarVersion1 = createVersionOfJar("Version1", "MyFunction", "MyJar.jar");
    File jarVersion2 = createVersionOfJar("Version2", "MyFunction", "MyJar.jar");

    server.startServer();

    GemFireCacheImpl gemFireCache = GemFireCacheImpl.getInstance();
    DistributedSystem distributedSystem = gemFireCache.getDistributedSystem();

    ClassPathLoader.getLatest().getJarDeployer().deploy("MyJar.jar", jarVersion1);

    assertThatClassCanBeLoaded("jddunit.function.MyFunction");
    Execution execution = FunctionService.onMember(distributedSystem.getDistributedMember());

    List<String> result = (List<String>) execution.execute("MyFunction").getResult();
    assertThat(result.get(0)).isEqualTo("Version1");

    ClassPathLoader.getLatest().getJarDeployer().deploy("MyJar.jar", jarVersion2);
    result = (List<String>) execution.execute("MyFunction").getResult();
    assertThat(result.get(0)).isEqualTo("Version2");
  }


  private File createVersionOfJar(String version, String functionName, String jarName)
      throws IOException {
    String classContents =
        "package jddunit.function;" + "import org.apache.geode.cache.execute.Function;"
            + "import org.apache.geode.cache.execute.FunctionContext;" + "public class "
            + functionName + " implements Function {" + "public boolean hasResult() {return true;}"
            + "public String getId() {return \"" + functionName + "\";}"
            + "public void execute(FunctionContext context) {context.getResultSender().lastResult(\""
            + version + "\");}}";

    File jar = new File(this.temporaryFolder.newFolder(version), jarName);
    ClassBuilder functionClassBuilder = new ClassBuilder();
    functionClassBuilder.writeJarFromContent("jddunit/function/" + functionName, classContents,
        jar);

    return jar;
  }

  private void assertThatClassCanBeLoaded(String className) throws ClassNotFoundException {
    assertThat(ClassPathLoader.getLatest().forName(className)).isNotNull();
  }
}
