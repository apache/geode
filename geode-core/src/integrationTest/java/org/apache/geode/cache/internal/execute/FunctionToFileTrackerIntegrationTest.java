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

package org.apache.geode.cache.internal.execute;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.deployment.DeploymentServiceFactory;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.test.compiler.ClassBuilder;

public class FunctionToFileTrackerIntegrationTest {

  private ClassBuilder classBuilder = new ClassBuilder();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {}

  @Test
  public void registerFunctions() throws IOException {
    File functionJar = registerFunctionJar();

    Deployment deployment = createDeploymentFromJar(functionJar);
    DeploymentServiceFactory.getJarDeploymentServiceInstance().deploy(deployment);

    Set<String> registeredFunctions = FunctionService.getRegisteredFunctions().keySet();
    assertThat(registeredFunctions.size()).isEqualTo(1);
    assertThat(registeredFunctions).containsExactly("JarClassLoaderJUnitFunction");
  }

  private File registerFunctionJar() throws IOException {
    final File parentJarFile = temporaryFolder.newFile("JarClassLoaderJUnitParent.jar");
    final File usesJarFile = temporaryFolder.newFile("JarClassLoaderJUnitUses.jar");

    // Write out a JAR files.
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("package jcljunit.parent;");
    stringBuffer.append("public class JarClassLoaderJUnitParent {");
    stringBuffer.append("public String getValueParent() {");
    stringBuffer.append("return \"PARENT\";}}");

    byte[] jarBytes = this.classBuilder.createJarFromClassContent(
        "jcljunit/parent/JarClassLoaderJUnitParent", stringBuffer.toString());
    writeJarBytesToFile(parentJarFile, jarBytes);
    Deployment parentDeployment = createDeploymentFromJar(parentJarFile);
    DeploymentServiceFactory.getJarDeploymentServiceInstance().deploy(parentDeployment);

    stringBuffer = new StringBuffer();
    stringBuffer.append("package jcljunit.uses;");
    stringBuffer.append("public class JarClassLoaderJUnitUses {");
    stringBuffer.append("public String getValueUses() {");
    stringBuffer.append("return \"USES\";}}");

    jarBytes = this.classBuilder.createJarFromClassContent("jcljunit/uses/JarClassLoaderJUnitUses",
        stringBuffer.toString());
    writeJarBytesToFile(usesJarFile, jarBytes);
    Deployment userDeployment = createDeploymentFromJar(usesJarFile);
    DeploymentServiceFactory.getJarDeploymentServiceInstance().deploy(userDeployment);

    stringBuffer = new StringBuffer();
    stringBuffer.append("package jcljunit.function;");
    stringBuffer.append("import jcljunit.parent.JarClassLoaderJUnitParent;");
    stringBuffer.append("import jcljunit.uses.JarClassLoaderJUnitUses;");
    stringBuffer.append("import org.apache.geode.cache.execute.Function;");
    stringBuffer.append("import org.apache.geode.cache.execute.FunctionContext;");
    stringBuffer.append(
        "public class JarClassLoaderJUnitFunction  extends JarClassLoaderJUnitParent implements Function {");
    stringBuffer.append("private JarClassLoaderJUnitUses uses = new JarClassLoaderJUnitUses();");
    stringBuffer.append("public boolean hasResult() {return true;}");
    stringBuffer.append(
        "public void execute(FunctionContext context) {context.getResultSender().lastResult(getValueParent() + \":\" + uses.getValueUses());}");
    stringBuffer.append("public String getId() {return \"JarClassLoaderJUnitFunction\";}");
    stringBuffer.append("public boolean optimizeForWrite() {return false;}");
    stringBuffer.append("public boolean isHA() {return false;}}");

    ClassBuilder functionClassBuilder = new ClassBuilder();
    functionClassBuilder.addToClassPath(parentJarFile.getAbsolutePath());
    functionClassBuilder.addToClassPath(usesJarFile.getAbsolutePath());
    jarBytes = functionClassBuilder.createJarFromClassContent(
        "jcljunit/function/JarClassLoaderJUnitFunction", stringBuffer.toString());
    File functionJar = temporaryFolder.newFile("JarClassLoaderJUnitFunction.jar");
    writeJarBytesToFile(functionJar, jarBytes);
    return functionJar;
  }

  @Test
  public void unregisterFunctionsForDeployment() throws IOException {
    File functionJar = registerFunctionJar();

    Deployment deployment = createDeploymentFromJar(functionJar);
    DeploymentServiceFactory.getJarDeploymentServiceInstance().deploy(deployment);

    Set<String> registeredFunctions = FunctionService.getRegisteredFunctions().keySet();
    assertThat(registeredFunctions.size()).isEqualTo(1);
    assertThat(registeredFunctions).containsExactly("JarClassLoaderJUnitFunction");

    DeploymentServiceFactory.getJarDeploymentServiceInstance()
        .undeployByDeploymentName(deployment.getDeploymentName());
    registeredFunctions = FunctionService.getRegisteredFunctions().keySet();
    assertThat(registeredFunctions.size()).isEqualTo(0);
  }

  private Deployment createDeploymentFromJar(File jar) {
    Deployment deployment = new Deployment(jar.getName(), "test", Instant.now().toString());
    deployment.setFile(jar);
    return deployment;
  }

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.close();
  }
}
