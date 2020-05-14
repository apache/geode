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

import java.io.File;
import java.io.IOException;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.test.compiler.ClassBuilder;

/**
 * Integration tests for {@link ClassPathLoader}.
 */
public class ClassPathLoaderJUnitTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName name = new TestName();

  @After
  public void resetClassPathLoader() {
    ClassPathLoader.setLatestToDefault();
  }

  @Test
  public void deployingJarsUnrelatedToDeserializedObjectsShouldNotCauseFailingInstanceOfChecks()
      throws Exception {
    String testName = name.getMethodName();
    String classAName = testName + "classA";
    String classBName = testName + "classB";

    File jarA = createVersionOfJar(testName + "JarAVersion11", classAName, testName + "JarA.jar");
    File jarB = createVersionOfJar(testName + "JarBVersion11", classBName, testName + "JarB.jar");

    String jarAFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarA).getDeployedFileName();
    Class class1 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = class1.newInstance();

    String jarBFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarB).getDeployedFileName();
    Class class2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object anotherClassAInstance = class2.newInstance();

    assertThat(classAInstance).isInstanceOf(anotherClassAInstance.getClass());
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarAFileName);
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarBFileName);
  }

  @Test
  public void shouldBeAbleToLoadClassesWithInterdependenciesAcrossDifferentJars() throws Exception {
    String testName = name.getMethodName();
    String classAName = testName + "classA";
    String classBName = testName + "classB";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " extends " + classBName
            + " { public boolean hasResult() {return true;}}";
    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";

    File jarB = createJarFromClassContents(testName + "JarBVersion1", classBName,
        testName + "JarB.jar", classBContents);
    File jarA = createJarFromClassContents(testName + "JarAVersion1", classAName,
        testName + "JarA.jar", classAContents,
        jarB.getAbsolutePath());
    String jarAFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarA).getDeployedFileName();
    String jarBFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarB).getDeployedFileName();

    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    assertThat(classAName).isEqualTo(classAInstance.getClass().getSimpleName());
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarAFileName);
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarBFileName);
  }

  @Test
  public void loadingInterdependentJarsShouldNotCauseClassIncompatibilities() throws Exception {
    String testName = name.getMethodName();
    String classAName = testName + "classA";
    String classBName = testName + "classB";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " extends " + classBName
            + " { public boolean hasResult() {return true;}}";

    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";

    File jarB = createJarFromClassContents(testName + "JarBVersion1", classBName,
        testName + "JarB.jar", classBContents);
    File jarA = createJarFromClassContents(testName + "JarAVersion1", classAName,
        testName + "JarA.jar", classAContents,
        jarB.getAbsolutePath());

    String jarAFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarA).getDeployedFileName();
    String jarBFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarB).getDeployedFileName();

    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    assertThat(classBInstance.getClass()).isAssignableFrom(classAInstance.getClass());
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarAFileName);
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarBFileName);
  }

  @Test
  public void loadingParentClassFirstFromInterdependentJarsShouldNotCauseClassIncompatibilities()
      throws Exception {
    String testName = name.getMethodName();
    String classAName = testName + "classA";
    String classBName = testName + "classB";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " extends " + classBName
            + " { public boolean hasResult() {return true;}}";

    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";

    File jarB = createJarFromClassContents(testName + "JarBVersion1", classBName,
        testName + "JarB.jar", classBContents);
    File jarA = createJarFromClassContents(testName + "JarAVersion1", classAName,
        testName + "JarA.jar", classAContents,
        jarB.getAbsolutePath());

    String jarBFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarB).getDeployedFileName();
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    String jarAFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarA).getDeployedFileName();
    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    Class classB2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classB2Instance = classB2.newInstance();

    assertThat(classB2Instance).isInstanceOf(classBInstance.getClass());
    assertThat(classBInstance.getClass()).isAssignableFrom(classAInstance.getClass());
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarBFileName);
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarAFileName);
  }

  @Test
  public void redeploySubclassJarThatExtendsInterdependentJarShouldNowLoadNewSubclass()
      throws Exception {
    String testName = name.getMethodName();
    String classAName = testName + "classA";
    String classBName = testName + "classB";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " extends " + classBName
            + " { public boolean hasResult() {return true;}}";

    String classA2Contents =
        "package jddunit.function;"
            + "public class "
            + classAName + " extends " + classBName
            + " { public boolean hasResult() {return false;}}";

    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";


    File jarB = createJarFromClassContents(testName + "JarBVersion1", classBName,
        testName + "JarB.jar", classBContents);
    File jarA = createJarFromClassContents(testName + "JarAVersion1", classAName,
        testName + "JarA.jar", classAContents,
        jarB.getAbsolutePath());
    File jarAV2 =
        createJarFromClassContents(testName + "JarAVersion2", classAName, testName + "JarA.jar",
            classA2Contents,
            jarB.getAbsolutePath());

    String jarBFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarB).getDeployedFileName();
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);

    assertThat(ClassPathLoader.getLatest().getJarDeployer().deploy(jarA)).isNotNull();
    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    String jarAFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarAV2).getDeployedFileName();
    Class classA2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classA2Instance = classA2.newInstance();

    assertThat(classA2Instance).isNotInstanceOf(classA);
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarBFileName);
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarAFileName);
  }

  @Test
  public void redeployingParentClassDoesNotCauseSubclassIncompatibilities() throws Exception {
    String testName = name.getMethodName();
    String classAName = testName + "classA";
    String classBName = testName + "classB";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " extends " + classBName
            + " { public boolean hasResult() {return true;}}";

    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";

    String classB2Contents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return false;}}";

    File jarB = createJarFromClassContents(testName + "JarBVersion1", classBName,
        testName + "JarB.jar", classBContents);
    File jarA = createJarFromClassContents(testName + "JarAVersion1", classAName,
        testName + "JarA.jar", classAContents,
        jarB.getAbsolutePath());
    File jarBV2 =
        createJarFromClassContents(testName + "JarBVersion2", classBName, testName + "JarB.jar",
            classB2Contents);

    assertThat(ClassPathLoader.getLatest().getJarDeployer().deploy(jarB)).isNotNull();
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    String jarAFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarA).getDeployedFileName();
    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    String jarBv2FileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarBV2).getDeployedFileName();

    Class classA2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classA2Instance = classA2.newInstance();

    assertThat(classA2Instance).isInstanceOf(classAInstance.getClass());
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarAFileName);
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarBv2FileName);
  }

  @Test
  public void redeployingParentClassIfParentDeployedLastDoesNotCauseSubclassIncompatibilities()
      throws Exception {
    String testName = name.getMethodName();
    String classAName = testName + "classA";
    String classBName = testName + "classB";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " extends " + classBName
            + " { public boolean hasResult() {return true;}}";

    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";

    String classB2Contents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return false;}}";

    File jarB = createJarFromClassContents(testName + "JarBVersion1", classBName,
        testName + "JarB.jar", classBContents);
    File jarA = createJarFromClassContents(testName + "JarAVersion1", classAName,
        testName + "JarA.jar", classAContents,
        jarB.getAbsolutePath());
    File jarBV2 =
        createJarFromClassContents(testName + "JarBVersion2", classBName, testName + "JarB.jar",
            classB2Contents);

    String jarAFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarA).getDeployedFileName();
    assertThat(ClassPathLoader.getLatest().getJarDeployer().deploy(jarB)).isNotNull();

    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    String jarBFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarBV2).getDeployedFileName();
    Class classA2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classA2Instance = classA2.newInstance();

    assertThat(classA2Instance).isInstanceOf(classAInstance.getClass());
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarAFileName);
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarBFileName);
  }

  @Test
  public void redeployingJarWithRemovedClassShouldNoLongerAllowLoadingRemovedClass()
      throws Exception {
    String testName = name.getMethodName();
    String classAName = testName + "classA";
    String classBName = testName + "classB";
    String classB2Name = testName + "classB2";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " extends " + classBName
            + " { public boolean hasResult() {return true;}}";

    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";

    String classB2Contents =
        "package jddunit.function;"
            + "public class "
            + classB2Name + " { public boolean someMethod() {return false;}}";

    File jarB = createJarFromClassContents(testName + "JarBVersion1", classBName,
        testName + "JarB.jar", classBContents);
    File jarA = createJarFromClassContents(testName + "JarAVersion1", classAName,
        testName + "JarA.jar", classAContents,
        jarB.getAbsolutePath());
    File jarBV2 =
        createJarFromClassContents(testName + "JarBVersion2", classB2Name, testName + "JarB.jar",
            classB2Contents);

    String jarBFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarB).getDeployedFileName();
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    String jarBv2FileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarBV2).getDeployedFileName();

    try {
      Class classB2 =
          ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    } catch (ClassNotFoundException ex) {
      // Expected
    } finally {
      ClassPathLoader.getLatest().getJarDeployer().undeploy(jarBFileName);
    }
  }

  @Test
  public void undeployedUnrelatedJarShouldNotAffectDeserializedObjectComparison() throws Exception {
    String testName = name.getMethodName();
    String classAName = testName + "classA";
    String classBName = testName + "classB";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " { public boolean hasResult() {return true;}}";

    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";

    File jarB = createJarFromClassContents(testName + "JarBVersion1", classBName,
        testName + "JarB.jar", classBContents);
    File jarA = createJarFromClassContents(testName + "JarAVersion1", classAName,
        testName + "JarA.jar", classAContents);

    assertThat(ClassPathLoader.getLatest().getJarDeployer().deploy(jarB)).isNotNull();
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    String jarAFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarA).getDeployedFileName();
    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    ClassPathLoader.getLatest().getJarDeployer().undeploy(testName + "JarB.jar");
    Object classA2Instance = classA.newInstance();
    assertThat(classA2Instance).isInstanceOf(classAInstance.getClass());
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarAFileName);
  }

  @Test
  public void undeployedJarShouldNoLongerAllowLoadingUndeployedClass() throws Exception {
    String testName = name.getMethodName();
    String classAName = testName + "classA";
    String classBName = testName + "classB";
    String classB2Name = testName + "classB2";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " { public boolean hasResult() {return true;}}";

    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";

    String classB2Contents =
        "package jddunit.function;"
            + "public class "
            + classB2Name + " { public boolean someMethod() {return false;}}";

    File jarB = createJarFromClassContents(testName + "JarBVersion1", classBName,
        testName + "JarB.jar", classBContents);
    File jarA = createJarFromClassContents(testName + "JarAVersion1", classAName,
        testName + "JarA.jar", classAContents,
        jarB.getAbsolutePath());

    assertThat(ClassPathLoader.getLatest().getJarDeployer().deploy(jarB)).isNotNull();
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    String jarAFileName =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarA).getDeployedFileName();

    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    ClassPathLoader.getLatest().getJarDeployer().undeploy(testName + "JarB.jar");
    try {
      classB =
          ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
      classBInstance = classB.newInstance();
    } catch (ClassNotFoundException ex) {
      // expected
    } finally {
      ClassPathLoader.getLatest().getJarDeployer().undeploy(jarAFileName);
    }

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

    return createJarFromClassContents(version, functionName, jarName, classContents);
  }

  private File createJarFromClassContents(String version, String functionName, String jarName,
      String classContents)
      throws IOException {

    File jar = new File(this.temporaryFolder.newFolder(version), jarName);
    ClassBuilder classBuilder = new ClassBuilder();
    classBuilder.writeJarFromContent("jddunit/function/" + functionName, classContents,
        jar);

    return jar;
  }

  private File createJarFromClassContents(String version, String functionName, String jarName,
      String classContents, String additionalClassPath)
      throws IOException {

    File jar = new File(this.temporaryFolder.newFolder(version), jarName);
    ClassBuilder classBuilder = new ClassBuilder();
    classBuilder.addToClassPath(additionalClassPath);
    classBuilder.writeJarFromContent("jddunit/function/" + functionName, classContents,
        jar);

    return jar;
  }

  private void assertThatClassCanBeLoaded(String className) throws ClassNotFoundException {
    assertThat(ClassPathLoader.getLatest().forName(className)).isNotNull();
  }
}
