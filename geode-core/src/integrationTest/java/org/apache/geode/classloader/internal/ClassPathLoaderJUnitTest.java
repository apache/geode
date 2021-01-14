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
package org.apache.geode.classloader.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.time.Instant;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.test.compiler.ClassBuilder;

/** Integration tests for {@link org.apache.geode.classloader.internal.ClassPathLoader}. */
public class ClassPathLoaderJUnitTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public TestName name = new TestName();

  @After
  public void resetClassPathLoader() {
    ClassPathLoader.setLatestToDefault(null);
  }

  @Test
  public void deployingJarsUnrelatedToDeserializedObjectsShouldNotCauseFailingInstanceOfChecks()
      throws Exception {
    String testName = name.getMethodName();
    String classAName = testName + "classA";
    String classBName = testName + "classB";

    File jarA = createVersionOfJar(testName + "JarAVersion11", classAName, testName + "JarA.jar");
    File jarB = createVersionOfJar(testName + "JarBVersion11", classBName, testName + "JarB.jar");

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarA));
    Class class1 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = class1.newInstance();

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarB));
    Class class2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object anotherClassAInstance = class2.newInstance();

    assertThat(classAInstance).isInstanceOf(anotherClassAInstance.getClass());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarA.getName());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarB.getName());
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
    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarA));
    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarB));

    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    assertThat(classAName).isEqualTo(classAInstance.getClass().getSimpleName());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarA.getName());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarB.getName());
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

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarA));
    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarB));

    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    assertThat(classBInstance.getClass()).isAssignableFrom(classAInstance.getClass());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarA.getName());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarB.getName());
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

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarB));
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarA));
    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    Class classB2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classB2Instance = classB2.newInstance();

    assertThat(classB2Instance).isInstanceOf(classBInstance.getClass());
    assertThat(classBInstance.getClass()).isAssignableFrom(classAInstance.getClass());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarB.getName());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarA.getName());
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

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarB));
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);

    assertThat(
        ClassPathLoader.getLatest().getJarDeploymentService()
            .deploy(createDeploymentFromJar(jarA)))
                .isNotNull();
    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarAV2));
    Class classA2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classA2Instance = classA2.newInstance();

    assertThat(classA2Instance).isNotInstanceOf(classA);
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarB.getName());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarA.getName());
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

    assertThat(
        ClassPathLoader.getLatest().getJarDeploymentService()
            .deploy(createDeploymentFromJar(jarB)))
                .isNotNull();
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarA));
    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarBV2));

    Class classA2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classA2Instance = classA2.newInstance();

    assertThat(classA2Instance).isInstanceOf(classAInstance.getClass());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarA.getName());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarBV2.getName());
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

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarA));
    assertThat(
        ClassPathLoader.getLatest().getJarDeploymentService()
            .deploy(createDeploymentFromJar(jarB)))
                .isNotNull();

    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarBV2));
    Class classA2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classA2Instance = classA2.newInstance();

    assertThat(classA2Instance).isInstanceOf(classAInstance.getClass());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarA.getName());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarB.getName());
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

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarB));
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarBV2));

    try {
      Class classB2 =
          ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    } catch (ClassNotFoundException ex) {
      // Expected
    } finally {
      ClassPathLoader.getLatest().getJarDeploymentService()
          .undeployByFileName(jarB.getName());
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

    assertThat(
        ClassPathLoader.getLatest().getJarDeploymentService()
            .deploy(createDeploymentFromJar(jarB)))
                .isNotNull();
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarA));
    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(testName + "JarB.jar");
    Object classA2Instance = classA.newInstance();
    assertThat(classA2Instance).isInstanceOf(classAInstance.getClass());
    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(jarA.getName());
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

    assertThat(ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarB)))
            .isNotNull();
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    ClassPathLoader.getLatest().getJarDeploymentService()
        .deploy(createDeploymentFromJar(jarA));

    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    ClassPathLoader.getLatest().getJarDeploymentService()
        .undeployByFileName(testName + "JarB.jar");
    try {
      classB =
          ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
      classBInstance = classB.newInstance();
    } catch (ClassNotFoundException ex) {
      // expected
    } finally {
      ClassPathLoader.getLatest().getJarDeploymentService()
          .undeployByFileName(jarA.getName());
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

  private Deployment createDeploymentFromJar(File jar) {
    Deployment deployment = new Deployment(jar.getName(), "test", Instant.now().toString());
    deployment.setFile(jar);
    return deployment;
  }
}
