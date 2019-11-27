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

import org.apache.geode.test.compiler.ClassBuilder;

/**
 * Integration tests for {@link ClassPathLoader}.
 */
public class ClassPathLoaderJUnitTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @After
  public void resetClassPathLoader() {
    ClassPathLoader.setLatestToDefault();
  }

  @Test
  public void deployingJarsUnrelatedToDeserializedObjectsShouldNotCauseFailingInstanceOfChecks()
      throws Exception {
    String classAName = "classA";
    String classBName = "classB";

    File jarA = createVersionOfJar("JarAVersion11", classAName, "JarA.jar");
    File jarB = createVersionOfJar("JarBVersion11", classBName, "JarB.jar");

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarA.jar", jarA);
    Class class1 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = class1.newInstance();

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarB);
    Class class2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object anotherClassAInstance = class2.newInstance();

    assertThat(classAInstance).isInstanceOf(anotherClassAInstance.getClass());
  }

  @Test
  public void shouldBeAbleToLoadClassesWithInterdependenciesAcrossDifferentJars() throws Exception {
    String classAName = "classA";
    String classBName = "classB";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " extends " + classBName
            + " { public boolean hasResult() {return true;}}";
    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";

    File jarB = createJarFromClassContents("JarBVersion1", classBName, "JarB.jar", classBContents);
    File jarA = createJarFromClassContents("JarAVersion1", classAName, "JarA.jar", classAContents,
        jarB.getAbsolutePath());

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarA.jar", jarA);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarB);

    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    assertThat(classAName).isEqualTo(classAInstance.getClass().getSimpleName());
  }

  @Test
  public void loadingInterdependentJarsShouldNotCauseClassIncompatibilities() throws Exception {
    String classAName = "classA";
    String classBName = "classB";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " extends " + classBName
            + " { public boolean hasResult() {return true;}}";

    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";

    File jarB = createJarFromClassContents("JarBVersion1", classBName, "JarB.jar", classBContents);
    File jarA = createJarFromClassContents("JarAVersion1", classAName, "JarA.jar", classAContents,
        jarB.getAbsolutePath());

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarA.jar", jarA);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarB);

    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    assertThat(classBInstance.getClass()).isAssignableFrom(classAInstance.getClass());
  }

  @Test
  public void loadingParentClassFirstFromInterdependentJarsShouldNotCauseClassIncompatibilities()
      throws Exception {
    String classAName = "classA";
    String classBName = "classB";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " extends " + classBName
            + " { public boolean hasResult() {return true;}}";

    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";

    File jarB = createJarFromClassContents("JarBVersion1", classBName, "JarB.jar", classBContents);
    File jarA = createJarFromClassContents("JarAVersion1", classAName, "JarA.jar", classAContents,
        jarB.getAbsolutePath());

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarB);
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarA.jar", jarA);
    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    Class classB2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classB2Instance = classB2.newInstance();

    assertThat(classB2Instance).isInstanceOf(classBInstance.getClass());
    assertThat(classBInstance.getClass()).isAssignableFrom(classAInstance.getClass());
  }

  @Test
  public void redeploySubclassJarThatExtendsInterdependentJarShouldNowLoadNewSubclass()
      throws Exception {
    String classAName = "classA";
    String classBName = "classB";

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


    File jarB = createJarFromClassContents("JarBVersion1", classBName, "JarB.jar", classBContents);
    File jarA = createJarFromClassContents("JarAVersion1", classAName, "JarA.jar", classAContents,
        jarB.getAbsolutePath());
    File jarAV2 =
        createJarFromClassContents("JarAVersion2", classAName, "JarA.jar", classA2Contents,
            jarB.getAbsolutePath());

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarB);
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarA.jar", jarA);
    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarA.jar", jarAV2);
    Class classA2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classA2Instance = classA2.newInstance();

    assertThat(classA2Instance).isNotInstanceOf(classA);
  }

  @Test
  public void redeployingParentClassDoesNotCauseSubclassIncompatibilities() throws Exception {
    String classAName = "classA";
    String classBName = "classB";

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

    File jarB = createJarFromClassContents("JarBVersion1", classBName, "JarB.jar", classBContents);
    File jarA = createJarFromClassContents("JarAVersion1", classAName, "JarA.jar", classAContents,
        jarB.getAbsolutePath());
    File jarBV2 =
        createJarFromClassContents("JarBVersion2", classBName, "JarB.jar", classB2Contents);

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarB);
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarA.jar", jarA);
    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarBV2);

    Class classA2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classA2Instance = classA2.newInstance();

    assertThat(classA2Instance).isInstanceOf(classAInstance.getClass());
  }

  @Test
  public void redeployingParentClassIfParentDeployedLastDoesNotCauseSubclassIncompatibilities()
      throws Exception {
    String classAName = "classA";
    String classBName = "classB";

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

    File jarB = createJarFromClassContents("JarBVersion1", classBName, "JarB.jar", classBContents);
    File jarA = createJarFromClassContents("JarAVersion1", classAName, "JarA.jar", classAContents,
        jarB.getAbsolutePath());
    File jarBV2 =
        createJarFromClassContents("JarBVersion2", classBName, "JarB.jar", classB2Contents);

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarA.jar", jarA);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarB);

    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarBV2);
    Class classA2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classA2Instance = classA2.newInstance();

    assertThat(classA2Instance).isInstanceOf(classAInstance.getClass());
  }

  @Test(expected = ClassNotFoundException.class)
  public void redeployingJarWithRemovedClassShouldNoLongerAllowLoadingRemovedClass()
      throws Exception {
    String classAName = "classA";
    String classBName = "classB";
    String classB2Name = "classB2";

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

    File jarB = createJarFromClassContents("JarBVersion1", classBName, "JarB.jar", classBContents);
    File jarA = createJarFromClassContents("JarAVersion1", classAName, "JarA.jar", classAContents,
        jarB.getAbsolutePath());
    File jarBV2 =
        createJarFromClassContents("JarBVersion2", classB2Name, "JarB.jar", classB2Contents);

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarB);
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarBV2);
    Class classB2 =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
  }

  @Test
  public void undeployedUnrelatedJarShouldNotAffectDeserializedObjectComparison() throws Exception {
    String classAName = "classA";
    String classBName = "classB";

    String classAContents =
        "package jddunit.function;"
            + "public class "
            + classAName + " { public boolean hasResult() {return true;}}";

    String classBContents =
        "package jddunit.function;"
            + "public class "
            + classBName + " { public boolean someMethod() {return true;}}";

    File jarB = createJarFromClassContents("JarBVersion1", classBName, "JarB.jar", classBContents);
    File jarA = createJarFromClassContents("JarAVersion1", classAName, "JarA.jar", classAContents);

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarB);
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarA.jar", jarA);
    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    ClassPathLoader.getLatest().getJarDeployer().undeploy("JarB.jar");
    Object classA2Instance = classA.newInstance();
    assertThat(classA2Instance).isInstanceOf(classAInstance.getClass());
  }

  @Test(expected = ClassNotFoundException.class)
  public void undeployedJarShouldNoLongerAllowLoadingUndeployedClass() throws Exception {
    String classAName = "classA";
    String classBName = "classB";
    String classB2Name = "classB2";

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

    File jarB = createJarFromClassContents("JarBVersion1", classBName, "JarB.jar", classBContents);
    File jarA = createJarFromClassContents("JarAVersion1", classAName, "JarA.jar", classAContents,
        jarB.getAbsolutePath());

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarB.jar", jarB);
    Class classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    Object classBInstance = classB.newInstance();

    Class classA =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classAName);
    Object classAInstance = classA.newInstance();

    ClassPathLoader.getLatest().getJarDeployer().undeploy("JarB.jar");
    classB =
        ClassPathLoader.getLatest().asClassLoader().loadClass("jddunit.function." + classBName);
    classBInstance = classB.newInstance();

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
