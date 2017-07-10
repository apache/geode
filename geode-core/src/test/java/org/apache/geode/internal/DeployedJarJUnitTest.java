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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class DeployedJarJUnitTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private ClassBuilder classBuilder;

  @Before
  public void setup() throws Exception {
    File workingDir = temporaryFolder.newFolder();
    ClassPathLoader.setLatestToDefault(workingDir);
    classBuilder = new ClassBuilder();
  }

  @After
  public void tearDown() throws Exception {
    for (String functionName : FunctionService.getRegisteredFunctions().keySet()) {
      FunctionService.unregisterFunction(functionName);
    }

    ClassPathLoader.setLatestToDefault();
  }

  @Test
  public void testIsValidJarContent() throws IOException {
    assertThat(
        DeployedJar.hasValidJarContent(this.classBuilder.createJarFromName("JarClassLoaderJUnitA")))
            .isTrue();
  }

  @Test
  public void testIsInvalidJarContent() {
    assertThat(DeployedJar.hasValidJarContent("INVALID JAR CONTENT".getBytes())).isFalse();
  }

  @Test
  public void testClassOnClasspath() throws Exception {
    // Deploy the first JAR file and make sure the class is on the Classpath
    byte[] jarBytes =
        this.classBuilder.createJarFromClassContent("com/jcljunit/JarClassLoaderJUnitA",
            "package com.jcljunit; public class JarClassLoaderJUnitA {}");
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnit.jar", jarBytes);

    ClassPathLoader.getLatest().forName("com.jcljunit.JarClassLoaderJUnitA");

    // Update the JAR file and make sure the first class is no longer on the Classpath
    // and the second one is.
    jarBytes = this.classBuilder.createJarFromClassContent("com/jcljunit/JarClassLoaderJUnitB",
        "package com.jcljunit; public class JarClassLoaderJUnitB {}");
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnit.jar", jarBytes);

    ClassPathLoader.getLatest().forName("com.jcljunit.JarClassLoaderJUnitB");
    assertThatThrownBy(
        () -> ClassPathLoader.getLatest().forName("com.jcljunit.JarClassLoaderJUnitA"))
            .isInstanceOf(ClassNotFoundException.class);
  }

  @Test
  public void testFailingCompilation() throws Exception {
    String functionString = "import org.apache.geode.cache.Declarable;"
        + "import org.apache.geode.cache.execute.Function;"
        + "import org.apache.geode.cache.execute.FunctionContext;"
        + "public class JarClassLoaderJUnitFunction implements Function {}";

    assertThatThrownBy(() -> this.classBuilder
        .createJarFromClassContent("JarClassLoaderJUnitFunction", functionString)).isNotNull();
  }

  @Test
  public void testFunctions() throws Exception {
    // Test creating a JAR file with a function
    String functionString =
        "import java.util.Properties;" + "import org.apache.geode.cache.Declarable;"
            + "import org.apache.geode.cache.execute.Function;"
            + "import org.apache.geode.cache.execute.FunctionContext;"
            + "public class JarClassLoaderJUnitFunction implements Function {"
            + "public void init(Properties props) {}" + "public boolean hasResult() {return true;}"
            + "public void execute(FunctionContext context) {context.getResultSender().lastResult(\"GOODv1\");}"
            + "public String getId() {return \"JarClassLoaderJUnitFunction\";}"
            + "public boolean optimizeForWrite() {return false;}"
            + "public boolean isHA() {return false;}}";

    byte[] jarBytes =
        this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnit.jar", jarBytes);

    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertThat(function).isNotNull();
    TestResultSender resultSender = new TestResultSender();
    FunctionContext functionContext =
        new FunctionContextImpl(null, function.getId(), null, resultSender);
    function.execute(functionContext);
    assertThat(resultSender.getResults()).isEqualTo("GOODv1");

    // Test updating the function with a new JAR file
    functionString = functionString.replace("v1", "v2");
    jarBytes =
        this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnit.jar", jarBytes);

    function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertThat(function).isNotNull();
    resultSender = new TestResultSender();
    functionContext = new FunctionContextImpl(null, function.getId(), null, resultSender);
    function.execute(functionContext);
    assertThat(resultSender.getResults()).isEqualTo("GOODv2");

    // Test returning null for the Id
    String functionNullIdString =
        functionString.replace("return \"JarClassLoaderJUnitFunction\"", "return null");
    jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction",
        functionNullIdString);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnit.jar", jarBytes);

    assertThat(FunctionService.getFunction("JarClassLoaderJUnitFunction")).isNull();

    // Test removing the JAR
    ClassPathLoader.getLatest().getJarDeployer().undeploy("JarClassLoaderJUnit.jar");
    assertThat(FunctionService.getFunction("JarClassLoaderJUnitFunction")).isNull();
  }

  /**
   * Ensure that abstract functions aren't added to the Function Service.
   */
  @Test
  public void testAbstractFunction() throws Exception {
    // Add an abstract Function to the Classpath
    String functionString = "import org.apache.geode.cache.execute.Function;"
        + "public abstract class JarClassLoaderJUnitFunction implements Function {"
        + "public String getId() {return \"JarClassLoaderJUnitFunction\";}}";

    byte[] jarBytes =
        this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitFunction.jar",
        jarBytes);

    ClassPathLoader.getLatest().forName("JarClassLoaderJUnitFunction");

    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertThat(function).isNull();
  }

  @Test
  public void testDeclarableFunctionsWithNoCacheXml() throws Exception {
    final String jarName = "JarClassLoaderJUnitNoXml.jar";

    // Add a Declarable Function without parameters for the class to the Classpath
    String functionString =
        "import java.util.Properties;" + "import org.apache.geode.cache.Declarable;"
            + "import org.apache.geode.cache.execute.Function;"
            + "import org.apache.geode.cache.execute.FunctionContext;"
            + "public class JarClassLoaderJUnitFunctionNoXml implements Function, Declarable {"
            + "public String getId() {return \"JarClassLoaderJUnitFunctionNoXml\";}"
            + "public void init(Properties props) {}"
            + "public void execute(FunctionContext context) {context.getResultSender().lastResult(\"NOPARMSv1\");}"
            + "public boolean hasResult() {return true;}"
            + "public boolean optimizeForWrite() {return false;}"
            + "public boolean isHA() {return false;}}";

    byte[] jarBytes = this.classBuilder
        .createJarFromClassContent("JarClassLoaderJUnitFunctionNoXml", functionString);

    ClassPathLoader.getLatest().getJarDeployer().deploy(jarName, jarBytes);

    ClassPathLoader.getLatest().forName("JarClassLoaderJUnitFunctionNoXml");

    // Check to see if the function without parameters executes correctly
    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunctionNoXml");
    assertThat(function).isNotNull();
    TestResultSender resultSender = new TestResultSender();
    function.execute(new FunctionContextImpl(null, function.getId(), null, resultSender));
    assertThat((String) resultSender.getResults()).isEqualTo("NOPARMSv1");
  }

  @Test
  public void testDependencyBetweenJars() throws Exception {
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
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitParent.jar", jarBytes);

    stringBuffer = new StringBuffer();
    stringBuffer.append("package jcljunit.uses;");
    stringBuffer.append("public class JarClassLoaderJUnitUses {");
    stringBuffer.append("public String getValueUses() {");
    stringBuffer.append("return \"USES\";}}");

    jarBytes = this.classBuilder.createJarFromClassContent("jcljunit/uses/JarClassLoaderJUnitUses",
        stringBuffer.toString());
    writeJarBytesToFile(usesJarFile, jarBytes);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitUses.jar", jarBytes);

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

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitFunction.jar",
        jarBytes);

    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertThat(function).isNotNull();
    TestResultSender resultSender = new TestResultSender();
    FunctionContext functionContext =
        new FunctionContextImpl(null, function.getId(), null, resultSender);
    function.execute(functionContext);
    assertThat((String) resultSender.getResults()).isEqualTo("PARENT:USES");
  }

  @Test
  public void testFindResource() throws IOException, ClassNotFoundException {
    final String fileName = "file.txt";
    final String fileContent = "FILE CONTENT";

    byte[] jarBytes = this.classBuilder.createJarFromFileContent(fileName, fileContent);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitResource.jar",
        jarBytes);

    InputStream inputStream = ClassPathLoader.getLatest().getResourceAsStream(fileName);
    assertThat(inputStream).isNotNull();

    final byte[] fileBytes = new byte[fileContent.length()];
    inputStream.read(fileBytes);
    inputStream.close();
    assertThat(fileContent).isEqualTo(new String(fileBytes));
  }

  @Test
  public void testUpdateClassInJar() throws Exception {
    // First use of the JAR file
    byte[] jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitTestClass",
        "public class JarClassLoaderJUnitTestClass { public Integer getValue5() { return new Integer(5); } }");
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitUpdate.jar", jarBytes);

    Class<?> clazz = ClassPathLoader.getLatest().forName("JarClassLoaderJUnitTestClass");
    Object object = clazz.newInstance();
    Method getValue5Method = clazz.getMethod("getValue5");
    Integer value = (Integer) getValue5Method.invoke(object);
    assertThat(value).isEqualTo(5);

    // Now create an updated JAR file and make sure that the method from the new
    // class is available.
    jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitTestClass",
        "public class JarClassLoaderJUnitTestClass { public Integer getValue10() { return new Integer(10); } }");
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitUpdate.jar", jarBytes);

    clazz = ClassPathLoader.getLatest().forName("JarClassLoaderJUnitTestClass");
    object = clazz.newInstance();
    Method getValue10Method = clazz.getMethod("getValue10");
    value = (Integer) getValue10Method.invoke(object);
    assertThat(value).isEqualTo(10);
  }

  @Test
  public void testMultiThreadingDoesNotCauseDeadlock() throws Exception {
    // Add two JARs to the classpath
    byte[] jarBytes = this.classBuilder.createJarFromName("JarClassLoaderJUnitA");
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitA.jar", jarBytes);

    jarBytes = this.classBuilder.createJarFromClassContent("com/jcljunit/JarClassLoaderJUnitB",
        "package com.jcljunit; public class JarClassLoaderJUnitB {}");
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitB.jar", jarBytes);

    String[] classNames = new String[] {"JarClassLoaderJUnitA", "com.jcljunit.JarClassLoaderJUnitB",
        "NON-EXISTENT CLASS"};

    final int threadCount = 10;
    ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
    for (int i = 0; i < threadCount; i++) {
      executorService.submit(new ForNameExerciser(classNames));
    }

    executorService.shutdown();
    Awaitility.await().atMost(60, TimeUnit.SECONDS).until(executorService::isTerminated);

    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    long[] threadIds = threadMXBean.findDeadlockedThreads();

    if (threadIds != null) {
      StringBuilder deadLockTrace = new StringBuilder();
      for (long threadId : threadIds) {
        ThreadInfo threadInfo = threadMXBean.getThreadInfo(threadId, 100);
        deadLockTrace.append(threadInfo.getThreadName()).append("\n");
        for (StackTraceElement stackTraceElem : threadInfo.getStackTrace()) {
          deadLockTrace.append("\t").append(stackTraceElem).append("\n");
        }
      }
      System.out.println(deadLockTrace);
    }
    assertThat(threadIds).isNull();
  }

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.close();
  }

  private static class TestResultSender implements ResultSender<Object> {
    private Object result;

    public TestResultSender() {}

    protected Object getResults() {
      return this.result;
    }

    @Override
    public void lastResult(final Object lastResult) {
      this.result = lastResult;
    }

    @Override
    public void sendResult(final Object oneResult) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void sendException(final Throwable t) {
      throw new UnsupportedOperationException();
    }
  }

  static final Random random = new Random();

  private class ForNameExerciser implements Runnable {
    private final int numLoops = 1000;
    private final String[] classNames;

    ForNameExerciser(final String[] classNames) {
      this.classNames = classNames;
    }

    @Override
    public void run() {
      for (int i = 0; i < this.numLoops; i++) {
        try {
          // Random select a name from the list of class names and try to load it
          String className = this.classNames[random.nextInt(this.classNames.length)];
          ClassPathLoader.getLatest().forName(className);
        } catch (ClassNotFoundException expected) { // expected
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
  }
}
