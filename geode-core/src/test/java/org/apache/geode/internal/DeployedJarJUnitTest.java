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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.test.junit.categories.IntegrationTest;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * TODO: Need to fix this testDeclarableFunctionsWithParms and testClassOnClasspath on Windows:
 */
@Category(IntegrationTest.class)
public class DeployedJarJUnitTest {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private final ClassBuilder classBuilder = new ClassBuilder();

  @Before
  public void setup() throws Exception {
    File workingDir = temporaryFolder.newFolder();

    ClassPathLoader.setLatestToDefault(workingDir);
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
    assertTrue(
        DeployedJar.isValidJarContent(this.classBuilder.createJarFromName("JarClassLoaderJUnitA")));
  }

  @Test
  public void testIsInvalidJarContent() {
    assertFalse(DeployedJar.isValidJarContent("INVALID JAR CONTENT".getBytes()));
  }

  @Test
  public void testClassOnClasspath() throws Exception {
    // Deploy the first JAR file and make sure the class is on the Classpath
    byte[] jarBytes =
        this.classBuilder.createJarFromClassContent("com/jcljunit/JarClassLoaderJUnitA",
            "package com.jcljunit; public class JarClassLoaderJUnitA {}");
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnit.jar", jarBytes);

    try {
      ClassPathLoader.getLatest().forName("com.jcljunit.JarClassLoaderJUnitA");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    // Update the JAR file and make sure the first class is no longer on the Classpath
    // and the second one is.
    jarBytes = this.classBuilder.createJarFromClassContent("com/jcljunit/JarClassLoaderJUnitB",
        "package com.jcljunit; public class JarClassLoaderJUnitB {}");
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnit.jar", jarBytes);

    try {
      ClassPathLoader.getLatest().forName("com.jcljunit.JarClassLoaderJUnitB");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    try {
      ClassPathLoader.getLatest().forName("com.jcljunit.JarClassLoaderJUnitA");
      fail("Class should not be found on Classpath");
    } catch (ClassNotFoundException expected) { // expected
    }

  }

  @Test
  public void testFailingCompilation() throws Exception {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("import org.apache.geode.cache.Declarable;");
    stringBuffer.append("import org.apache.geode.cache.execute.Function;");
    stringBuffer.append("import org.apache.geode.cache.execute.FunctionContext;");
    stringBuffer.append("public class JarClassLoaderJUnitFunction implements Function {}");
    String functionString = stringBuffer.toString();

    try {
      this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
      fail("This code should have failed to compile and thrown an exception");
    } catch (Exception ex) {
      // All good
    }
  }

  @Test
  public void testFunctions() throws IOException, ClassNotFoundException {
    // Test creating a JAR file with a function
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("import java.util.Properties;");
    stringBuffer.append("import org.apache.geode.cache.Declarable;");
    stringBuffer.append("import org.apache.geode.cache.execute.Function;");
    stringBuffer.append("import org.apache.geode.cache.execute.FunctionContext;");
    stringBuffer.append("public class JarClassLoaderJUnitFunction implements Function {");
    stringBuffer.append("public void init(Properties props) {}");
    stringBuffer.append("public boolean hasResult() {return true;}");
    stringBuffer.append(
        "public void execute(FunctionContext context) {context.getResultSender().lastResult(\"GOODv1\");}");
    stringBuffer.append("public String getId() {return \"JarClassLoaderJUnitFunction\";}");
    stringBuffer.append("public boolean optimizeForWrite() {return false;}");
    stringBuffer.append("public boolean isHA() {return false;}}");
    String functionString = stringBuffer.toString();

    byte[] jarBytes =
        this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnit.jar", jarBytes);

    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertNotNull(function);
    TestResultSender resultSender = new TestResultSender();
    FunctionContext functionContext = new FunctionContextImpl(function.getId(), null, resultSender);
    function.execute(functionContext);
    assertEquals("GOODv1", (String) resultSender.getResults());

    // Test updating the function with a new JAR file
    functionString = functionString.replace("v1", "v2");
    jarBytes =
        this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnit.jar", jarBytes);

    function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertNotNull(function);
    resultSender = new TestResultSender();
    functionContext = new FunctionContextImpl(function.getId(), null, resultSender);
    function.execute(functionContext);
    assertEquals("GOODv2", (String) resultSender.getResults());

    // Test returning null for the Id
    String functionNullIdString =
        functionString.replace("return \"JarClassLoaderJUnitFunction\"", "return null");
    jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction",
        functionNullIdString);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnit.jar", jarBytes);

    assertNull(FunctionService.getFunction("JarClassLoaderJUnitFunction"));

    // Test removing the JAR
    ClassPathLoader.getLatest().getJarDeployer().undeploy("JarClassLoaderJUnit.jar");
    assertNull(FunctionService.getFunction("JarClassLoaderJUnitFunction"));
  }

  /**
   * Ensure that abstract functions aren't added to the Function Service.
   */
  @Test
  public void testAbstractFunction() throws IOException, ClassNotFoundException {
    // Add an abstract Function to the Classpath
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("import org.apache.geode.cache.execute.Function;");
    stringBuffer.append("public abstract class JarClassLoaderJUnitFunction implements Function {");
    stringBuffer.append("public String getId() {return \"JarClassLoaderJUnitFunction\";}}");
    String functionString = stringBuffer.toString();

    byte[] jarBytes =
        this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitFunction.jar",
        jarBytes);

    try {
      ClassPathLoader.getLatest().forName("JarClassLoaderJUnitFunction");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertNull(function);
  }

  @Test
  public void testDeclarableFunctionsWithNoCacheXml() throws Exception {

    final String jarName = "JarClassLoaderJUnitNoXml.jar";

    // Add a Declarable Function without parameters for the class to the Classpath
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("import java.util.Properties;");
    stringBuffer.append("import org.apache.geode.cache.Declarable;");
    stringBuffer.append("import org.apache.geode.cache.execute.Function;");
    stringBuffer.append("import org.apache.geode.cache.execute.FunctionContext;");
    stringBuffer
        .append("public class JarClassLoaderJUnitFunctionNoXml implements Function, Declarable {");
    stringBuffer.append("public String getId() {return \"JarClassLoaderJUnitFunctionNoXml\";}");
    stringBuffer.append("public void init(Properties props) {}");
    stringBuffer.append(
        "public void execute(FunctionContext context) {context.getResultSender().lastResult(\"NOPARMSv1\");}");
    stringBuffer.append("public boolean hasResult() {return true;}");
    stringBuffer.append("public boolean optimizeForWrite() {return false;}");
    stringBuffer.append("public boolean isHA() {return false;}}");
    String functionString = stringBuffer.toString();

    byte[] jarBytes = this.classBuilder
        .createJarFromClassContent("JarClassLoaderJUnitFunctionNoXml", functionString);

    ClassPathLoader.getLatest().getJarDeployer().deploy(jarName, jarBytes);

    try {
      ClassPathLoader.getLatest().forName("JarClassLoaderJUnitFunctionNoXml");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    // Check to see if the function without parameters executes correctly
    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunctionNoXml");
    assertNotNull(function);
    TestResultSender resultSender = new TestResultSender();
    function.execute(new FunctionContextImpl(function.getId(), null, resultSender));
    assertEquals("NOPARMSv1", (String) resultSender.getResults());
  }

  @Test
  public void testDependencyBetweenJars() throws IOException, ClassNotFoundException {
    final File parentJarFile = temporaryFolder.newFile("JarClassLoaderJUnitParent.jar");
    final File usesJarFile = temporaryFolder.newFile("JarClassLoaderJUnitUses.jar");

    JarDeployer jarDeployer = ClassPathLoader.getLatest().getJarDeployer();

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
    assertNotNull(function);
    TestResultSender resultSender = new TestResultSender();
    FunctionContext functionContext = new FunctionContextImpl(function.getId(), null, resultSender);
    function.execute(functionContext);
    assertEquals("PARENT:USES", (String) resultSender.getResults());
  }

  @Test
  public void testFindResource() throws IOException, ClassNotFoundException {
    final String fileName = "file.txt";
    final String fileContent = "FILE CONTENT";

    byte[] jarBytes = this.classBuilder.createJarFromFileContent(fileName, fileContent);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitResource.jar",
        jarBytes);

    InputStream inputStream = ClassPathLoader.getLatest().getResourceAsStream(fileName);
    assertNotNull(inputStream);

    final byte[] fileBytes = new byte[fileContent.length()];
    inputStream.read(fileBytes);
    inputStream.close();
    assertTrue(fileContent.equals(new String(fileBytes)));
  }

  @Test
  public void testUpdateClassInJar() throws IOException, ClassNotFoundException {
    // First use of the JAR file
    byte[] jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitTestClass",
        "public class JarClassLoaderJUnitTestClass { public Integer getValue5() { return new Integer(5); } }");
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitUpdate.jar", jarBytes);

    try {
      Class<?> clazz = ClassPathLoader.getLatest().forName("JarClassLoaderJUnitTestClass");
      Object object = clazz.newInstance();
      Method getValue5Method = clazz.getMethod("getValue5", new Class[] {});
      Integer value = (Integer) getValue5Method.invoke(object, new Object[] {});
      assertEquals(value.intValue(), 5);

    } catch (InvocationTargetException itex) {
      fail("JAR file not correctly added to Classpath" + itex);
    } catch (NoSuchMethodException nsmex) {
      fail("JAR file not correctly added to Classpath" + nsmex);
    } catch (InstantiationException iex) {
      fail("JAR file not correctly added to Classpath" + iex);
    } catch (IllegalAccessException iaex) {
      fail("JAR file not correctly added to Classpath" + iaex);
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath" + cnfex);
    }

    // Now create an updated JAR file and make sure that the method from the new
    // class is available.
    jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitTestClass",
        "public class JarClassLoaderJUnitTestClass { public Integer getValue10() { return new Integer(10); } }");
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitUpdate.jar", jarBytes);


    try {
      Class<?> clazz = ClassPathLoader.getLatest().forName("JarClassLoaderJUnitTestClass");
      Object object = clazz.newInstance();
      Method getValue10Method = clazz.getMethod("getValue10", new Class[] {});
      Integer value = (Integer) getValue10Method.invoke(object, new Object[] {});
      assertEquals(value.intValue(), 10);

    } catch (InvocationTargetException itex) {
      fail("JAR file not correctly added to Classpath" + itex);
    } catch (NoSuchMethodException nsmex) {
      fail("JAR file not correctly added to Classpath" + nsmex);
    } catch (InstantiationException iex) {
      fail("JAR file not correctly added to Classpath" + iex);
    } catch (IllegalAccessException iaex) {
      fail("JAR file not correctly added to Classpath" + iaex);
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath" + cnfex);
    }
  }

  @Test
  public void testMultiThread() throws IOException, ClassNotFoundException {
    // Add two JARs to the classpath
    byte[] jarBytes = this.classBuilder.createJarFromName("JarClassLoaderJUnitA");
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitA.jar", jarBytes);

    jarBytes = this.classBuilder.createJarFromClassContent("com/jcljunit/JarClassLoaderJUnitB",
        "package com.jcljunit; public class JarClassLoaderJUnitB {}");
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitB.jar", jarBytes);

    String[] classNames = new String[] {"JarClassLoaderJUnitA", "com.jcljunit.JarClassLoaderJUnitB",
        "NON-EXISTENT CLASS"};

    // Spawn some threads which try to instantiate these classes
    final int threadCount = 10;
    final int numLoops = 1000;
    final CyclicBarrier cyclicBarrier = new CyclicBarrier(threadCount + 1);
    for (int i = 0; i < threadCount; i++) {
      new ForNameExerciser(cyclicBarrier, numLoops, classNames).start();
    }

    // Wait for all of the threads to be ready
    try {
      cyclicBarrier.await();
    } catch (InterruptedException iex) {
      fail("Interrupted while waiting for barrier");
    } catch (BrokenBarrierException bbex) {
      fail("Broken barrier while waiting");
    }

    // Loop while each thread tries N times to instantiate a non-existent class
    for (int i = 0; i < numLoops; i++) {
      try {
        cyclicBarrier.await(5, TimeUnit.SECONDS);
      } catch (InterruptedException iex) {
        fail("Interrupted while waiting for barrier");
      } catch (TimeoutException tex) {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadMXBean.findDeadlockedThreads();

        if (threadIds != null) {
          StringBuffer deadLockTrace = new StringBuffer();
          for (long threadId : threadIds) {
            ThreadInfo threadInfo = threadMXBean.getThreadInfo(threadId, 100);
            deadLockTrace.append(threadInfo.getThreadName()).append("\n");
            for (StackTraceElement stackTraceElem : threadInfo.getStackTrace()) {
              deadLockTrace.append("\t").append(stackTraceElem).append("\n");
            }
          }

          fail("Deadlock with trace:\n" + deadLockTrace.toString());
        }

        fail("Timeout while waiting for barrier - no deadlock detected");
      } catch (BrokenBarrierException bbex) {
        fail("Broken barrier while waiting");
      }
    }
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

  private class ForNameExerciser extends Thread {
    private final CyclicBarrier cyclicBarrier;
    private final int numLoops;
    private final String[] classNames;

    ForNameExerciser(final CyclicBarrier cyclicBarrier, final int numLoops,
        final String[] classNames) {
      this.cyclicBarrier = cyclicBarrier;
      this.numLoops = numLoops;
      this.classNames = classNames;
    }

    @Override
    public void run() {
      try {
        this.cyclicBarrier.await();
      } catch (InterruptedException iex) {
        fail("Interrupted while waiting for latch");
      } catch (BrokenBarrierException bbex) {
        fail("Broken barrier while waiting");
      }
      for (int i = 0; i < this.numLoops; i++) {
        try {
          // Random select a name from the list of class names and try to load it
          String className = this.classNames[random.nextInt(this.classNames.length)];
          ClassPathLoader.getLatest().forName(className);
        } catch (ClassNotFoundException expected) { // expected
        }
        try {
          this.cyclicBarrier.await();
        } catch (InterruptedException iex) {
          fail("Interrupted while waiting for barrrier");
        } catch (BrokenBarrierException bbex) {
          fail("Broken barrier while waiting");
        }
      }
    }
  }
}
