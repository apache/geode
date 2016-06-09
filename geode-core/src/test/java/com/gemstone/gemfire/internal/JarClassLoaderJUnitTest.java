/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.internal.cache.InternalCache;
import com.gemstone.gemfire.internal.cache.execute.FunctionContextImpl;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * TODO: Need to fix this testDeclarableFunctionsWithParms and testClassOnClasspath on Windows:
 */
@Category(IntegrationTest.class)
public class JarClassLoaderJUnitTest {

  private static final String JAR_PREFIX = "vf.gf#";
  
  private final ClassBuilder classBuilder = new ClassBuilder();
  final Pattern pattern = Pattern.compile("^" + JAR_PREFIX + "JarClassLoaderJUnit.*#\\d++$");

  private InternalCache cache;

  @After
  public void tearDown() throws Exception {
    for (ClassLoader classLoader : ClassPathLoader.getLatest().getClassLoaders()) {
      if (classLoader instanceof JarClassLoader) {
        JarClassLoader jarClassLoader = (JarClassLoader) classLoader;
        if (jarClassLoader.getJarName().startsWith("JarClassLoaderJUnit")) {
          ClassPathLoader.getLatest().removeAndSetLatest(jarClassLoader);
        }
      }
    }
    for (String functionName : FunctionService.getRegisteredFunctions().keySet()) {
      if (functionName.startsWith("JarClassLoaderJUnit")) {
        FunctionService.unregisterFunction(functionName);
      }
    }

    if (this.cache != null) {
      this.cache.close();
    }

    deleteSavedJarFiles();
  }
  
  @Test
  public void testValidJarContent() throws IOException {
    assertTrue(JarClassLoader.isValidJarContent(this.classBuilder.createJarFromName("JarClassLoaderJUnitA")));
  }
  
  @Test
  public void testInvalidJarContent() {
    assertFalse(JarClassLoader.isValidJarContent("INVALID JAR CONTENT".getBytes()));
  }
  
  @Test
  public void testClassOnClasspath() throws IOException {
    final File jarFile1 = new File(JAR_PREFIX + "JarClassLoaderJUnit.jar#1");
    final File jarFile2 = new File(JAR_PREFIX + "JarClassLoaderJUnit.jar#2");
    ClassPathLoader classPathLoader = ClassPathLoader.createWithDefaults(false);

    // Deploy the first JAR file and make sure the class is on the Classpath
    byte[] jarBytes = this.classBuilder.createJarFromClassContent("com/jcljunit/JarClassLoaderJUnitA",
        "package com.jcljunit; public class JarClassLoaderJUnitA {}");
    writeJarBytesToFile(jarFile1, jarBytes);
    JarClassLoader classLoader = new JarClassLoader(jarFile1, "JarClassLoaderJUnit.jar", jarBytes);
    classPathLoader = classPathLoader.addOrReplace(classLoader);

    try {
      classPathLoader.forName("com.jcljunit.JarClassLoaderJUnitA");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    // Update the JAR file and make sure the first class is no longer on the Classpath
    // and the second one is.
    jarBytes = this.classBuilder.createJarFromClassContent("com/jcljunit/JarClassLoaderJUnitB",
        "package com.jcljunit; public class JarClassLoaderJUnitB {}");
    writeJarBytesToFile(jarFile2, jarBytes);
    classLoader = new JarClassLoader(jarFile2, "JarClassLoaderJUnit.jar", jarBytes);
    classPathLoader = classPathLoader.addOrReplace(classLoader);

    try {
      classPathLoader.forName("com.jcljunit.JarClassLoaderJUnitB");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    try {
      classPathLoader.forName("com.jcljunit.JarClassLoaderJUnitA");
      fail("Class should not be found on Classpath");
    } catch (ClassNotFoundException expected) { // expected
    }

    classPathLoader.remove(classLoader);
  }

  @Test
  public void testFailingCompilation() throws Exception {
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("import com.gemstone.gemfire.cache.Declarable;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.Function;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.FunctionContext;");
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
    final File jarFile1 = new File(JAR_PREFIX + "JarClassLoaderJUnit.jar#1");
    final File jarFile2 = new File(JAR_PREFIX + "JarClassLoaderJUnit.jar#2");
    ClassPathLoader classPathLoader = ClassPathLoader.createWithDefaults(false);

    // Test creating a JAR file with a function
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("import java.util.Properties;");
    stringBuffer.append("import com.gemstone.gemfire.cache.Declarable;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.Function;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.FunctionContext;");
    stringBuffer.append("public class JarClassLoaderJUnitFunction implements Function {");
    stringBuffer.append("public void init(Properties props) {}");
    stringBuffer.append("public boolean hasResult() {return true;}");
    stringBuffer.append("public void execute(FunctionContext context) {context.getResultSender().lastResult(\"GOODv1\");}");
    stringBuffer.append("public String getId() {return \"JarClassLoaderJUnitFunction\";}");
    stringBuffer.append("public boolean optimizeForWrite() {return false;}");
    stringBuffer.append("public boolean isHA() {return false;}}");
    String functionString = stringBuffer.toString();

    byte[] jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
    writeJarBytesToFile(jarFile1, jarBytes);
    JarClassLoader classLoader = new JarClassLoader(jarFile1, "JarClassLoaderJUnit.jar", jarBytes);
    classPathLoader = classPathLoader.addOrReplace(classLoader);
    classLoader.loadClassesAndRegisterFunctions();

    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertNotNull(function);
    TestResultSender resultSender = new TestResultSender();
    FunctionContext functionContext = new FunctionContextImpl(function.getId(), null, resultSender);
    function.execute(functionContext);
    assertEquals("GOODv1", (String) resultSender.getResults());

    // Test updating the function with a new JAR file
    functionString = functionString.replace("v1", "v2");
    jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
    writeJarBytesToFile(jarFile2, jarBytes);
    classLoader = new JarClassLoader(jarFile2, "JarClassLoaderJUnit.jar", jarBytes);
    classPathLoader = classPathLoader.addOrReplace(classLoader);
    classLoader.loadClassesAndRegisterFunctions();

    function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertNotNull(function);
    resultSender = new TestResultSender();
    functionContext = new FunctionContextImpl(function.getId(), null, resultSender);
    function.execute(functionContext);
    assertEquals("GOODv2", (String) resultSender.getResults());

    // Test returning null for the Id
    String functionNullIdString = functionString.replace("return \"JarClassLoaderJUnitFunction\"", "return null");
    jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionNullIdString);
    writeJarBytesToFile(jarFile1, jarBytes);
    classLoader = new JarClassLoader(jarFile1, "JarClassLoaderJUnit.jar", jarBytes);
    classPathLoader = classPathLoader.addOrReplace(classLoader);
    classLoader.loadClassesAndRegisterFunctions();
    assertNull(FunctionService.getFunction("JarClassLoaderJUnitFunction"));

    // Test removing the JAR
    classPathLoader = classPathLoader.remove(classLoader);
    assertNull(FunctionService.getFunction("JarClassLoaderJUnitFunction"));
  }

  /**
   * Ensure that abstract functions aren't added to the Function Service.
   */
  @Test
  public void testAbstractFunction() throws IOException, ClassNotFoundException  {
    final File jarFile1 = new File(JAR_PREFIX + "JarClassLoaderJUnit.jar#1");

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    CacheFactory cacheFactory = new CacheFactory(properties);
    this.cache = (InternalCache) cacheFactory.create();

    // Add an abstract Function to the Classpath
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.Function;");
    stringBuffer.append("public abstract class JarClassLoaderJUnitFunction implements Function {");
    stringBuffer.append("public String getId() {return \"JarClassLoaderJUnitFunction\";}}");
    String functionString = stringBuffer.toString();

    byte[] jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
    writeJarBytesToFile(jarFile1, jarBytes);
    JarClassLoader classLoader = new JarClassLoader(jarFile1, "JarClassLoaderJUnitFunction.jar", jarBytes);
    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(classLoader);
    classLoader.loadClassesAndRegisterFunctions();

    try {
      ClassPathLoader.getLatest().forName("JarClassLoaderJUnitFunction");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }
    
    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertNull(function);
  }
  
  @Test
  public void testDeclarableFunctionsWithNoCacheXml() throws IOException, ClassNotFoundException  {
    final File jarFile1 = new File(JAR_PREFIX + "JarClassLoaderJUnitNoXml.jar#1");

    // Add a Declarable Function without parameters for the class to the Classpath
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("import java.util.Properties;");
    stringBuffer.append("import com.gemstone.gemfire.cache.Declarable;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.Function;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.FunctionContext;");
    stringBuffer.append("public class JarClassLoaderJUnitFunctionNoXml implements Function, Declarable {");
    stringBuffer.append("public String getId() {return \"JarClassLoaderJUnitFunctionNoXml\";}");
    stringBuffer.append("public void init(Properties props) {}");
    stringBuffer.append("public void execute(FunctionContext context) {context.getResultSender().lastResult(\"NOPARMSv1\");}");
    stringBuffer.append("public boolean hasResult() {return true;}");
    stringBuffer.append("public boolean optimizeForWrite() {return false;}");
    stringBuffer.append("public boolean isHA() {return false;}}");
    String functionString = stringBuffer.toString();

    byte[] jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunctionNoXml", functionString);
    writeJarBytesToFile(jarFile1, jarBytes);
    JarClassLoader classLoader = new JarClassLoader(jarFile1, "JarClassLoaderJUnitFunctionNoXml.jar", jarBytes);
    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(classLoader);
    classLoader.loadClassesAndRegisterFunctions();

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
  public void testDeclarableFunctionsWithoutParms() throws IOException, ClassNotFoundException  {
    final File jarFile1 = new File(JAR_PREFIX + "JarClassLoaderJUnit.jar#1");
    final File jarFile2 = new File(JAR_PREFIX + "JarClassLoaderJUnit.jar#2");

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    CacheFactory cacheFactory = new CacheFactory(properties);
    this.cache = (InternalCache) cacheFactory.create();

    // Add a Declarable Function without parameters for the class to the Classpath
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("import java.util.Properties;");
    stringBuffer.append("import com.gemstone.gemfire.cache.Declarable;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.Function;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.FunctionContext;");
    stringBuffer.append("public class JarClassLoaderJUnitFunction implements Function, Declarable {");
    stringBuffer.append("public String getId() {return \"JarClassLoaderJUnitFunction\";}");
    stringBuffer.append("public void init(Properties props) {}");
    stringBuffer.append("public void execute(FunctionContext context) {context.getResultSender().lastResult(\"NOPARMSv1\");}");
    stringBuffer.append("public boolean hasResult() {return true;}");
    stringBuffer.append("public boolean optimizeForWrite() {return false;}");
    stringBuffer.append("public boolean isHA() {return false;}}");
    String functionString = stringBuffer.toString();

    byte[] jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
    writeJarBytesToFile(jarFile1, jarBytes);
    JarClassLoader classLoader = new JarClassLoader(jarFile1, "JarClassLoaderJUnitFunction.jar", jarBytes);
    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(classLoader);
    classLoader.loadClassesAndRegisterFunctions();

    try {
      ClassPathLoader.getLatest().forName("JarClassLoaderJUnitFunction");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    // Create a cache.xml file and configure the cache with it
    stringBuffer = new StringBuffer();
    stringBuffer.append("<?xml version=\"1.0\"?>");
    stringBuffer.append("<!DOCTYPE cache PUBLIC");
    stringBuffer.append("  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 7.0//EN\"");
    stringBuffer.append("  \"http://www.gemstone.com/dtd/cache6_6.dtd\">");
    stringBuffer.append("<cache>");
    stringBuffer.append("  <function-service>");
    stringBuffer.append("    <function>");
    stringBuffer.append("      <class-name>JarClassLoaderJUnitFunction</class-name>");
    stringBuffer.append("    </function>");
    stringBuffer.append(" </function-service>");
    stringBuffer.append("</cache>");
    String cacheXmlString = stringBuffer.toString();
    this.cache.loadCacheXml(new ByteArrayInputStream(cacheXmlString.getBytes()));

    // Check to see if the function without parameters executes correctly
    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertNotNull(function);
    TestResultSender resultSender = new TestResultSender();
    function.execute(new FunctionContextImpl(function.getId(), null, resultSender));
    assertEquals("NOPARMSv1", (String) resultSender.getResults());

    // Update the second function (change the value returned from execute) by deploying a JAR file
    functionString = functionString.replace("v1", "v2");
    jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
    writeJarBytesToFile(jarFile2, jarBytes);

    classLoader = new JarClassLoader(jarFile2, "JarClassLoaderJUnitFunction.jar", jarBytes);
    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(classLoader);
    classLoader.loadClassesAndRegisterFunctions();

    // Check to see if the updated function without parameters executes correctly
    function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertNotNull(function);
    function.execute(new FunctionContextImpl(function.getId(), null, resultSender));
    assertEquals("NOPARMSv2", (String) resultSender.getResults());
  }

  @Test
  public void testDeclarableFunctionsWithParms() throws IOException, ClassNotFoundException  {
    final File jarFile1 = new File(JAR_PREFIX + "JarClassLoaderJUnit.jar#1");
    final File jarFile2 = new File(JAR_PREFIX + "JarClassLoaderJUnit.jar#2");

    Properties properties = new Properties();
    properties.setProperty(MCAST_PORT, "0");
    CacheFactory cacheFactory = new CacheFactory(properties);
    this.cache = (InternalCache) cacheFactory.create();

    // Add a Declarable Function with parameters to the class to the Classpath
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("import java.util.Properties;");
    stringBuffer.append("import com.gemstone.gemfire.cache.Declarable;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.Function;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.FunctionContext;");
    stringBuffer.append("public class JarClassLoaderJUnitFunction implements Function, Declarable {");
    stringBuffer.append("private Properties properties;");
    stringBuffer.append("public String getId() {if(this.properties==null) {return \"JarClassLoaderJUnitFunction\";} else {return (String) this.properties.get(\"id\");}}");
    stringBuffer.append("public void init(Properties props) {properties = props;}");
    stringBuffer
        .append("public void execute(FunctionContext context) {context.getResultSender().lastResult(properties.get(\"returnValue\") + \"v1\");}");
    stringBuffer.append("public boolean hasResult() {return true;}");
    stringBuffer.append("public boolean optimizeForWrite() {return false;}");
    stringBuffer.append("public boolean isHA() {return false;}}");
    String functionString = stringBuffer.toString();

    byte[] jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
    writeJarBytesToFile(jarFile1, jarBytes);
    JarClassLoader classLoader = new JarClassLoader(jarFile1, "JarClassLoaderJUnitFunction.jar", jarBytes);
    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(classLoader);
    classLoader.loadClassesAndRegisterFunctions();

    try {
      ClassPathLoader.getLatest().forName("JarClassLoaderJUnitFunction");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }

    // Create a cache.xml file and configure the cache with it
    stringBuffer = new StringBuffer();
    stringBuffer.append("<?xml version=\"1.0\"?>");
    stringBuffer.append("<!DOCTYPE cache PUBLIC");
    stringBuffer.append("  \"-//GemStone Systems, Inc.//GemFire Declarative Caching 7.0//EN\"");
    stringBuffer.append("  \"http://www.gemstone.com/dtd/cache6_6.dtd\">");
    stringBuffer.append("<cache>");
    stringBuffer.append("  <function-service>");
    stringBuffer.append("    <function>");
    stringBuffer.append("      <class-name>JarClassLoaderJUnitFunction</class-name>");
    stringBuffer.append("      <parameter name=\"id\"><string>JarClassLoaderJUnitFunctionA</string></parameter>");
    stringBuffer.append("      <parameter name=\"returnValue\"><string>DOG</string></parameter>");
    stringBuffer.append("    </function>");
    stringBuffer.append("    <function>");
    stringBuffer.append("      <class-name>JarClassLoaderJUnitFunction</class-name>");
    stringBuffer.append("      <parameter name=\"id\"><string>JarClassLoaderJUnitFunctionB</string></parameter>");
    stringBuffer.append("      <parameter name=\"returnValue\"><string>CAT</string></parameter>");
    stringBuffer.append("    </function>");
    stringBuffer.append(" </function-service>");
    stringBuffer.append("</cache>");
    String cacheXmlString = stringBuffer.toString();
    this.cache.loadCacheXml(new ByteArrayInputStream(cacheXmlString.getBytes()));

    // Check to see if the functions with parameters execute correctly
    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunctionA");
    assertNotNull(function);
    TestResultSender resultSender = new TestResultSender();
    function.execute(new FunctionContextImpl(function.getId(), null, resultSender));
    assertEquals("DOGv1", (String) resultSender.getResults());

    function = FunctionService.getFunction("JarClassLoaderJUnitFunctionB");
    assertNotNull(function);
    function.execute(new FunctionContextImpl(function.getId(), null, resultSender));
    assertEquals("CATv1", (String) resultSender.getResults());

    // Update the first function (change the value returned from execute)
    functionString = functionString.replace("v1", "v2");
    jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
    writeJarBytesToFile(jarFile2, jarBytes);
    classLoader = new JarClassLoader(jarFile2, "JarClassLoaderJUnitFunction.jar", jarBytes);
    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(classLoader);
    classLoader.loadClassesAndRegisterFunctions();

    // Check to see if the updated functions with parameters execute correctly
    function = FunctionService.getFunction("JarClassLoaderJUnitFunctionA");
    assertNotNull(function);
    function.execute(new FunctionContextImpl(function.getId(), null, resultSender));
    assertEquals("DOGv2", (String) resultSender.getResults());

    function = FunctionService.getFunction("JarClassLoaderJUnitFunctionB");
    assertNotNull(function);
    function.execute(new FunctionContextImpl(function.getId(), null, resultSender));
    assertEquals("CATv2", (String) resultSender.getResults());

    // Update cache xml to add a new function and replace an existing one
    cacheXmlString = cacheXmlString.replace("JarClassLoaderJUnitFunctionA", "JarClassLoaderJUnitFunctionC").replace("CAT", "BIRD");
    this.cache.loadCacheXml(new ByteArrayInputStream(cacheXmlString.getBytes()));

    // Update the first function (change the value returned from execute)
    functionString = functionString.replace("v2", "v3");
    jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitFunction", functionString);
    writeJarBytesToFile(jarFile1, jarBytes);
    classLoader = new JarClassLoader(jarFile1, "JarClassLoaderJUnitFunction.jar", jarBytes);
    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(classLoader);
    classLoader.loadClassesAndRegisterFunctions();

    // Check to see if the updated functions with parameters execute correctly
    function = FunctionService.getFunction("JarClassLoaderJUnitFunctionA");
    assertNotNull(function);
    function.execute(new FunctionContextImpl(function.getId(), null, resultSender));
    assertEquals("DOGv3", (String) resultSender.getResults());

    function = FunctionService.getFunction("JarClassLoaderJUnitFunctionC");
    assertNotNull(function);
    function.execute(new FunctionContextImpl(function.getId(), null, resultSender));
    assertEquals("DOGv3", (String) resultSender.getResults());

    function = FunctionService.getFunction("JarClassLoaderJUnitFunctionB");
    assertNotNull(function);
    function.execute(new FunctionContextImpl(function.getId(), null, resultSender));
    assertEquals("BIRDv3", (String) resultSender.getResults());
  }

  @Test
  public void testDependencyBetweenJars() throws IOException, ClassNotFoundException  {
    final File parentJarFile = new File(JAR_PREFIX + "JarClassLoaderJUnitParent.jar#1");
    final File usesJarFile = new File(JAR_PREFIX + "JarClassLoaderJUnitUses.jar#1");
    final File functionJarFile = new File(JAR_PREFIX + "JarClassLoaderJUnitFunction.jar#1");

    // Write out a JAR files.
    StringBuffer stringBuffer = new StringBuffer();
    stringBuffer.append("package jcljunit.parent;");
    stringBuffer.append("public class JarClassLoaderJUnitParent {");
    stringBuffer.append("public String getValueParent() {");
    stringBuffer.append("return \"PARENT\";}}");

    byte[] jarBytes = this.classBuilder.createJarFromClassContent("jcljunit/parent/JarClassLoaderJUnitParent", stringBuffer.toString());
    writeJarBytesToFile(parentJarFile, jarBytes);
    JarClassLoader parentClassLoader = new JarClassLoader(parentJarFile, "JarClassLoaderJUnitParent.jar", jarBytes);

    stringBuffer = new StringBuffer();
    stringBuffer.append("package jcljunit.uses;");
    stringBuffer.append("public class JarClassLoaderJUnitUses {");
    stringBuffer.append("public String getValueUses() {");
    stringBuffer.append("return \"USES\";}}");

    jarBytes = this.classBuilder.createJarFromClassContent("jcljunit/uses/JarClassLoaderJUnitUses", stringBuffer.toString());
    writeJarBytesToFile(usesJarFile, jarBytes);
    JarClassLoader usesClassLoader = new JarClassLoader(usesJarFile, "JarClassLoaderJUnitUses.jar", jarBytes);

    stringBuffer = new StringBuffer();
    stringBuffer.append("package jcljunit.function;");
    stringBuffer.append("import jcljunit.parent.JarClassLoaderJUnitParent;");
    stringBuffer.append("import jcljunit.uses.JarClassLoaderJUnitUses;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.Function;");
    stringBuffer.append("import com.gemstone.gemfire.cache.execute.FunctionContext;");
    stringBuffer.append("public class JarClassLoaderJUnitFunction  extends JarClassLoaderJUnitParent implements Function {");
    stringBuffer.append("private JarClassLoaderJUnitUses uses = new JarClassLoaderJUnitUses();");
    stringBuffer.append("public boolean hasResult() {return true;}");
    stringBuffer
        .append("public void execute(FunctionContext context) {context.getResultSender().lastResult(getValueParent() + \":\" + uses.getValueUses());}");
    stringBuffer.append("public String getId() {return \"JarClassLoaderJUnitFunction\";}");
    stringBuffer.append("public boolean optimizeForWrite() {return false;}");
    stringBuffer.append("public boolean isHA() {return false;}}");

    ClassBuilder functionClassBuilder = new ClassBuilder();
    functionClassBuilder.addToClassPath(parentJarFile.getAbsolutePath());
    functionClassBuilder.addToClassPath(usesJarFile.getAbsolutePath());
    jarBytes = functionClassBuilder.createJarFromClassContent("jcljunit/function/JarClassLoaderJUnitFunction", stringBuffer.toString());
    writeJarBytesToFile(functionJarFile, jarBytes);
    JarClassLoader functionClassLoader = new JarClassLoader(functionJarFile, "JarClassLoaderJUnitFunction.jar", jarBytes);

    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(functionClassLoader);
    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(parentClassLoader);
    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(usesClassLoader);

    functionClassLoader.loadClassesAndRegisterFunctions();

    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunction");
    assertNotNull(function);
    TestResultSender resultSender = new TestResultSender();
    FunctionContext functionContext = new FunctionContextImpl(function.getId(), null, resultSender);
    function.execute(functionContext);
    assertEquals("PARENT:USES", (String) resultSender.getResults());
  }

  @Test
  public void testFindResource() throws IOException, ClassNotFoundException  {
    final File jarFile1 = new File(JAR_PREFIX + "JarClassLoaderJUnitResource.jar#1");
    ClassPathLoader classPathLoader = ClassPathLoader.createWithDefaults(false);
    final String fileName = "file.txt";
    final String fileContent = "FILE CONTENT";

    byte[] jarBytes = this.classBuilder.createJarFromFileContent(fileName, fileContent);
    writeJarBytesToFile(jarFile1, jarBytes);
    JarClassLoader classLoader = new JarClassLoader(jarFile1, "JarClassLoaderJUnitResource.jar", jarBytes);
    classPathLoader = classPathLoader.addOrReplace(classLoader);
    classLoader.loadClassesAndRegisterFunctions();
    
    InputStream inputStream = classLoader.getResourceAsStream(fileName);
    assertNotNull(inputStream);
    
    final byte[] fileBytes = new byte[fileContent.length()];
    inputStream.read(fileBytes);
    inputStream.close();
    assertTrue(fileContent.equals(new String(fileBytes)));
  }
  
  @Test
  public void testUpdateClassInJar() throws IOException, ClassNotFoundException  {
    final File jarFile1 = new File(JAR_PREFIX + "JarClassLoaderJUnit.jar#1");
    final File jarFile2 = new File(JAR_PREFIX + "JarClassLoaderJUnit.jar#2");
    ClassPathLoader classPathLoader = ClassPathLoader.createWithDefaults(false);

    // First use of the JAR file
    byte[] jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitTestClass",
        "public class JarClassLoaderJUnitTestClass { public Integer getValue5() { return new Integer(5); } }");
    writeJarBytesToFile(jarFile1, jarBytes);
    JarClassLoader classLoader = new JarClassLoader(jarFile1, "JarClassLoaderJUnit.jar", jarBytes);
    classPathLoader = classPathLoader.addOrReplace(classLoader);
    classLoader.loadClassesAndRegisterFunctions();

    try {
      Class<?> clazz = classPathLoader.forName("JarClassLoaderJUnitTestClass");
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
    writeJarBytesToFile(jarFile2, jarBytes);
    classLoader = new JarClassLoader(jarFile2, "JarClassLoaderJUnit.jar", jarBytes);
    classPathLoader = classPathLoader.addOrReplace(classLoader);
    classLoader.loadClassesAndRegisterFunctions();

    try {
      Class<?> clazz = classPathLoader.forName("JarClassLoaderJUnitTestClass");
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
  public void testMultiThread() throws IOException {
    final File jarFile1 = new File(JAR_PREFIX + "JarClassLoaderJUnitA.jar#1");
    final File jarFile2 = new File(JAR_PREFIX + "JarClassLoaderJUnitB.jar#1");

    // Add two JARs to the classpath
    byte[] jarBytes = this.classBuilder.createJarFromName("JarClassLoaderJUnitA");
    writeJarBytesToFile(jarFile1, jarBytes);
    JarClassLoader classLoader = new JarClassLoader(jarFile1, "JarClassLoaderJUnitA.jar", jarBytes);
    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(classLoader);

    jarBytes = this.classBuilder.createJarFromClassContent("com/jcljunit/JarClassLoaderJUnitB",
        "package com.jcljunit; public class JarClassLoaderJUnitB {}");
    writeJarBytesToFile(jarFile2, jarBytes);
    classLoader = new JarClassLoader(jarFile2, "JarClassLoaderJUnitB.jar", jarBytes);
    ClassPathLoader.getLatest().addOrReplaceAndSetLatest(classLoader);

    String[] classNames = new String[] { "JarClassLoaderJUnitA", "com.jcljunit.JarClassLoaderJUnitB", "NON-EXISTENT CLASS" };

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

  private void deleteSavedJarFiles() {
    File dirFile = new File(".");

    // Find all created JAR files
    File[] oldJarFiles = dirFile.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(final File file, final String name) {
        return JarClassLoaderJUnitTest.this.pattern.matcher(name).matches();
      }
    });

    // Now delete them
    if (oldJarFiles != null) {
      for (File oldJarFile : oldJarFiles) {
        if (!oldJarFile.delete()) {
          RandomAccessFile randomAccessFile = null;
          try {
            randomAccessFile = new RandomAccessFile(oldJarFile, "rw");
            randomAccessFile.setLength(0);
          } catch (IOException ioex) {
            fail("IOException when trying to deal with a stubborn JAR file");
          } finally {
            try {
              if (randomAccessFile != null) {
                randomAccessFile.close();
              }
            } catch (IOException ioex) {
              fail("IOException when trying to deal with a stubborn JAR file");
            }
          }
          oldJarFile.deleteOnExit();
        }
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

    public TestResultSender() {
    }

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

    ForNameExerciser(final CyclicBarrier cyclicBarrier, final int numLoops, final String[] classNames) {
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
