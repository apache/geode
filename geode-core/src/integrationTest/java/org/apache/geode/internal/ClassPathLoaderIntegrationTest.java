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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Enumeration;
import java.util.List;
import java.util.Vector;

import org.apache.bcel.Constants;
import org.apache.bcel.classfile.JavaClass;
import org.apache.bcel.generic.ClassGen;
import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.Function;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.cache.execute.FunctionContextImpl;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.junit.rules.RestoreTCCLRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

/**
 * Integration tests for {@link ClassPathLoader}.
 *
 * Extracted from ClassPathLoaderTest.
 */
public class ClassPathLoaderIntegrationTest {
  private static final int TEMP_FILE_BYTES_COUNT = 256;
  private File tempFile;
  private ClassBuilder classBuilder = new ClassBuilder();

  @Rule
  public RestoreTCCLRule restoreTCCLRule = new RestoreTCCLRule();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    System.setProperty(ClassPathLoader.EXCLUDE_TCCL_PROPERTY, "false");

    this.tempFile = this.temporaryFolder.newFile("tempFile1.tmp");
    FileOutputStream fos = new FileOutputStream(this.tempFile);
    fos.write(new byte[TEMP_FILE_BYTES_COUNT]);
    fos.close();

    File tempFile2 = this.temporaryFolder.newFile("tempFile2.tmp");
    fos = new FileOutputStream(tempFile2);
    fos.write(new byte[TEMP_FILE_BYTES_COUNT]);
    fos.close();

    // System.setProperty("user.dir", temporaryFolder.getRoot().getAbsolutePath());
    ClassPathLoader.setLatestToDefault(temporaryFolder.getRoot());
  }

  @Test
  public void testClassLoaderWithNullTccl() throws IOException, ClassNotFoundException {
    // GEODE-2796
    Thread.currentThread().setContextClassLoader(null);
    String jarName = "JarDeployerIntegrationTest.jar";
    String classAResource = "integration/parent/ClassA.class";
    String classAName = "integration.parent.ClassA";
    File firstJar = createJarWithClass("ClassA");

    // First deploy of the JAR file
    ClassPathLoader.getLatest().getJarDeployer().deploy(jarName, firstJar);

    assertThatClassCanBeLoaded(classAName);
    assertThatResourceCanBeLoaded(classAResource);
  }

  @Test
  public void testDeployFileAndChange() throws IOException, ClassNotFoundException {
    String jarName = "JarDeployerIntegrationTest.jar";

    String classAResource = "integration/parent/ClassA.class";
    String classBResource = "integration/parent/ClassB.class";

    String classAName = "integration.parent.ClassA";
    String classBName = "integration.parent.ClassB";

    File firstJar = createJarWithClass("ClassA");
    ByteArrayOutputStream firstJarBytes = new ByteArrayOutputStream();
    IOUtils.copy(new FileInputStream(firstJar), firstJarBytes);

    // First deploy of the JAR file
    File firstDeployedJarFile =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarName, firstJar).getFile();

    assertThat(firstDeployedJarFile).exists().hasBinaryContent(firstJarBytes.toByteArray());
    assertThat(firstDeployedJarFile.getName()).contains(".v1.").doesNotContain(".v2.");

    assertThatClassCanBeLoaded(classAName);
    assertThatClassCannotBeLoaded(classBName);

    assertThatResourceCanBeLoaded(classAResource);
    assertThatResourceCannotBeLoaded(classBResource);

    // Now deploy an updated JAR file and make sure that the next version of the JAR file
    // was created and the first one is no longer used
    File secondJar = createJarWithClass("ClassB");
    ByteArrayOutputStream secondJarBytes = new ByteArrayOutputStream();
    IOUtils.copy(new FileInputStream(secondJar), secondJarBytes);

    File secondDeployedJarFile =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarName, secondJar).getFile();

    assertThat(secondDeployedJarFile).exists().hasBinaryContent(secondJarBytes.toByteArray());
    assertThat(secondDeployedJarFile.getName()).contains(".v2.").doesNotContain(".v1.");

    assertThatClassCanBeLoaded(classBName);
    assertThatClassCannotBeLoaded(classAName);

    assertThatResourceCanBeLoaded(classBResource);
    assertThatResourceCannotBeLoaded(classAResource);

    // Now undeploy JAR and make sure it gets cleaned up
    ClassPathLoader.getLatest().getJarDeployer().undeploy(jarName);
    assertThatClassCannotBeLoaded(classBName);
    assertThatClassCannotBeLoaded(classAName);

    assertThatResourceCannotBeLoaded(classBResource);
    assertThatResourceCannotBeLoaded(classAResource);
  }

  @Test
  public void testDeployNoUpdateWhenNoChange() throws IOException, ClassNotFoundException {
    String jarName = "JarDeployerIntegrationTest.jar";

    // First deploy of the JAR file
    byte[] jarBytes = new ClassBuilder().createJarFromName("JarDeployerDUnitDNUWNC");
    File jarFile = temporaryFolder.newFile();
    writeJarBytesToFile(jarFile, jarBytes);
    DeployedJar jarClassLoader =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarName, jarFile);
    File deployedJar = new File(jarClassLoader.getFileCanonicalPath());

    assertThat(deployedJar).exists();
    assertThat(deployedJar.getName()).contains(".v1.");

    // Re-deploy of the same JAR should do nothing
    DeployedJar newJarClassLoader =
        ClassPathLoader.getLatest().getJarDeployer().deploy(jarName, jarFile);
    assertThat(newJarClassLoader).isNull();
    assertThat(deployedJar).exists();
  }

  @Test
  public void deployNewVersionOfFunctionOverOldVersion() throws Exception {
    File jarVersion1 = createVersionOfJar("Version1");
    File jarVersion2 = createVersionOfJar("Version2");

    ServerStarterRule serverStarterRule = new ServerStarterRule().withWorkingDir();
    serverStarterRule.before();
    serverStarterRule.startServer();
    DistributedSystem distributedSystem =
        serverStarterRule.getCache().getInternalDistributedSystem();

    ClassPathLoader.getLatest().getJarDeployer().deploy("MyJar.jar", jarVersion1);

    assertThatClassCanBeLoaded("jddunit.function.MyFunction");
    Execution execution = FunctionService.onMember(distributedSystem.getDistributedMember());

    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) execution.execute("MyFunction").getResult();
    assertThat(result.get(0)).isEqualTo("Version1");

    ClassPathLoader.getLatest().getJarDeployer().deploy("MyJar.jar", jarVersion2);

    @SuppressWarnings("unchecked")
    List<String> result2 = (List<String>) execution.execute("MyFunction").getResult();
    assertThat(result2.get(0)).isEqualTo("Version2");

    serverStarterRule.after();
  }

  @Test
  public void deployWithExistingDependentJars() throws Exception {
    ClassBuilder classBuilder = new ClassBuilder();
    final File parentJarFile =
        new File(temporaryFolder.getRoot(), "JarDeployerDUnitAParent.v1.jar");
    final File usesJarFile = new File(temporaryFolder.getRoot(), "JarDeployerDUnitUses.v1.jar");
    final File functionJarFile =
        new File(temporaryFolder.getRoot(), "JarDeployerDUnitFunction.v1.jar");

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

    ServerStarterRule serverStarterRule =
        new ServerStarterRule().withWorkingDir(temporaryFolder.getRoot());
    serverStarterRule.before();
    serverStarterRule.startServer();
    DistributedSystem distributedSystem =
        serverStarterRule.getCache().getInternalDistributedSystem();

    Execution execution = FunctionService.onMember(distributedSystem.getDistributedMember());
    ResultCollector resultCollector = execution.execute("JarDeployerDUnitFunction");
    @SuppressWarnings("unchecked")
    List<String> result = (List<String>) resultCollector.getResult();
    assertThat(result.get(0)).isEqualTo("PARENT:USES");

    serverStarterRule.after();
  }

  private File createVersionOfJar(String version) throws IOException {
    String jarName = "MyJar.jar";
    String functionName = "MyFunction";
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

  private void assertThatClassCannotBeLoaded(String className) {
    assertThatThrownBy(() -> ClassPathLoader.getLatest().forName(className))
        .isExactlyInstanceOf(ClassNotFoundException.class);
  }

  private void assertThatResourceCanBeLoaded(String resourceName) throws IOException {
    // ClassPathLoader.getResource
    assertThat(ClassPathLoader.getLatest().getResource(resourceName)).isNotNull();

    // ClassPathLoader.getResources
    Enumeration<URL> urls = ClassPathLoader.getLatest().getResources(resourceName);
    assertThat(urls).isNotNull();
    assertThat(urls.hasMoreElements()).isTrue();

    // ClassPathLoader.getResourceAsStream
    InputStream is = ClassPathLoader.getLatest().getResourceAsStream(resourceName);
    assertThat(is).isNotNull();
  }

  private void assertThatResourceCannotBeLoaded(String resourceName) throws IOException {
    // ClassPathLoader.getResource
    assertThat(ClassPathLoader.getLatest().getResource(resourceName)).isNull();

    // ClassPathLoader.getResources
    Enumeration<URL> urls = ClassPathLoader.getLatest().getResources(resourceName);
    assertThat(urls.hasMoreElements()).isFalse();

    // ClassPathLoader.getResourceAsStream
    InputStream is = ClassPathLoader.getLatest().getResourceAsStream(resourceName);
    assertThat(is).isNull();
  }

  /**
   * Verifies that <tt>getResource</tt> works with TCCL from {@link ClassPathLoader}.
   */
  @Test
  public void testGetResourceWithTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceWithTCCL");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);

    String resourceToGet = "com/nowhere/testGetResourceWithTCCL.rsc";
    assertThat(dcl.getResource(resourceToGet)).isNull();

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      URL url = dcl.getResource(resourceToGet);
      assertThat(url).isNotNull();

      InputStream is = url.openStream();
      assertThat(is).isNotNull();

      int totalBytesRead = 0;
      byte[] input = new byte[128];

      BufferedInputStream bis = new BufferedInputStream(is);
      for (int bytesRead = bis.read(input); bytesRead > -1;) {
        totalBytesRead += bytesRead;
        bytesRead = bis.read(input);
      }
      bis.close();

      assertThat(totalBytesRead).isEqualTo(TEMP_FILE_BYTES_COUNT);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that <tt>getResources</tt> works with TCCL from {@link ClassPathLoader}.
   */
  @Test
  public void testGetResourcesWithTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceWithTCCL");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);

    String resourceToGet = "com/nowhere/testGetResourceWithTCCL.rsc";
    Enumeration<URL> urls = dcl.getResources(resourceToGet);
    assertThat(urls).isNotNull();
    assertThat(urls.hasMoreElements()).isFalse();

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      urls = dcl.getResources(resourceToGet);
      assertThat(urls).isNotNull();

      URL url = urls.nextElement();
      InputStream is = url.openStream();
      assertThat(is).isNotNull();

      int totalBytesRead = 0;
      byte[] input = new byte[128];

      BufferedInputStream bis = new BufferedInputStream(is);
      for (int bytesRead = bis.read(input); bytesRead > -1;) {
        totalBytesRead += bytesRead;
        bytesRead = bis.read(input);
      }
      bis.close();

      assertThat(totalBytesRead).isEqualTo(TEMP_FILE_BYTES_COUNT);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that <tt>getResourceAsStream</tt> works with TCCL from {@link ClassPathLoader}.
   */
  @Test
  public void testGetResourceAsStreamWithTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceAsStreamWithTCCL");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);

    String resourceToGet = "com/nowhere/testGetResourceAsStreamWithTCCL.rsc";
    assertThat(dcl.getResourceAsStream(resourceToGet)).isNull();

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // ensure that TCCL is only CL that can find this resource
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      InputStream is = dcl.getResourceAsStream(resourceToGet);
      assertThat(is).isNotNull();

      int totalBytesRead = 0;
      byte[] input = new byte[128];

      BufferedInputStream bis = new BufferedInputStream(is);
      for (int bytesRead = bis.read(input); bytesRead > -1;) {
        totalBytesRead += bytesRead;
        bytesRead = bis.read(input);
      }
      bis.close();

      assertThat(totalBytesRead).isEqualTo(TEMP_FILE_BYTES_COUNT);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeclarableFunctionsWithNoCacheXml() throws Exception {
    final String jarFilename = "JarClassLoaderJUnitNoXml.jar";

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
    File jarFile = temporaryFolder.newFile();
    writeJarBytesToFile(jarFile, jarBytes);

    ClassPathLoader.getLatest().getJarDeployer().deploy(jarFilename, jarFile);

    ClassPathLoader.getLatest().forName("JarClassLoaderJUnitFunctionNoXml");

    // Check to see if the function without parameters executes correctly
    Function function = FunctionService.getFunction("JarClassLoaderJUnitFunctionNoXml");
    assertThat(function).isNotNull();
    TestResultSender resultSender = new TestResultSender();
    function.execute(new FunctionContextImpl(null, function.getId(), null, resultSender));
    assertThat((String) resultSender.getResults()).isEqualTo("NOPARMSv1");
  }

  @Test
  @SuppressWarnings("unchecked")
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
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitParent.jar",
        parentJarFile);

    stringBuffer = new StringBuffer();
    stringBuffer.append("package jcljunit.uses;");
    stringBuffer.append("public class JarClassLoaderJUnitUses {");
    stringBuffer.append("public String getValueUses() {");
    stringBuffer.append("return \"USES\";}}");

    jarBytes = this.classBuilder.createJarFromClassContent("jcljunit/uses/JarClassLoaderJUnitUses",
        stringBuffer.toString());
    writeJarBytesToFile(usesJarFile, jarBytes);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitUses.jar", usesJarFile);

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
    File jarFunction = temporaryFolder.newFile();
    writeJarBytesToFile(jarFunction, jarBytes);

    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitFunction.jar",
        jarFunction);

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
    File tempJar = temporaryFolder.newFile();
    writeJarBytesToFile(tempJar, jarBytes);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitResource.jar", tempJar);

    InputStream inputStream = ClassPathLoader.getLatest().getResourceAsStream(fileName);
    assertThat(inputStream).isNotNull();

    final byte[] fileBytes = new byte[fileContent.length()];
    assertThat(inputStream.read(fileBytes)).isNotNull();
    inputStream.close();
    assertThat(fileContent).isEqualTo(new String(fileBytes));
  }


  @Test
  public void testUpdateClassInJar() throws Exception {
    // First use of the JAR file
    byte[] jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitTestClass",
        "public class JarClassLoaderJUnitTestClass { public Integer getValue5() { return new Integer(5); } }");
    File jarFile = temporaryFolder.newFile();
    writeJarBytesToFile(jarFile, jarBytes);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitUpdate.jar", jarFile);

    Class<?> clazz = ClassPathLoader.getLatest().forName("JarClassLoaderJUnitTestClass");
    Object object = clazz.newInstance();
    Method getValue5Method = clazz.getMethod("getValue5");
    Integer value = (Integer) getValue5Method.invoke(object);
    assertThat(value).isEqualTo(5);

    // Now create an updated JAR file and make sure that the method from the new
    // class is available.
    jarBytes = this.classBuilder.createJarFromClassContent("JarClassLoaderJUnitTestClass",
        "public class JarClassLoaderJUnitTestClass { public Integer getValue10() { return new Integer(10); } }");
    File jarFile2 = temporaryFolder.newFile();
    writeJarBytesToFile(jarFile2, jarBytes);
    ClassPathLoader.getLatest().getJarDeployer().deploy("JarClassLoaderJUnitUpdate.jar", jarFile2);

    clazz = ClassPathLoader.getLatest().forName("JarClassLoaderJUnitTestClass");
    object = clazz.newInstance();
    Method getValue10Method = clazz.getMethod("getValue10");
    value = (Integer) getValue10Method.invoke(object);
    assertThat(value).isEqualTo(10);
  }

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.close();
  }

  /**
   * Custom class loader which uses BCEL to always dynamically generate a class for any class name
   * it tries to load.
   */
  private class GeneratingClassLoader extends ClassLoader {

    /**
     * Currently unused but potentially useful for some future test. This causes this loader to only
     * generate a class that the parent could not find.
     *
     * @param parent the parent class loader to check with first
     */
    @SuppressWarnings("unused")
    public GeneratingClassLoader(ClassLoader parent) {
      super(parent);
    }

    /**
     * Specifies no parent to ensure that this loader generates the named class.
     */
    GeneratingClassLoader() {
      super(null); // no parent!!
    }

    @Override
    @SuppressWarnings("deprecation")
    protected Class<?> findClass(String name) {
      ClassGen cg = new ClassGen(name, "java.lang.Object", "<generated>",
          Constants.ACC_SUPER | Constants.ACC_PUBLIC, null);
      cg.addEmptyConstructor(Constants.ACC_PUBLIC);
      JavaClass jClazz = cg.getJavaClass();
      byte[] bytes = jClazz.getBytes();
      return defineClass(jClazz.getClassName(), bytes, 0, bytes.length);
    }

    @Override
    protected URL findResource(String name) {
      URL url = null;
      try {
        url = getTempFile().getAbsoluteFile().toURI().toURL();
        System.out.println("GeneratingClassLoader#findResource returning " + url);
      } catch (IOException e) {
        // Do nothing.
      }

      return url;
    }

    @Override
    protected Enumeration<URL> findResources(String name) {
      URL url = null;
      try {
        url = getTempFile().getAbsoluteFile().toURI().toURL();
        System.out.println("GeneratingClassLoader#findResources returning " + url);
      } catch (IOException e) {
        // Do nothing.
      }

      Vector<URL> urls = new Vector<>();
      urls.add(url);
      return urls.elements();
    }

    private File getTempFile() {
      return tempFile;
    }
  }

  private File createJarWithClass(String className) throws IOException {
    String stringBuilder = "package integration.parent;" + "public class " + className + " {}";

    byte[] jarBytes = new ClassBuilder()
        .createJarFromClassContent("integration/parent/" + className, stringBuilder);

    File jarFile = temporaryFolder.newFile();
    IOUtils.copy(new ByteArrayInputStream(jarBytes), new FileOutputStream(jarFile));

    return jarFile;
  }

  private static class TestResultSender implements ResultSender<Object> {
    private Object result;

    TestResultSender() {}

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
}
