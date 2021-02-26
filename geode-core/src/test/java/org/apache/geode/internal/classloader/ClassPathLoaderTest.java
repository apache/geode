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
package org.apache.geode.internal.classloader;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
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

import org.apache.geode.deployment.internal.JarDeploymentService;
import org.apache.geode.deployment.internal.JarDeploymentServiceFactory;
import org.apache.geode.management.configuration.Deployment;
import org.apache.geode.test.compiler.ClassBuilder;

/**
 * Unit tests for {@link ClassPathLoader}.
 *
 * @since GemFire 6.5.1.4
 */
public class ClassPathLoaderTest {

  private static final int GENERATED_CLASS_BYTES_COUNT = 354;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    System.setProperty(ClassPathLoader.EXCLUDE_TCCL_PROPERTY, "false");
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} is always initialized and returns a
   * <tt>ClassPathLoader</tt> instance.
   */
  @Test
  public void testLatestExists() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testLatestExists");

    assertThat(ClassPathLoader.getLatest()).isNotNull();
  }

  @Test
  public void testZeroLengthFile() throws IOException {
    File zeroFile = tempFolder.newFile("JarDeployerDUnitZLF.jar");
    zeroFile.createNewFile();

    JarDeploymentService jarDeploymentService =
        JarDeploymentServiceFactory.getJarDeploymentServiceInstance();
    assertThatThrownBy(() -> {
      Deployment deployment =
          new Deployment("JarDeployerDUnitZLF.jar", "test", Instant.now().toString());
      deployment.setFile(zeroFile);
      jarDeploymentService.deploy(
          deployment);
    }).isInstanceOf(IllegalArgumentException.class);

    byte[] validBytes = new ClassBuilder().createJarFromName("JarDeployerDUnitZLF1");
    File validFile = tempFolder.newFile("JarDeployerDUnitZLF1.jar");
    IOUtils.copy(new ByteArrayInputStream(validBytes), new FileOutputStream(validFile));

    Set<File> files = new HashSet<>();
    files.add(validFile);
    files.add(zeroFile);

    assertThatThrownBy(() -> {
      for (File file : files) {
        Deployment deployment = new Deployment(file.getName(), "test", Instant.now().toString());
        deployment.setFile(file);
        jarDeploymentService.deploy(deployment);
      }
    }).isInstanceOf(IllegalArgumentException.class);

    // clean up the deployed files
    jarDeploymentService.listDeployed()
        .forEach(deployment -> jarDeploymentService
            .undeployByDeploymentName(deployment.getDeploymentName()));
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} throws <tt>ClassNotFoundException</tt> when
   * class does not exist.
   */
  @Test
  public void testForNameThrowsClassNotFoundException() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testForNameThrowsClassNotFoundException");

    String classToLoad = "com.nowhere.DoesNotExist";

    assertThatThrownBy(() -> ClassPathLoader.getLatest().forName(classToLoad))
        .isInstanceOf(ClassNotFoundException.class);
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} finds and loads class via
   * <tt>Class.forName(String, boolean, ClassLoader)</tt> when class does exist.
   */
  @Test
  public void testForName() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testForName");

    String classToLoad = "org.apache.geode.internal.classpathloaderjunittest.DoesExist";
    Class<?> clazz = ClassPathLoader.getLatest().forName(classToLoad);
    assertThat(clazz).isNotNull();
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} can actually <tt>getResource</tt> when it
   * exists.
   */
  @Test
  public void testGetResource() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResource");

    String resourceToGet = "org/apache/geode/internal/classpathloaderjunittest/DoesExist.class";
    URL url = ClassPathLoader.getLatest().getResource(resourceToGet);
    assertThat(url).isNotNull();

    InputStream is = url != null ? url.openStream() : null;
    assertThat(is).isNotNull();

    int totalBytesRead = 0;
    byte[] input = new byte[256];

    BufferedInputStream bis = new BufferedInputStream(is);
    for (int bytesRead = bis.read(input); bytesRead > -1;) {
      totalBytesRead += bytesRead;
      bytesRead = bis.read(input);
    }
    bis.close();

    // if the following fails then maybe javac changed and DoesExist.class
    // contains other than 374 bytes of data... consider updating this test
    assertThat(totalBytesRead).isEqualTo(GENERATED_CLASS_BYTES_COUNT);
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} can actually <tt>getResources</tt> when it
   * exists.
   */
  @Test
  public void testGetResources() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResources");

    String resourceToGet = "org/apache/geode/internal/classpathloaderjunittest/DoesExist.class";
    Enumeration<URL> urls = ClassPathLoader.getLatest().getResources(resourceToGet);
    assertThat(urls).isNotNull();
    assertThat(urls.hasMoreElements()).isTrue();

    URL url = urls.nextElement();
    InputStream is = url != null ? url.openStream() : null;
    assertThat(is).isNotNull();

    int totalBytesRead = 0;
    byte[] input = new byte[256];

    BufferedInputStream bis = new BufferedInputStream(is);
    for (int bytesRead = bis.read(input); bytesRead > -1;) {
      totalBytesRead += bytesRead;
      bytesRead = bis.read(input);
    }
    bis.close();

    // if the following fails then maybe javac changed and DoesExist.class
    // contains other than 374 bytes of data... consider updating this test
    assertThat(totalBytesRead).isEqualTo(GENERATED_CLASS_BYTES_COUNT);
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} can actually <tt>getResourceAsStream</tt>
   * when it exists.
   */
  @Test
  public void testGetResourceAsStream() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceAsStream");

    String resourceToGet = "org/apache/geode/internal/classpathloaderjunittest/DoesExist.class";
    InputStream is = ClassPathLoader.getLatest().getResourceAsStream(resourceToGet);
    assertThat(is).isNotNull();

    int totalBytesRead = 0;
    byte[] input = new byte[256];

    BufferedInputStream bis = new BufferedInputStream(is);
    for (int bytesRead = bis.read(input); bytesRead > -1;) {
      totalBytesRead += bytesRead;
      bytesRead = bis.read(input);
    }
    bis.close();

    // if the following fails then maybe javac changed and DoesExist.class
    // contains other than 374 bytes of data... consider updating this test
    assertThat(totalBytesRead).isEqualTo(GENERATED_CLASS_BYTES_COUNT);
  }

  /**
   * Verifies that the {@link GeneratingClassLoader} works and always generates the named class.
   * This is a control which ensures that tests depending on <tt>GeneratingClassLoader</tt> are
   * valid.
   */
  @Test
  public void testGeneratingClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGeneratingClassLoader");

    ClassLoader gcl = new GeneratingClassLoader();
    String classToLoad = "com.nowhere.TestGeneratingClassLoader";

    Class<?> clazz = gcl.loadClass(classToLoad);
    assertThat(clazz).isNotNull();
    assertThat(clazz.getName()).isEqualTo(classToLoad);

    Object obj = clazz.newInstance();
    assertThat(obj.getClass().getName()).isEqualTo(clazz.getName());

    assertThatThrownBy(() -> {
      Class.forName(classToLoad);
    }).isInstanceOf(ClassNotFoundException.class);

    Class<?> clazzForName = Class.forName(classToLoad, true, gcl);
    assertThat(clazzForName).isNotNull();
    assertThat(clazzForName).isEqualTo(clazz);

    Object objForName = clazzForName.newInstance();
    assertThat(objForName.getClass().getName()).isEqualTo(classToLoad);
  }

  /**
   * Verifies that {@link Class#forName(String, boolean, ClassLoader)} used with
   * {@link ClassPathLoader} works as expected with named object arrays, while
   * {@link ClassLoader#loadClass(String)} throws ClassNotFoundException for named object arrays.
   */
  @Test
  public void testForNameWithObjectArray() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testForNameWithObjectArray");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);

    String classToLoad = "[Ljava.lang.String;";
    Class<?> clazz = null;
    clazz = dcl.forName(classToLoad);
    assertThat(clazz.getName()).isEqualTo(classToLoad);
  }

  /**
   * Verifies that TCCL finds the class when {@link Class#forName(String, boolean, ClassLoader)}
   * uses {@link ClassPathLoader}.
   */
  @Test
  public void testForNameWithTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testForNameWithTCCL");

    final ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    final String classToLoad = "com.nowhere.TestForNameWithTCCL";

    assertThatThrownBy(() -> {
      dcl.forName(classToLoad);

    }).isInstanceOf(ClassNotFoundException.class);

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // ensure that TCCL is only CL that can find this class
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      Class<?> clazz = dcl.forName(classToLoad);
      assertThat(clazz).isNotNull();
      Object instance = clazz.newInstance();
      assertThat(instance).isNotNull();
      assertThat(instance.getClass().getName()).isEqualTo(classToLoad);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }

    assertThatThrownBy(() -> {
      dcl.forName(classToLoad);
    }).isInstanceOf(ClassNotFoundException.class);
  }

  /**
   * Verifies that the {@link NullClassLoader} works and never finds the named class. This is a
   * control which ensures that tests depending on <tt>NullClassLoader</tt> are valid.
   */
  @Test
  public void testNullClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testNullClassLoader");

    ClassLoader cl = new NullClassLoader();
    String classToLoad = "java.lang.String";

    assertThatThrownBy(() -> {
      Class.forName(classToLoad, true, cl);
    }).isInstanceOf(ClassNotFoundException.class);

    String resourceToGet = "java/lang/String.class";

    URL url = cl.getResource(resourceToGet);
    assertThat(url).isNull();

    InputStream is = cl.getResourceAsStream(resourceToGet);
    assertThat(is).isNull();
  }

  /**
   * Verifies that the {@link SimpleClassLoader} works and finds classes that the parent can find.
   * This is a control which ensures that tests depending on <tt>SimpleClassLoader</tt> are valid.
   */
  @Test
  public void testSimpleClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testSimpleClassLoader");

    ClassLoader cl = new SimpleClassLoader(getClass().getClassLoader());
    String classToLoad = "java.lang.String";

    Class<?> clazz = Class.forName(classToLoad, true, cl);
    assertThat(clazz).isNotNull();

    String resourceToGet = "java/lang/String.class";

    URL url = cl.getResource(resourceToGet);
    assertThat(url).isNotNull();

    InputStream is = cl.getResourceAsStream(resourceToGet);
    assertThat(is).isNotNull();
  }

  /**
   * Verifies that the {@link BrokenClassLoader} is broken and always throws errors. This is a
   * control which ensures that tests depending on <tt>BrokenClassLoader</tt> are valid.
   */
  @Test
  public void testBrokenClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testBrokenClassLoader");

    ClassLoader cl = new BrokenClassLoader();

    String classToLoad = "java.lang.String";

    assertThatThrownBy(() -> {
      Class.forName(classToLoad, true, cl);
    }).isInstanceOf(BrokenError.class);

    String resourceToGet = "java/lang/String.class";
    assertThatThrownBy(() -> {
      cl.getResource(resourceToGet);
    }).isInstanceOf(BrokenError.class);

    assertThatThrownBy(() -> {
      cl.getResourceAsStream(resourceToGet);
    }).isInstanceOf(BrokenError.class);
  }

  /**
   * Verifies that the {@link BrokenClassLoader} is broken and always throws errors even when used
   * as a TCCL from {@link ClassPathLoader}. This is primarily a control which ensures that tests
   * depending on <tt>BrokenClassLoader</tt> are valid, but it also verifies that TCCL is included
   * by default by <tt>ClassPathLoader</tt>.
   */
  @Test
  public void testBrokenTCCLThrowsErrors() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testBrokenTCCLThrowsErrors");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // set the TCCL to throw errors
      Thread.currentThread().setContextClassLoader(new BrokenClassLoader());

      String classToLoad = "java.lang.String";
      assertThatThrownBy(() -> {
        dcl.forName(classToLoad);
      }).isInstanceOf(BrokenError.class);

      String resourceToGet = "java/lang/String.class";
      assertThatThrownBy(() -> {
        dcl.getResource(resourceToGet);
      }).isInstanceOf(BrokenError.class);

      assertThatThrownBy(() -> {
        dcl.getResourceAsStream(resourceToGet);
      }).isInstanceOf(BrokenError.class);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that the class classloader or system classloader will find the class or resource.
   * Parent is a {@link NullClassLoader} while the TCCL is an excluded {@link BrokenClassLoader}.
   */
  @Test
  public void testEverythingWithDefaultLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testEverythingWithDefaultLoader");

    // create DCL such that parent cannot find anything
    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(true);

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // set the TCCL to never find anything
      Thread.currentThread().setContextClassLoader(new BrokenClassLoader());

      String classToLoad = "java.lang.String";
      Class<?> clazz = dcl.forName(classToLoad);
      assertThat(clazz).isNotNull();

      String resourceToGet = "java/lang/String.class";
      URL url = dcl.getResource(resourceToGet);
      assertThat(url).isNotNull();
      InputStream is = dcl.getResourceAsStream(resourceToGet);
      assertThat(is).isNotNull();
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that setting <tt>excludeThreadContextClassLoader</tt> to true will indeed exclude the
   * TCCL.
   */
  @Test
  public void testExcludeTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testExcludeTCCL");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(true);

    String classToLoad = "com.nowhere.TestExcludeTCCL";

    assertThatThrownBy(() -> {
      dcl.forName(classToLoad);

    }).isInstanceOf(ClassNotFoundException.class);

    ClassLoader cl = Thread.currentThread().getContextClassLoader();

    try {
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());

      assertThatThrownBy(() -> {
        dcl.forName(classToLoad);
      }).isInstanceOf(ClassNotFoundException.class);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that <tt>getResource</tt> will skip TCCL if <tt>excludeThreadContextClassLoader</tt>
   * has been set to true.
   */
  @Test
  public void testGetResourceExcludeTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceExcludeTCCL");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(true);

    String resourceToGet = "com/nowhere/testGetResourceExcludeTCCL.rsc";
    assertThat(dcl.getResource(resourceToGet)).isNull();

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // ensure that TCCL is only CL that can find this resource
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      assertThat(dcl.getResource(resourceToGet)).isNull();
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that <tt>getResourceAsStream</tt> will skip TCCL if
   * <tt>excludeThreadContextClassLoader</tt> has been set to true.
   */
  @Test
  public void testGetResourceAsStreamExcludeTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceAsStreamExcludeTCCL");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(true);

    String resourceToGet = "com/nowhere/testGetResourceAsStreamExcludeTCCL.rsc";
    assertThat(dcl.getResourceAsStream(resourceToGet)).isNull();

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // ensure that TCCL is only CL that can find this resource
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      assertThat(dcl.getResourceAsStream(resourceToGet)).isNull();
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  private static void exploreClassLoaderSuperClass(String prefix, Class<?> clazz) {
    Class<?> superClazz = clazz.getSuperclass();
    if (superClazz != null) {
      System.out.println(
          prefix + "                       getSuperclass().getName() = " + superClazz.getName());
      exploreClassLoaderSuperClass(prefix, superClazz);
    }
  }

  /**
   * Custom class loader which will never find any class or resource.
   */
  static class NullClassLoader extends ClassLoader {
    public NullClassLoader() {
      super(null); // no parent!!
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      throw new ClassNotFoundException(name);
    }

    @Override
    public URL getResource(String name) {
      return null;
    }
  }

  /**
   * Custom class loader which will find anything the parent can find.
   */
  static class SimpleClassLoader extends ClassLoader {
    public SimpleClassLoader(ClassLoader parent) {
      super(parent);
    }
  }

  /**
   * Custom class loader which is broken and always throws errors.
   */
  static class BrokenClassLoader extends ClassLoader {
    public BrokenClassLoader() {
      super(null); // no parent!!
    }

    @Override
    public Class<?> loadClass(String name) throws ClassNotFoundException {
      throw new BrokenError();
    }

    @Override
    public URL getResource(String name) {
      throw new BrokenError();
    }
  }

  /**
   * Custom class loader which uses BCEL to always dynamically generate a class for any class name
   * it tries to load.
   */
  static class GeneratingClassLoader extends ClassLoader {

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
    public GeneratingClassLoader() {
      super(null); // no parent!!
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
      ClassGen cg = new ClassGen(name, "java.lang.Object", "<generated>",
          Constants.ACC_PUBLIC | Constants.ACC_SUPER, null);
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
      }
      return url;
    }

    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
      URL url = null;
      try {
        url = getTempFile().getAbsoluteFile().toURI().toURL();
        System.out.println("GeneratingClassLoader#findResources returning " + url);
      } catch (IOException e) {
      }
      Vector<URL> urls = new Vector<URL>();
      urls.add(url);
      return urls.elements();
    }

    protected File getTempFile() {
      return null;
    }
  }

  @SuppressWarnings("serial")
  static class BrokenError extends Error {
  }
}
