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
package org.apache.geode.internal;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Vector;

import org.apache.bcel.Constants;
import org.apache.bcel.classfile.JavaClass;
import org.apache.bcel.generic.ClassGen;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for {@link ClassPathLoader}.
 * 
 * @since GemFire 6.5.1.4
 */
@Category(UnitTest.class)
public class ClassPathLoaderTest {

  private static final int GENERATED_CLASS_BYTES_COUNT = 354;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    System.setProperty(ClassPathLoader.EXCLUDE_TCCL_PROPERTY, "false");
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} is always initialized and returns a <tt>ClassPathLoader</tt>
   * instance.
   */
  @Test
  public void testLatestExists() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testLatestExists");

    assertNotNull(ClassPathLoader.getLatest());
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} throws <tt>ClassNotFoundException</tt> when class does not exist.
   */
  @Test
  public void testForNameThrowsClassNotFoundException() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testForNameThrowsClassNotFoundException");

    try {
      String classToLoad = "com.nowhere.DoesNotExist";
      ClassPathLoader.getLatest().forName(classToLoad);
      fail();
    } catch (ClassNotFoundException expected) {
      // Expected
    }
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
    assertNotNull(clazz);
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} can actually <tt>getResource</tt> when it exists.
   */
  @Test
  public void testGetResource() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResource");

    String resourceToGet = "org/apache/geode/internal/classpathloaderjunittest/DoesExist.class";
    URL url = ClassPathLoader.getLatest().getResource(resourceToGet);
    assertNotNull(url);

    InputStream is = url != null ? url.openStream() : null;
    assertNotNull(is);

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
    assertEquals(GENERATED_CLASS_BYTES_COUNT, totalBytesRead);
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} can actually <tt>getResources</tt> when it exists.
   */
  @Test
  public void testGetResources() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResources");

    String resourceToGet = "org/apache/geode/internal/classpathloaderjunittest/DoesExist.class";
    Enumeration<URL> urls = ClassPathLoader.getLatest().getResources(resourceToGet);
    assertNotNull(urls);
    assertTrue(urls.hasMoreElements());

    URL url = urls.nextElement();
    InputStream is = url != null ? url.openStream() : null;
    assertNotNull(is);

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
    assertEquals(GENERATED_CLASS_BYTES_COUNT, totalBytesRead);
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} can actually <tt>getResourceAsStream</tt> when it exists.
   */
  @Test
  public void testGetResourceAsStream() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceAsStream");

    String resourceToGet = "org/apache/geode/internal/classpathloaderjunittest/DoesExist.class";
    InputStream is = ClassPathLoader.getLatest().getResourceAsStream(resourceToGet);
    assertNotNull(is);

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
    assertEquals(GENERATED_CLASS_BYTES_COUNT, totalBytesRead);
  }

  /**
   * Verifies that the {@link GeneratingClassLoader} works and always generates the named class. This is a control which
   * ensures that tests depending on <tt>GeneratingClassLoader</tt> are valid.
   */
  @Test
  public void testGeneratingClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGeneratingClassLoader");

    ClassLoader gcl = new GeneratingClassLoader();
    String classToLoad = "com.nowhere.TestGeneratingClassLoader";

    Class<?> clazz = gcl.loadClass(classToLoad);
    assertNotNull(clazz);
    assertEquals(classToLoad, clazz.getName());

    Object obj = clazz.newInstance();
    assertEquals(clazz.getName(), obj.getClass().getName());

    try {
      Class.forName(classToLoad);
      fail("Should have thrown ClassNotFoundException");
    } catch (ClassNotFoundException expected) {
      // Expected
    }

    Class<?> clazzForName = Class.forName(classToLoad, true, gcl);
    assertNotNull(clazzForName);
    assertEquals(clazz, clazzForName);

    Object objForName = clazzForName.newInstance();
    assertEquals(classToLoad, objForName.getClass().getName());
  }

  /**
   * Verifies that custom loader is used to load class.
   */
  @Test
  public void testForNameWithCustomLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testForNameWithCustomLoader");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    dcl = dcl.addOrReplace(new GeneratingClassLoader());

    String classToLoad = "com.nowhere.TestForNameWithCustomLoader";
    Class<?> clazz = dcl.forName(classToLoad);
    assertNotNull(clazz);
    assertEquals(classToLoad, clazz.getName());

    Object obj = clazz.newInstance();
    assertEquals(classToLoad, obj.getClass().getName());
  }

  /**
   * Verifies that {@link Class#forName(String, boolean, ClassLoader)} used with {@link ClassPathLoader} works as
   * expected with named object arrays, while {@link ClassLoader#loadClass(String)} throws ClassNotFoundException for
   * named object arrays.
   */
  @Test
  public void testForNameWithObjectArray() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testForNameWithObjectArray");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);

    String classToLoad = "[Ljava.lang.String;";
    Class<?> clazz = null;
    clazz = dcl.forName(classToLoad);
    assertEquals(classToLoad, clazz.getName());
  }

  /**
   * Verifies that TCCL finds the class when {@link Class#forName(String, boolean, ClassLoader)} uses
   * {@link ClassPathLoader}.
   */
  @Test
  public void testForNameWithTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testForNameWithTCCL");

    final ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    final String classToLoad = "com.nowhere.TestForNameWithTCCL";

    try {
      dcl.forName(classToLoad);
      fail("Should have thrown ClassNotFoundException");
    } catch (ClassNotFoundException expected) {
      // Expected
    }

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // ensure that TCCL is only CL that can find this class
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      Class<?> clazz = dcl.forName(classToLoad);
      assertNotNull(clazz);
      Object instance = clazz.newInstance();
      assertNotNull(instance);
      assertEquals(classToLoad, instance.getClass().getName());
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }

    try {
      dcl.forName(classToLoad);
      fail("Should have thrown ClassNotFoundException");
    } catch (ClassNotFoundException expected) {
      // Expected
    }

  }

  /**
   * Verifies that the {@link NullClassLoader} works and never finds the named class. This is a control which ensures
   * that tests depending on <tt>NullClassLoader</tt> are valid.
   */
  @Test
  public void testNullClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testNullClassLoader");

    ClassLoader cl = new NullClassLoader();
    String classToLoad = "java.lang.String";

    try {
      Class.forName(classToLoad, true, cl);
      fail();
    } catch (ClassNotFoundException expected) {
      // Expected
    }

    String resourceToGet = "java/lang/String.class";

    URL url = cl.getResource(resourceToGet);
    assertNull(url);

    InputStream is = cl.getResourceAsStream(resourceToGet);
    assertNull(is);
  }

  /**
   * Verifies that the {@link SimpleClassLoader} works and finds classes that the parent can find. This is a control
   * which ensures that tests depending on <tt>SimpleClassLoader</tt> are valid.
   */
  @Test
  public void testSimpleClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testSimpleClassLoader");

    ClassLoader cl = new SimpleClassLoader(getClass().getClassLoader());
    String classToLoad = "java.lang.String";

    Class<?> clazz = Class.forName(classToLoad, true, cl);
    assertNotNull(clazz);

    String resourceToGet = "java/lang/String.class";

    URL url = cl.getResource(resourceToGet);
    assertNotNull(url);

    InputStream is = cl.getResourceAsStream(resourceToGet);
    assertNotNull(is);
  }

  /**
   * Verifies that the {@link BrokenClassLoader} is broken and always throws errors. This is a control which ensures
   * that tests depending on <tt>BrokenClassLoader</tt> are valid.
   */
  @Test
  public void testBrokenClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testBrokenClassLoader");

    ClassLoader cl = new BrokenClassLoader();

    String classToLoad = "java.lang.String";
    try {
      Class.forName(classToLoad, true, cl);
      fail();
    } catch (ClassNotFoundException e) {
      throw e;
    } catch (BrokenError expected) {
      // Expected
    }

    String resourceToGet = "java/lang/String.class";
    try {
      cl.getResource(resourceToGet);
      fail();
    } catch (BrokenError expected) {
      // Expected
    }
    try {
      cl.getResourceAsStream(resourceToGet);
      fail();
    } catch (BrokenError expected) {
      // Expected
    }
  }

  /**
   * Verifies that the {@link BrokenClassLoader} is broken and always throws errors even when used as a TCCL from
   * {@link ClassPathLoader}. This is primarily a control which ensures that tests depending on
   * <tt>BrokenClassLoader</tt> are valid, but it also verifies that TCCL is included by default by
   * <tt>ClassPathLoader</tt>.
   */
  @Test
  public void testBrokenTCCLThrowsErrors() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testBrokenTCCLThrowsErrors");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    dcl.addOrReplace(new NullClassLoader());

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // set the TCCL to throw errors
      Thread.currentThread().setContextClassLoader(new BrokenClassLoader());

      String classToLoad = "java.lang.String";
      try {
        dcl.forName(classToLoad);
        fail();
      } catch (ClassNotFoundException e) {
        throw e;
      } catch (BrokenError expected) {
        // Expected
      }

      String resourceToGet = "java/lang/String.class";
      try {
        dcl.getResource(resourceToGet);
        fail();
      } catch (BrokenError expected) {
        // Expected
      }

      try {
        dcl.getResourceAsStream(resourceToGet);
        fail();
      } catch (BrokenError expected) {
        // Expected
      }
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that the class classloader or system classloader will find the class or resource. Parent is a
   * {@link NullClassLoader} while the TCCL is an excluded {@link BrokenClassLoader}.
   */
  @Test
  public void testEverythingWithDefaultLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testEverythingWithDefaultLoader");

    // create DCL such that parent cannot find anything
    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(true);
    dcl.addOrReplace(new NullClassLoader());

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // set the TCCL to never find anything
      Thread.currentThread().setContextClassLoader(new BrokenClassLoader());

      String classToLoad = "java.lang.String";
      Class<?> clazz = dcl.forName(classToLoad);
      assertNotNull(clazz);

      String resourceToGet = "java/lang/String.class";
      URL url = dcl.getResource(resourceToGet);
      assertNotNull(url);
      InputStream is = dcl.getResourceAsStream(resourceToGet);
      assertNotNull(is);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that the 3rd custom loader will find the class. Parent cannot find it and TCCL is broken. This verifies
   * that all custom loaders are checked and that the custom loaders are all checked before TCCL.
   */
  @Test
  public void testForNameWithMultipleCustomLoaders() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testForNameWithMultipleCustomLoaders");

    // create DCL such that the 3rd loader should find the class
    // first custom loader becomes parent which won't find anything
    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    final GeneratingClassLoader generatingClassLoader = new GeneratingClassLoader();
    dcl = dcl.addOrReplace(generatingClassLoader);
    dcl = dcl.addOrReplace(new SimpleClassLoader(getClass().getClassLoader()));
    dcl = dcl.addOrReplace(new NullClassLoader());

    String classToLoad = "com.nowhere.TestForNameWithMultipleCustomLoaders";

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // set TCCL to throw errors which makes sure we find before checking TCCL
      Thread.currentThread().setContextClassLoader(new BrokenClassLoader());

      Class<?> clazz = dcl.forName(classToLoad);
      assertNotNull(clazz);
      assertEquals(classToLoad, clazz.getName());
      assertTrue("Class not loaded by a GeneratingClassLoader.", clazz.getClassLoader() instanceof GeneratingClassLoader);
      assertEquals("Class not loaded by generatingClassLoader.", generatingClassLoader, clazz.getClassLoader());

      Object obj = clazz.newInstance();
      assertEquals(classToLoad, obj.getClass().getName());
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that setting <tt>excludeThreadContextClassLoader</tt> to true will indeed exclude the TCCL.
   */
  @Test
  public void testExcludeTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testExcludeTCCL");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(true);

    String classToLoad = "com.nowhere.TestExcludeTCCL";

    try {
      dcl.forName(classToLoad);
      fail("Should have thrown ClassNotFoundException");
    } catch (ClassNotFoundException expected) {
      // Expected
    }

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // ensure that TCCL is only CL that can find this class
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      dcl.forName(classToLoad);
      fail("Should have thrown ClassNotFoundException");
    } catch (ClassNotFoundException expected) {
      // Expected
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }

  }


  /**
   * Verifies that <tt>getResource</tt> will skip TCCL if <tt>excludeThreadContextClassLoader</tt> has been set to true.
   */
  @Test
  public void testGetResourceExcludeTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceExcludeTCCL");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(true);

    String resourceToGet = "com/nowhere/testGetResourceExcludeTCCL.rsc";
    assertNull(dcl.getResource(resourceToGet));

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // ensure that TCCL is only CL that can find this resource
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      assertNull(dcl.getResource(resourceToGet));
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that <tt>getResourceAsStream</tt> will skip TCCL if <tt>excludeThreadContextClassLoader</tt> has been set
   * to true.
   */
  @Test
  public void testGetResourceAsStreamExcludeTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceAsStreamExcludeTCCL");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(true);

    String resourceToGet = "com/nowhere/testGetResourceAsStreamExcludeTCCL.rsc";
    assertNull(dcl.getResourceAsStream(resourceToGet));

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // ensure that TCCL is only CL that can find this resource
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      assertNull(dcl.getResourceAsStream(resourceToGet));
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  @Test
  public void testAddFindsLatestClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testAddFindsLatestClassLoader");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    dcl = dcl.addOrReplace(new GeneratingClassLoader());

    String classToLoad = "com.nowhere.TestAddFindsLatestClassLoader";
    Class<?> clazz = dcl.forName(classToLoad);
    assertNotNull(clazz);

    dcl = dcl.addOrReplace(new BrokenClassLoader());

    try {
      dcl.forName(classToLoad);
      fail();
    } catch (BrokenError expected) {
      // Expected
    }
  }

  /**
   * Verifies removing a ClassLoader.
   */
  @Test
  public void testRemoveClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testRemoveClassLoader");

    GeneratingClassLoader genClassLoader = new GeneratingClassLoader();
    ClassPathLoader cpl = ClassPathLoader.createWithDefaults(false);
    cpl = cpl.addOrReplace(genClassLoader);

    String classToLoad = "com.nowhere.TestRemoveClassLoader";
    Class<?> clazz = cpl.forName(classToLoad);
    assertNotNull(clazz);

    cpl = cpl.remove(genClassLoader);

    try {
      clazz = cpl.forName(classToLoad);
      fail();
    } catch (ClassNotFoundException expected) {
      // Expected
    }
  }

  /**
   * Verifies that a ClassLoader will be replaced when added more than once.
   */
  @Test
  public void testClassLoaderReplace() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testClassLoaderReplace");

    String class1ToLoad = "ClassA";
    String class2ToLoad = "ClassB";

    ClassPathLoader cpl = ClassPathLoader.createWithDefaults(false);
    cpl = cpl.addOrReplace(new OneClassClassLoader(class1ToLoad));

    try {
      @SuppressWarnings("unused")
      Class<?> clazz = cpl.forName(class1ToLoad);
    } catch (ClassNotFoundException unexpected) {
      fail();
    }

    try {
      @SuppressWarnings("unused")
      Class<?> clazz = cpl.forName(class2ToLoad);
      fail();
    } catch (ClassNotFoundException expected) {
      // Expected
    }

    cpl = cpl.addOrReplace(new OneClassClassLoader(class2ToLoad));
    try {
      @SuppressWarnings("unused")
      Class<?> clazz = cpl.forName(class2ToLoad);
    } catch (ClassNotFoundException unexpected) {
      fail();
    }

    try {
      @SuppressWarnings("unused")
      Class<?> clazz = cpl.forName(class1ToLoad);
      fail();
    } catch (ClassNotFoundException expected) {
      // Expected
    }
  }

  @Test
  public void testAsClassLoaderLoadClassWithMultipleCustomLoaders() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testAsClassLoaderLoadClassWithMultipleCustomLoaders");

    // create DCL such that the 3rd loader should find the class
    // first custom loader becomes parent which won't find anything
    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    final GeneratingClassLoader generatingClassLoader = new GeneratingClassLoader();
    dcl = dcl.addOrReplace(generatingClassLoader);
    dcl = dcl.addOrReplace(new SimpleClassLoader(getClass().getClassLoader()));
    dcl = dcl.addOrReplace(new NullClassLoader());

    final String classToLoad = "com.nowhere.TestForNameWithMultipleCustomLoaders";

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // set TCCL to throw errors which makes sure we find before checking TCCL
      Thread.currentThread().setContextClassLoader(new BrokenClassLoader());

      final ClassLoader classLoader = dcl.asClassLoader();
      final Class<?> clazz = classLoader.loadClass(classToLoad);
      assertNotNull(clazz);
      assertEquals(classToLoad, clazz.getName());
      assertTrue(clazz.getClassLoader() instanceof GeneratingClassLoader);
      assertEquals(generatingClassLoader, clazz.getClassLoader());

      final Object obj = clazz.newInstance();
      assertEquals(classToLoad, obj.getClass().getName());
      
      final Class<?> clazz2 = dcl.forName(classToLoad);
      assertSame("Should load same class as calling classLoader.", clazz, clazz2);

      final Class<?> clazz3 = Class.forName(classToLoad, true, classLoader);
      assertSame("Should load same class as calling classLoader.", clazz, clazz3);

    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  private static void exploreClassLoaders() {
    System.out.println("Thread.currentThread().getContextClassLoader()...");
    exploreClassLoader(Thread.currentThread().getContextClassLoader(), 1);

    System.out.println("class.getClassLoader()...");
    exploreClassLoader(ClassPathLoaderTest.class.getClassLoader(), 1);

    System.out.println("ClassLoader.getSystemClassLoader()...");
    exploreClassLoader(ClassLoader.getSystemClassLoader(), 1);
  }

  private static void exploreClassLoader(ClassLoader cl, int indent) {
    String prefix = "";
    for (int i = 0; i < indent; i++) {
      prefix += "\t";
    }
    System.out.println(prefix + "ClassLoader toString() = " + cl);

    Class<?> clazz = cl.getClass();
    System.out.println(prefix + "ClassLoader getClass().getName() = " + clazz.getName());
    exploreClassLoaderSuperClass(prefix, clazz);

    try {
      URL[] urls = ((URLClassLoader) cl).getURLs();
      StringBuilder sb = new StringBuilder(prefix).append("ClassLoader getURLs = [");
      for (int i = 0; i < urls.length; i++) {
        if (i > 0)
          sb.append(", ");
        sb.append(urls[i].toString());
      }
      sb.append("]");
      System.out.println(sb.toString());
    } catch (Exception e) {
      System.out.println(prefix + "ClassLoader is not a URLClassLoader");
    }

    ClassLoader parent = cl.getParent();
    if (parent != null) {
      System.out.println(prefix + "ClassLoader has parent...");
      exploreClassLoader(parent, ++indent);
    }
  }

  private static void exploreClassLoaderSuperClass(String prefix, Class<?> clazz) {
    Class<?> superClazz = clazz.getSuperclass();
    if (superClazz != null) {
      System.out.println(prefix + "                       getSuperclass().getName() = " + superClazz.getName());
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
   * Custom class loader which uses BCEL to always dynamically generate a class for any class name it tries to load.
   */
  static class GeneratingClassLoader extends ClassLoader {

    /**
     * Currently unused but potentially useful for some future test. This causes this loader to only generate a class
     * that the parent could not find.
     *
     * @param parent
     *          the parent class loader to check with first
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
      ClassGen cg = new ClassGen(name, "java.lang.Object", "<generated>", Constants.ACC_PUBLIC | Constants.ACC_SUPER, null);
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

  static class OneClassClassLoader extends ClassLoader {

    private final GeneratingClassLoader genClassLoader = new GeneratingClassLoader();
    private String className;

    public OneClassClassLoader(final String className) {
      super(null); // no parent!!
      this.className = className;
    }
    
    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
      if (!name.equals(className)) {
        throw new ClassNotFoundException();
      } else {
        return this.genClassLoader.findClass(name);
      }
    }
    
    @Override
    public boolean equals(final Object other) {
      return (other instanceof OneClassClassLoader);
    }
  }
  
  @SuppressWarnings("serial")
  static class BrokenError extends Error {
  }
}
