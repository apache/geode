/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.Vector;

import junit.framework.TestCase;

import org.apache.bcel.Constants;
import org.apache.bcel.classfile.JavaClass;
import org.apache.bcel.generic.ClassGen;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

/**
 * Test the {@link ClassPathLoader}.
 * 
 * @author Kirk Lund
 * @since 6.5.1.4
 */
@Category(UnitTest.class)
public class ClassPathLoaderJUnitTest extends TestCase {

  private static final int TEMP_FILE_BYTES_COUNT = 256;
  private static final int GENERATED_CLASS_BYTES_COUNT = 362;

  private static final String ORIGINAL_EXCLUDE_TCCL_VALUE;
  
  private boolean deleteExtDir = false;
  private boolean deleteLibDir = false;

  static {
    ORIGINAL_EXCLUDE_TCCL_VALUE = System.getProperty(ClassPathLoader.EXCLUDE_TCCL_PROPERTY, Boolean
        .toString(ClassPathLoader.EXCLUDE_TCCL_DEFAULT_VALUE));

    exploreClassLoaders(); // optional output for developer's convenience
  }

  private volatile File tempFile;
  private volatile File tempFile2;

  public ClassPathLoaderJUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    System.setProperty(ClassPathLoader.EXCLUDE_TCCL_PROPERTY, "false");

    File workingDir = new File(System.getProperty("user.dir")).getAbsoluteFile();
    this.tempFile = File.createTempFile("ClassPathLoaderJUnitTest.", null, workingDir);
    FileOutputStream fos = new FileOutputStream(this.tempFile);
    fos.write(new byte[TEMP_FILE_BYTES_COUNT]);
    fos.close();

    this.tempFile2 = File.createTempFile("ClassPathLoaderJUnitTest.", null, workingDir);
    fos = new FileOutputStream(this.tempFile2);
    fos.write(new byte[TEMP_FILE_BYTES_COUNT]);
    fos.close();
  }

  @Override
  public void tearDown() throws Exception {
    System.setProperty(ClassPathLoader.EXCLUDE_TCCL_PROPERTY, ORIGINAL_EXCLUDE_TCCL_VALUE);
    
    // these deletions fail on windows
//    if (this.deleteLibDir) {
//      FileUtil.delete(ClassPathLoader.EXT_LIB_DIR.getParentFile());
//    } else {
//      if (this.deleteExtDir) {
//        FileUtil.delete(ClassPathLoader.EXT_LIB_DIR);
//      } else {
//        FileUtil.delete(new File(ClassPathLoader.EXT_LIB_DIR, "ClassPathLoaderJUnit1.jar"));
//        FileUtil.delete(new File(ClassPathLoader.EXT_LIB_DIR, "cplju"));
//      }
//    }
    
    assertTrue(this.tempFile.delete());
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} is always initialized and returns a <tt>ClassPathLoader</tt>
   * instance.
   */
  public void testLatestExists() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testLatestExists");

    assertNotNull(ClassPathLoader.getLatest());
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} throws <tt>ClassNotFoundException</tt> when class does not exist.
   */
  public void testForNameThrowsClassNotFoundException() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testForNameThrowsClassNotFoundException");

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
  public void testForName() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testForName");

    String classToLoad = "com.gemstone.gemfire.internal.classpathloaderjunittest.DoesExist";
    Class<?> clazz = ClassPathLoader.getLatest().forName(classToLoad);
    assertNotNull(clazz);
  }

  /**
   * Verifies that {@link ClassPathLoader#getLatest()} can actually <tt>getResource</tt> when it exists.
   */
  public void testGetResource() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResource");

    String resourceToGet = "com/gemstone/gemfire/internal/classpathloaderjunittest/DoesExist.class";
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
  public void testGetResources() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResources");

    String resourceToGet = "com/gemstone/gemfire/internal/classpathloaderjunittest/DoesExist.class";
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
  public void testGetResourceAsStream() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResourceAsStream");

    String resourceToGet = "com/gemstone/gemfire/internal/classpathloaderjunittest/DoesExist.class";
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
  public void testGeneratingClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGeneratingClassLoader");

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
  public void testForNameWithCustomLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testForNameWithCustomLoader");

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
  public void testForNameWithObjectArray() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testForNameWithObjectArray");

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
  public void testForNameWithTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testForNameWithTCCL");

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
  public void testNullClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testNullClassLoader");

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
  public void testSimpleClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testSimpleClassLoader");

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
  public void testBrokenClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testBrokenClassLoader");

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
  public void testBrokenTCCLThrowsErrors() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testBrokenTCCLThrowsErrors");

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
  public void testEverythingWithDefaultLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testEverythingWithDefaultLoader");

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
  public void testForNameWithMultipleCustomLoaders() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testForNameWithMultipleCustomLoaders");

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
   * Verifies that the 3rd custom loader will get the resource. Parent cannot find it and TCCL is broken. This verifies
   * that all custom loaders are checked and that the custom loaders are all checked before TCCL.
   */
  public void testGetResourceWithMultipleCustomLoaders() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResourceWithMultipleCustomLoaders");

    // create DCL such that the 3rd loader should find the resource
    // first custom loader becomes parent which won't find anything
    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    dcl = dcl.addOrReplace(new GeneratingClassLoader());
    dcl = dcl.addOrReplace(new SimpleClassLoader(getClass().getClassLoader()));
    dcl = dcl.addOrReplace(new NullClassLoader());

    String resourceToGet = "com/nowhere/testGetResourceWithMultipleCustomLoaders.rsc";

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // set TCCL to throw errors which makes sure we find before checking TCCL
      Thread.currentThread().setContextClassLoader(new BrokenClassLoader());

      URL url = dcl.getResource(resourceToGet);
      assertNotNull(url);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that the 3rd custom loader will get the resources. Parent cannot find it and TCCL is broken. This verifies
   * that all custom loaders are checked and that the custom loaders are all checked before TCCL.
   */
  public void testGetResourcesWithMultipleCustomLoaders() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResourceWithMultipleCustomLoaders");

    // create DCL such that the 3rd loader should find the resource
    // first custom loader becomes parent which won't find anything
    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    dcl = dcl.addOrReplace(new GeneratingClassLoader());
    dcl = dcl.addOrReplace(new GeneratingClassLoader2());
    dcl = dcl.addOrReplace(new SimpleClassLoader(getClass().getClassLoader()));
    dcl = dcl.addOrReplace(new NullClassLoader());

    String resourceToGet = "com/nowhere/testGetResourceWithMultipleCustomLoaders.rsc";

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // set TCCL to throw errors which makes sure we find before checking TCCL
      Thread.currentThread().setContextClassLoader(new BrokenClassLoader());

      Enumeration<URL> urls = dcl.getResources(resourceToGet);
      assertNotNull(urls);
      assertTrue(urls.hasMoreElements());
      
      URL url = urls.nextElement();
      assertNotNull(url);
      
      // Should find two with unique URLs
      assertTrue("Did not find all resources.", urls.hasMoreElements());
      URL url2 = urls.nextElement();
      assertNotNull(url2);
      assertTrue("Resource URLs should be unique.", !url.equals(url2));
      
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that the 3rd custom loader will get the resource. Parent cannot find it and TCCL is broken. This verifies
   * that all custom loaders are checked and that the custom loaders are all checked before TCCL.
   */
  public void testGetResourceAsStreamWithMultipleCustomLoaders() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResourceAsStreamWithMultipleCustomLoaders");

    // create DCL such that the 3rd loader should find the resource
    // first custom loader becomes parent which won't find anything
    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    dcl = dcl.addOrReplace(new GeneratingClassLoader());
    dcl = dcl.addOrReplace(new SimpleClassLoader(getClass().getClassLoader()));
    dcl = dcl.addOrReplace(new NullClassLoader());

    String resourceToGet = "com/nowhere/testGetResourceAsStreamWithMultipleCustomLoaders.rsc";

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // set TCCL to throw errors which makes sure we find before checking TCCL
      Thread.currentThread().setContextClassLoader(new BrokenClassLoader());

      InputStream is = dcl.getResourceAsStream(resourceToGet);
      assertNotNull(is);
      is.close();
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that setting <tt>excludeThreadContextClassLoader</tt> to true will indeed exclude the TCCL.
   */
  public void testExcludeTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testExcludeTCCL");

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
   * Verifies that <tt>getResource</tt> works with custom loader from {@link ClassPathLoader}.
   */
  public void testGetResourceWithCustomLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResourceWithCustomLoader");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    dcl = dcl.addOrReplace(new GeneratingClassLoader());

    String resourceToGet = "com/nowhere/testGetResourceWithCustomLoader.rsc";
    URL url = dcl.getResource(resourceToGet);
    assertNotNull(url);

    InputStream is = url != null ? url.openStream() : null;
    assertNotNull(is);

    int totalBytesRead = 0;
    byte[] input = new byte[128];

    BufferedInputStream bis = new BufferedInputStream(is);
    for (int bytesRead = bis.read(input); bytesRead > -1;) {
      totalBytesRead += bytesRead;
      bytesRead = bis.read(input);
    }
    bis.close();

    assertEquals(TEMP_FILE_BYTES_COUNT, totalBytesRead);
  }

  /**
   * Verifies that <tt>getResources</tt> works with custom loader from {@link ClassPathLoader}.
   */
  public void testGetResourcesWithCustomLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResourceWithCustomLoader");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    dcl = dcl.addOrReplace(new GeneratingClassLoader());

    String resourceToGet = "com/nowhere/testGetResourceWithCustomLoader.rsc";
    Enumeration<URL> urls = dcl.getResources(resourceToGet);
    assertNotNull(urls);
    assertTrue(urls.hasMoreElements());

    URL url = urls.nextElement();
    InputStream is = url != null ? url.openStream() : null;
    assertNotNull(is);

    int totalBytesRead = 0;
    byte[] input = new byte[128];

    BufferedInputStream bis = new BufferedInputStream(is);
    for (int bytesRead = bis.read(input); bytesRead > -1;) {
      totalBytesRead += bytesRead;
      bytesRead = bis.read(input);
    }
    bis.close();

    assertEquals(TEMP_FILE_BYTES_COUNT, totalBytesRead);
  }

  /**
   * Verifies that <tt>getResourceAsStream</tt> works with custom loader from {@link ClassPathLoader}.
   */
  public void testGetResourceAsStreamWithCustomLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResourceAsStreamWithCustomLoader");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);
    dcl = dcl.addOrReplace(new GeneratingClassLoader());

    String resourceToGet = "com/nowhere/testGetResourceAsStreamWithCustomLoader.rsc";
    InputStream is = dcl.getResourceAsStream(resourceToGet);
    assertNotNull(is);

    int totalBytesRead = 0;
    byte[] input = new byte[128];

    BufferedInputStream bis = new BufferedInputStream(is);
    for (int bytesRead = bis.read(input); bytesRead > -1;) {
      totalBytesRead += bytesRead;
      bytesRead = bis.read(input);
    }
    bis.close();

    assertEquals(TEMP_FILE_BYTES_COUNT, totalBytesRead);
  }

  /**
   * Verifies that <tt>getResource</tt> works with TCCL from {@link ClassPathLoader}.
   */
  public void testGetResourceWithTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResourceWithTCCL");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);

    String resourceToGet = "com/nowhere/testGetResourceWithTCCL.rsc";
    assertNull(dcl.getResource(resourceToGet));

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      URL url = dcl.getResource(resourceToGet);
      assertNotNull(url);

      InputStream is = url.openStream();
      assertNotNull(is);

      int totalBytesRead = 0;
      byte[] input = new byte[128];

      BufferedInputStream bis = new BufferedInputStream(is);
      for (int bytesRead = bis.read(input); bytesRead > -1;) {
        totalBytesRead += bytesRead;
        bytesRead = bis.read(input);
      }
      bis.close();

      assertEquals(TEMP_FILE_BYTES_COUNT, totalBytesRead);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that <tt>getResources</tt> works with TCCL from {@link ClassPathLoader}.
   */
  public void testGetResourcesWithTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResourceWithTCCL");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);

    String resourceToGet = "com/nowhere/testGetResourceWithTCCL.rsc";
    Enumeration<URL> urls = dcl.getResources(resourceToGet);
    assertNotNull(urls);
    assertFalse(urls.hasMoreElements());

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      urls = dcl.getResources(resourceToGet);
      assertNotNull(urls);

      URL url = urls.nextElement();
      InputStream is = url.openStream();
      assertNotNull(is);

      int totalBytesRead = 0;
      byte[] input = new byte[128];

      BufferedInputStream bis = new BufferedInputStream(is);
      for (int bytesRead = bis.read(input); bytesRead > -1;) {
        totalBytesRead += bytesRead;
        bytesRead = bis.read(input);
      }
      bis.close();

      assertEquals(TEMP_FILE_BYTES_COUNT, totalBytesRead);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that <tt>getResourceAsStream</tt> works with TCCL from {@link ClassPathLoader}.
   */
  public void testGetResourceAsStreamWithTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResourceAsStreamWithTCCL");

    ClassPathLoader dcl = ClassPathLoader.createWithDefaults(false);

    String resourceToGet = "com/nowhere/testGetResourceAsStreamWithTCCL.rsc";
    assertNull(dcl.getResourceAsStream(resourceToGet));

    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    try {
      // ensure that TCCL is only CL that can find this resource
      Thread.currentThread().setContextClassLoader(new GeneratingClassLoader());
      InputStream is = dcl.getResourceAsStream(resourceToGet);
      assertNotNull(is);

      int totalBytesRead = 0;
      byte[] input = new byte[128];

      BufferedInputStream bis = new BufferedInputStream(is);
      for (int bytesRead = bis.read(input); bytesRead > -1;) {
        totalBytesRead += bytesRead;
        bytesRead = bis.read(input);
      }
      bis.close();

      assertEquals(TEMP_FILE_BYTES_COUNT, totalBytesRead);
    } finally {
      Thread.currentThread().setContextClassLoader(cl);
    }
  }

  /**
   * Verifies that <tt>getResource</tt> will skip TCCL if <tt>excludeThreadContextClassLoader</tt> has been set to true.
   */
  public void testGetResourceExcludeTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResourceExcludeTCCL");

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
  public void testGetResourceAsStreamExcludeTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testGetResourceAsStreamExcludeTCCL");

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

  public void testAddFindsLatestClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testAddFindsLatestClassLoader");

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
  public void testRemoveClassLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testRemoveClassLoader");

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
  public void testClassLoaderReplace() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testClassLoaderReplace");

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

  /**
   * Verifies that JAR files found in the extlib directory will be correctly
   * added to the {@link ClassPathLoader}.
   */
  public void testJarsInExtLib() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testJarsInExtLib");

    this.deleteLibDir = ClassPathLoader.EXT_LIB_DIR.getParentFile().mkdirs();
    this.deleteExtDir = ClassPathLoader.EXT_LIB_DIR.mkdirs();
    
    File subdir = new File(ClassPathLoader.EXT_LIB_DIR, "cplju");
    subdir.mkdir();
    
    final ClassBuilder classBuilder = new ClassBuilder();
    
    writeJarBytesToFile(new File(ClassPathLoader.EXT_LIB_DIR, "ClassPathLoaderJUnit1.jar"),
        classBuilder.createJarFromClassContent("com/cpljunit1/ClassPathLoaderJUnit1", "package com.cpljunit1; public class ClassPathLoaderJUnit1 {}"));
    writeJarBytesToFile(new File(subdir, "ClassPathLoaderJUnit2.jar"),
        classBuilder.createJarFromClassContent("com/cpljunit2/ClassPathLoaderJUnit2", "package com.cpljunit2; public class ClassPathLoaderJUnit2 {}"));
    
    ClassPathLoader classPathLoader = ClassPathLoader.createWithDefaults(false);
    try {
      classPathLoader.forName("com.cpljunit1.ClassPathLoaderJUnit1");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }
    
    try {
      classPathLoader.forName("com.cpljunit2.ClassPathLoaderJUnit2");
    } catch (ClassNotFoundException cnfex) {
      fail("JAR file not correctly added to Classpath");
    }
    
    assertNotNull(classPathLoader.getResource("com/cpljunit2/ClassPathLoaderJUnit2.class"));
    
    Enumeration<URL> urls = classPathLoader.getResources("com/cpljunit1");
    if  (!urls.hasMoreElements()) {
      fail("Resources should return one element");
    }
  }
  
  @Test
  public void testAsClassLoaderLoadClassWithMultipleCustomLoaders() throws Exception {
    System.out.println("\nStarting ClassPathLoaderJUnitTest#testAsClassLoaderLoadClassWithMultipleCustomLoaders");

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

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.close();
  }
  
  private static void exploreClassLoaders() {
    System.out.println("Thread.currentThread().getContextClassLoader()...");
    exploreClassLoader(Thread.currentThread().getContextClassLoader(), 1);

    System.out.println("class.getClassLoader()...");
    exploreClassLoader(ClassPathLoaderJUnitTest.class.getClassLoader(), 1);

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
   * Custom class loader which uses BCEL to always dynamically generate a class for any class name it tries to load.
   */
  private class GeneratingClassLoader extends ClassLoader {

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
      return ClassPathLoaderJUnitTest.this.tempFile;
    }
  }

  /**
   * Custom class loader which uses BCEL to always dynamically generate a class for any class name it tries to load.
   */
  private class GeneratingClassLoader2 extends GeneratingClassLoader {
    @Override
    protected File getTempFile() {
      return ClassPathLoaderJUnitTest.this.tempFile2;
    }
  }
  
  /**
   * Custom class loader which will never find any class or resource.
   */
  private class NullClassLoader extends ClassLoader {
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
  private class SimpleClassLoader extends ClassLoader {
    public SimpleClassLoader(ClassLoader parent) {
      super(parent);
    }
  }

  private class OneClassClassLoader extends ClassLoader {
    private final GeneratingClassLoader genClassLoader = new GeneratingClassLoader();
    private String className;

    public OneClassClassLoader(final String className) {
      super(null); // no parent!!
      this.className = className;
    }
    
    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
      if (!name.equals(className))
        throw new ClassNotFoundException();

      else
        return this.genClassLoader.findClass(name);
    }
    
    @Override
    public boolean equals(final Object other) {
      return (other instanceof OneClassClassLoader);
    }
  }
  
  /**
   * Custom class loader which is broken and always throws errors.
   */
  private class BrokenClassLoader extends ClassLoader {
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

  @SuppressWarnings("serial")
  private class BrokenError extends Error {
  }
}
