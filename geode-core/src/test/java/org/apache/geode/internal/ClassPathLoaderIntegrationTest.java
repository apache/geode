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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.Vector;

import org.apache.bcel.Constants;
import org.apache.bcel.classfile.JavaClass;
import org.apache.bcel.generic.ClassGen;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.internal.ClassPathLoaderTest.BrokenClassLoader;
import org.apache.geode.internal.ClassPathLoaderTest.NullClassLoader;
import org.apache.geode.internal.ClassPathLoaderTest.SimpleClassLoader;

import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Integration tests for {@link ClassPathLoader}.
 *
 * Extracted from ClassPathLoaderTest.
 */
@Category(IntegrationTest.class)
public class ClassPathLoaderIntegrationTest {

  private static final int TEMP_FILE_BYTES_COUNT = 256;

  private volatile File tempFile;
  private volatile File tempFile2;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    System.setProperty(ClassPathLoader.EXCLUDE_TCCL_PROPERTY, "false");
    System.setProperty(ClassPathLoader.EXT_LIB_DIR_PARENT_PROPERTY, this.temporaryFolder.getRoot().getAbsolutePath());

    this.tempFile = this.temporaryFolder.newFile("tempFile1.tmp");
    FileOutputStream fos = new FileOutputStream(this.tempFile);
    fos.write(new byte[TEMP_FILE_BYTES_COUNT]);
    fos.close();

    this.tempFile2 = this.temporaryFolder.newFile("tempFile2.tmp");
    fos = new FileOutputStream(this.tempFile2);
    fos.write(new byte[TEMP_FILE_BYTES_COUNT]);
    fos.close();
  }

  /**
   * Verifies that <tt>getResource</tt> works with custom loader from {@link ClassPathLoader}.
   */
  @Test
  public void testGetResourceWithCustomLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceWithCustomLoader");

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
  @Test
  public void testGetResourcesWithCustomLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceWithCustomLoader");

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
  @Test
  public void testGetResourceAsStreamWithCustomLoader() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceAsStreamWithCustomLoader");

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
  @Test
  public void testGetResourceWithTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceWithTCCL");

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
  @Test
  public void testGetResourcesWithTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceWithTCCL");

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
  @Test
  public void testGetResourceAsStreamWithTCCL() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceAsStreamWithTCCL");

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
   * Verifies that JAR files found in the extlib directory will be correctly
   * added to the {@link ClassPathLoader}.
   */
  @Test
  public void testJarsInExtLib() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testJarsInExtLib");

    File EXT_LIB_DIR = ClassPathLoader.defineEXT_LIB_DIR();
    EXT_LIB_DIR.mkdir();

    File subdir = new File(EXT_LIB_DIR, "cplju");
    subdir.mkdir();

    final ClassBuilder classBuilder = new ClassBuilder();

    writeJarBytesToFile(new File(EXT_LIB_DIR, "ClassPathLoaderJUnit1.jar"),
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

  /**
   * Verifies that the 3rd custom loader will get the resource. Parent cannot find it and TCCL is broken. This verifies
   * that all custom loaders are checked and that the custom loaders are all checked before TCCL.
   */
  @Test
  public void testGetResourceAsStreamWithMultipleCustomLoaders() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceAsStreamWithMultipleCustomLoaders");

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
   * Verifies that the 3rd custom loader will get the resource. Parent cannot find it and TCCL is broken. This verifies
   * that all custom loaders are checked and that the custom loaders are all checked before TCCL.
   */
  @Test
  public void testGetResourceWithMultipleCustomLoaders() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceWithMultipleCustomLoaders");

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
  @Test
  public void testGetResourcesWithMultipleCustomLoaders() throws Exception {
    System.out.println("\nStarting ClassPathLoaderTest#testGetResourceWithMultipleCustomLoaders");

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

  private void writeJarBytesToFile(File jarFile, byte[] jarBytes) throws IOException {
    final OutputStream outStream = new FileOutputStream(jarFile);
    outStream.write(jarBytes);
    outStream.close();
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
      return tempFile;
    }
  }

  /**
   * Custom class loader which uses BCEL to always dynamically generate a class for any class name it tries to load.
   */
  private class GeneratingClassLoader2 extends GeneratingClassLoader {
    @Override
    protected File getTempFile() {
      return tempFile2;
    }
  }
}
