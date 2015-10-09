/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.compression;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.xerial.snappy.SnappyLoader;

import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests the Snappy {@link Compressor}.
 * @author rholmes
 */
@Category(UnitTest.class)
public class SnappyCompressorJUnitTest extends TestCase {
  /**
   * Tests {@link Compressor#compress(byte[])} and {@link Compressor#decompress(byte[])} using
   * the Snappy compressor.
   */
  @Test
  public void testCompressByteArray() {
    String compressMe = "Hello, how are you?";
    byte[] compressMeData = SnappyCompressor.getDefaultInstance().compress(compressMe.getBytes());
    String uncompressedMe = new String(SnappyCompressor.getDefaultInstance().decompress(compressMeData));

    assertEquals(compressMe, uncompressedMe);
  }
  
  /**
   * Tests {@link SnappyCompressor()} constructor.
   * @throws SecurityException 
   * @throws NoSuchMethodException 
   * @throws InvocationTargetException 
   * @throws IllegalArgumentException 
   * @throws IllegalAccessException 
   */
  @Test
  public void testConstructor() throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    SnappyCompressor.getDefaultInstance();
    // repeat findNativeLibrary and make sure it's pointing at a file in tmpdir
    Method findNativeLibraryMethod = SnappyLoader.class.getDeclaredMethod("findNativeLibrary", new Class[0]);
    findNativeLibraryMethod.setAccessible(true);
    File nativeLibrary = (File) findNativeLibraryMethod.invoke(null);
    System.out.println(nativeLibrary);
    assertNotNull(nativeLibrary);
    assertTrue(nativeLibrary + " does not exist", nativeLibrary.exists());
    File tmpDir = new File(System.getProperty("java.io.tmpdir"));
    assertTrue(tmpDir.exists());
    File parent = nativeLibrary.getParentFile();
    assertNotNull(parent);
    assertEquals(tmpDir, parent);
  }
}