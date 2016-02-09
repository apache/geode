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
   * Tests {@link SnappyCompressor#SnappyCompressor()} constructor.
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
