/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import static org.junit.Assert.*;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * @author dsmith
 *
 */
@Category(UnitTest.class)
public class FileUtilJUnitTest {
  
  @Test
  public void testCopyFile() throws IOException {
    File source = File.createTempFile("FileUtilJUnitTest", null);
    File dest = File.createTempFile("FileUtilJUnitTest", null);
    try {
      FileOutputStream fos = new FileOutputStream(source);
      DataOutput daos = new DataOutputStream(fos);
      try {
        for(long i =0; i < FileUtil.MAX_TRANSFER_SIZE * 2.5 / 8; i++) {
          daos.writeLong(i);
        }
      } finally {
        fos.close();
      }
      FileUtil.copy(source, dest);

      FileInputStream fis = new FileInputStream(dest);
      DataInput dis = new DataInputStream(fis);
      try {
        for(long i =0; i < FileUtil.MAX_TRANSFER_SIZE * 2.5 / 8; i++) {
          assertEquals(i, dis.readLong());
        }
        assertEquals(-1, fis.read());
      } finally {
        fis.close();
      }
    } finally {
      source.delete();
      dest.delete();
    }
  }

  @Test
  public void testStripOffExtension() {
    String fileName = "filename";
    assertEquals("filename", FileUtil.stripOffExtension(fileName));
    fileName = "filename.txt";
    assertEquals("filename", FileUtil.stripOffExtension(fileName));
    fileName = "filename.txt.txt";
    assertEquals("filename.txt", FileUtil.stripOffExtension(fileName));
    fileName = "filename.txt.log";
    assertEquals("filename.txt", FileUtil.stripOffExtension(fileName));
    fileName = "/dir/dir/dir/dir/filename.txt.log";
    assertEquals("/dir/dir/dir/dir/filename.txt", FileUtil.stripOffExtension(fileName));
  }
}
