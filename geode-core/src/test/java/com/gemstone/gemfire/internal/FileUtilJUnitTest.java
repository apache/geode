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

import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
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

  @Test
  public void testDeleteFile() throws IOException {
    File file = File.createTempFile("FileUtilJUnitTest", null);
    assertTrue(file.exists());
    FileUtil.delete(file);
    assertFalse(file.exists());
  }

  @Test
  public void testDeleteDir() throws IOException {
    File dir = new File("testDirName");
    dir.mkdir();
    File file = File.createTempFile("testFile", null, dir);
    assertTrue(dir.exists());
    assertTrue(file.exists());
    FileUtil.delete(dir);
    assertFalse(file.exists());
    assertFalse(dir.exists());
  }
}
