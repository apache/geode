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
package com.gemstone.gemfire.management.internal.web.util;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.management.internal.web.io.MultipartFileAdapter;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.springframework.web.multipart.MultipartFile;

/**
 * The ConvertUtilsJUnitTest class is a test suite testing the contract and functionality of the ConvertUtilsJUnitTest class.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.web.util.ConvertUtils
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 8.0
 */
@Category(UnitTest.class)
public class ConvertUtilsJUnitTest {

  protected MultipartFile createMultipartFile(final String filename, final byte[] content) {
    return new MultipartFileAdapter() {
      @Override public byte[] getBytes() throws IOException {
        return content;
      }
      @Override public InputStream getInputStream() throws IOException {
        return new ByteArrayInputStream(getBytes());
      }
      @Override public String getName() {
        return filename;
      }
      @Override public String getOriginalFilename() {
        return filename;
      }
      @Override public long getSize() {
        return content.length;
      }
    };
  }

  protected Resource createResource(final String filename, final byte[] content) {
    return new ByteArrayResource(content, String.format("Content of file (%1$s).", filename)) {
      @Override public String getFilename() {
        return filename;
      }
    };
  }

  @Test
  public void testConvertFileData() throws IOException {
    final String[] filenames = { "/path/to/file1.ext", "/path/to/another/file2.ext" };
    final String[] fileContent = { "This is the contents of file 1.", "This is the contents of file 2." };

    final List<byte[]> fileData = new ArrayList<byte[]>(2);

    for (int index = 0; index < filenames.length; index++) {
      fileData.add(filenames[index].getBytes());
      fileData.add(fileContent[index].getBytes());
    }

    final Resource[] resources = ConvertUtils.convert(fileData.toArray(new byte[fileData.size()][]));

    assertNotNull(resources);
    assertEquals(filenames.length, resources.length);

    for (int index = 0; index < resources.length; index++) {
      assertEquals(filenames[index], resources[index].getFilename());
      assertEquals(fileContent[index], new String(IOUtils.toByteArray(resources[index].getInputStream())));
    }
  }

  @Test
  public void testConvertFileDataWithNull() {
    final Resource[] resources = ConvertUtils.convert((byte[][]) null);

    assertNotNull(resources);
    assertEquals(0, resources.length);
  }

  @Test
  public void testConvertMultipartFile() throws IOException {
    final MultipartFile[] files = {
      createMultipartFile("/path/to/multi-part/file1.txt", "The contents of multi-part file1.".getBytes()),
      createMultipartFile("/path/to/multi-part/file2.txt", "The contents of multi-part file2.".getBytes())
    };

    final byte[][] fileData = ConvertUtils.convert(files);

    assertNotNull(fileData);
    assertEquals(files.length * 2, fileData.length);

    for (int index = 0; index < fileData.length; index += 2) {
      assertEquals(files[index / 2].getOriginalFilename(), new String(fileData[index]));
      assertEquals(new String(files[index / 2].getBytes()), new String(fileData[index + 1]));
    }
  }

  @Test
  public void testConvertResource() throws IOException {
    final Resource[] resources = {
      createResource("/path/to/file1.txt", "Contents of file1.".getBytes()),
      createResource("/path/to/file2.txt", "Contents of file2.".getBytes())
    };

    final byte[][] fileData = ConvertUtils.convert(resources);

    assertNotNull(fileData);
    assertEquals(resources.length * 2, fileData.length);

    for (int index = 0; index < fileData.length; index += 2) {
      assertEquals(resources[index / 2].getFilename(), new String(fileData[index]));
      assertEquals(new String(IOUtils.toByteArray(resources[index / 2].getInputStream())), new String(fileData[index + 1]));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConvertResourceWithResourceHavingNoFilename() throws IOException {
    try {
      ConvertUtils.convert(createResource(null, "test".getBytes()));
    }
    catch (IllegalArgumentException expected) {
      assertEquals("The filename of Resource (Byte array resource [Content of file (null).]) must be specified!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testConvertResourceWithEmpty() throws IOException {
    final byte[][] fileData = ConvertUtils.convert(new Resource[0]);

    assertNotNull(fileData);
    assertEquals(0, fileData.length);
  }

  @Test
  public void testConvertResourceWithNull() throws IOException {
    final byte[][] fileData = ConvertUtils.convert((Resource[]) null);

    assertNotNull(fileData);
    assertEquals(0, fileData.length);
  }

}
