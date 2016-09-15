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
package org.apache.geode.internal.util;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;

import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.lib.legacy.ClassImposteriser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Test class for testing the contract and functionality of the IOUtils class.
 * <p/>
 * @see org.apache.geode.internal.util.IOUtils
 * @see org.jmock.Expectations
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 */
@Category(UnitTest.class)
public class IOUtilsJUnitTest {

  private Mockery mockContext;

  @Before
  public void setup() {
    mockContext = new Mockery() {{
      setImposteriser(ClassImposteriser.INSTANCE);
    }};
  }

  @After
  public void tearDown() {
    mockContext.assertIsSatisfied();
    mockContext = null;
  }

  /**
   * Gets a fully-qualified path anchored at root.
   * <p/>
   * @param pathElements a String array containing the elements of the path.
   * @return a fully-qualified pathname as a String value.
   */
  private String toPathname(final String... pathElements) {
    if (pathElements != null) {
      final StringBuilder buffer = new StringBuilder();
      for (String pathElement : pathElements) {
        buffer.append(File.separator);
        buffer.append(pathElement);
      }
      return buffer.toString();
    }

    return null;
  }

  @Test
  public void testAppendToPath() {
    assertEquals(null, IOUtils.appendToPath(null, (String[]) null));
    assertEquals(File.separator, IOUtils.appendToPath(null));
    assertEquals(File.separator, IOUtils.appendToPath(""));
    assertEquals(File.separator, IOUtils.appendToPath(" "));
    assertEquals(File.separator, IOUtils.appendToPath(File.separator));
    assertEquals(toPathname("bin", "a.out"), IOUtils.appendToPath(null, "bin", "a.out"));
    assertEquals(toPathname("bin", "a.out"), IOUtils.appendToPath(File.separator, "bin", "a.out"));
    assertEquals(toPathname("usr", "local", "bin", "a.out"), IOUtils
      .appendToPath(toPathname("usr", "local"), "bin", "a.out"));
  }

  @Test
  public void testClose() throws IOException {
    final Closeable mockCloseable = mockContext.mock(Closeable.class, "Closeable");

    mockContext.checking(new Expectations() {{
      oneOf(mockCloseable).close();
    }});

    IOUtils.close(mockCloseable);
  }

  @Test
  public void testCloseIgnoresIOException() throws IOException {
    final Closeable mockCloseable = mockContext.mock(Closeable.class, "Closeable");

    mockContext.checking(new Expectations() {{
      oneOf(mockCloseable).close();
      will(throwException(new IOException("test")));
    }});

    try {
      IOUtils.close(mockCloseable);
    }
    catch (Throwable unexpected) {
      if (unexpected instanceof IOException) {
        fail("Calling close on a Closeable object unexpectedly threw an IOException!");
      }
    }
  }

  @Test
  public void testCreatePath() {
    assertEquals("", IOUtils.createPath());
    assertEquals("/path/to/file.test".replace("/", File.separator), IOUtils.createPath("path", "to", "file.test"));
    assertEquals("/path/to/a/directory".replace("/", File.separator), IOUtils.createPath("path", "to", "a", "directory"));
  }

  @Test
  public void testCreatePathWithSeparator() {
    assertEquals("", IOUtils.createPath(new String[0], "-"));
    assertEquals("-path-to-file.ext".replace("/", File.separator), IOUtils.createPath(new String[] {
      "path",
      "to",
      "file.ext"
    }, "-"));
    assertEquals("-path-to-a-directory", IOUtils.createPath(new String[] { "path", "to", "a", "directory" }, "-"));
  }

  @Test
  public void testGetFilename() {
    assertNull(IOUtils.getFilename(null));
    assertEquals("", IOUtils.getFilename(""));
    assertEquals("  ", IOUtils.getFilename("  "));
    assertEquals("", IOUtils.getFilename(File.separator));
    assertEquals("a.ext", IOUtils.getFilename("a.ext"));
    assertEquals("b.ext", IOUtils.getFilename(toPathname("b.ext")));
    assertEquals("c.ext", IOUtils.getFilename(toPathname("path", "to", "c.ext")));
    assertEquals("filename.ext", IOUtils.getFilename(toPathname("export", "path", "to", "some", "filename.ext")));
    assertEquals("", IOUtils.getFilename(toPathname("path", "to", "a", "directory") + File.separator));
  }

  @Test
  public void testIsExistingPathname() {
    assertTrue(IOUtils.isExistingPathname(System.getProperty("java.home")));
    assertTrue(IOUtils.isExistingPathname(System.getProperty("user.home")));
    assertFalse(IOUtils.isExistingPathname("/path/to/non_existing/directory/"));
    assertFalse(IOUtils.isExistingPathname("/path/to/non_existing/file.ext"));
  }

  @Test
  public void testObjectSerialization() throws IOException, ClassNotFoundException {
    final Calendar now = Calendar.getInstance();

    assertNotNull(now);

    final byte[] nowBytes = IOUtils.serializeObject(now);

    assertNotNull(nowBytes);
    assertTrue(nowBytes.length != 0);

    final Object nowObj = IOUtils.deserializeObject(nowBytes);

    assertTrue(nowObj instanceof Calendar);
    assertEquals(now.getTimeInMillis(), ((Calendar) nowObj).getTimeInMillis());
  }

  @Test
  public void testObjectSerializationWithClassLoader() throws IOException, ClassNotFoundException {
    final BigDecimal pi = new BigDecimal(Math.PI);

    final byte[] piBytes = IOUtils.serializeObject(pi);

    assertNotNull(piBytes);
    assertTrue(piBytes.length != 0);

    final Object piObj = IOUtils.deserializeObject(piBytes, IOUtilsJUnitTest.class.getClassLoader());

    assertTrue(piObj instanceof BigDecimal);
    assertEquals(pi, piObj);
  }

  @Test
  public void testToByteArray() throws IOException {
    final byte[] expected = new byte[] { (byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE };

    final byte[] actual = IOUtils.toByteArray(new ByteArrayInputStream(expected));

    assertNotNull(actual);
    assertEquals(expected.length, actual.length);

    for (int index = 0; index < actual.length; index++) {
      assertEquals(expected[index], actual[index]);
    }
  }

  @Test(expected = IOException.class)
  public void testToByteArrayThrowsIOException() throws IOException {
    final InputStream mockIn = mockContext.mock(InputStream.class, "testToByteArrayThrowsIOException");

    mockContext.checking(new Expectations() {{
      oneOf(mockIn).read(with(aNonNull(byte[].class)));
      will(throwException(new IOException("test")));
      oneOf(mockIn).close();
    }});

    IOUtils.toByteArray(mockIn);
  }

  @Test(expected = AssertionError.class)
  public void testToByteArrayWithNull() throws IOException {
    try {
      IOUtils.toByteArray(null);
    }
    catch (AssertionError expected) {
      assertEquals("The input stream to read bytes from cannot be null!", expected.getMessage());
      throw expected;
    }
  }

  @Test
  public void testTryGetCanonicalFileElseGetAbsoluteFile() throws Exception {
    final MockFile file = (MockFile) IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(
      new MockFile("/path/to/non_existing/file.test", null));

    assertNotNull(file);
    assertFalse(file.exists());
    assertTrue(file.isGetCanonicalFileCalled());
    assertFalse(file.isGetAbsoluteFileCalled());
  }

  @Test
  public void testTryGetCanonicalFileElseGetAbsoluteFileHandlesIOException() throws Exception {
    final MockFile file = (MockFile) IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(
      new MockFile(System.getProperty("user.home"), new IOException("test")));

    assertNotNull(file);
    assertTrue(file.exists());
    assertTrue(file.isGetCanonicalFileCalled());
    assertTrue(file.isGetAbsoluteFileCalled());
  }

  @Test
  public void testVerifyPathnameExists() throws FileNotFoundException {
    assertEquals(System.getProperty("java.io.tmpdir"), IOUtils.verifyPathnameExists(
      System.getProperty("java.io.tmpdir")));
  }

  @Test(expected = FileNotFoundException.class)
  public void testVerifyPathnameExistsWithNonExistingPathname() throws FileNotFoundException {
    try {
      IOUtils.verifyPathnameExists("/path/to/non_existing/file.test");
    }
    catch (FileNotFoundException expected) {
      assertEquals("Pathname (/path/to/non_existing/file.test) could not be found!", expected.getMessage());
      throw expected;
    }
  }

  private static final class MockFile extends File {

    private boolean isGetAbsoluteFileCalled = false;
    private boolean isGetCanonicalFileCalled = false;

    private final IOException e;

    public MockFile(final String pathname, IOException e) {
      super(pathname);
      this.e = e;
    }

    @Override
    public File getAbsoluteFile() {
      isGetAbsoluteFileCalled = true;
      return this;
    }

    protected boolean isGetAbsoluteFileCalled() {
      return isGetAbsoluteFileCalled;
    }

    @Override
    public File getCanonicalFile() throws IOException {
      isGetCanonicalFileCalled = true;

      if (e != null) {
        throw e;
      }

      return this;
    }

    protected boolean isGetCanonicalFileCalled() {
      return isGetCanonicalFileCalled;
    }
  }

}
