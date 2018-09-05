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
package org.apache.geode.internal.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Calendar;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;


/**
 * Test class for testing the contract and functionality of the IOUtils class.
 * <p/>
 *
 * @see org.apache.geode.internal.util.IOUtils
 * @see org.junit.Test
 */
public class IOUtilsJUnitTest {

  /**
   * Gets a fully-qualified path anchored at root.
   * <p/>
   *
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
    assertThat(IOUtils.appendToPath(null, (String[]) null)).isNull();
    assertThat(IOUtils.appendToPath(null)).isEqualTo(File.separator);
    assertThat(IOUtils.appendToPath("")).isEqualTo(File.separator);
    assertThat(IOUtils.appendToPath(" ")).isEqualTo(File.separator);
    assertThat(IOUtils.appendToPath(File.separator)).isEqualTo(File.separator);
    assertThat(IOUtils.appendToPath(null, "bin", "a.out")).isEqualTo(toPathname("bin", "a.out"));
    assertThat(IOUtils.appendToPath(File.separator, "bin", "a.out"))
        .isEqualTo(toPathname("bin", "a.out"));
    assertThat(IOUtils.appendToPath(toPathname("usr", "local"), "bin", "a.out"))
        .isEqualTo(toPathname("usr", "local", "bin", "a.out"));
  }

  @Test
  public void testClose() throws IOException {
    final Closeable mockCloseable = spy(Closeable.class);
    IOUtils.close(mockCloseable);
    verify(mockCloseable, times(1)).close();
  }

  @Test
  public void testCloseIgnoresIOException() throws IOException {
    final Closeable mockCloseable = mock(Closeable.class);
    doThrow(new IOException("Mock Exception")).when(mockCloseable).close();
    IOUtils.close(mockCloseable);
  }

  @Test
  public void testCreatePath() {
    assertThat(IOUtils.createPath()).isEqualTo("");
    assertThat(IOUtils.createPath("path", "to", "file.test"))
        .isEqualTo("/path/to/file.test".replace("/", File.separator));
    assertThat(IOUtils.createPath("path", "to", "a", "directory"))
        .isEqualTo("/path/to/a/directory".replace("/", File.separator));
  }

  @Test
  public void testCreatePathWithSeparator() {
    assertThat(IOUtils.createPath(new String[0], "-")).isEqualTo("");
    assertThat(IOUtils.createPath(new String[] {"path", "to", "file.ext"}, "-"))
        .isEqualTo("-path-to-file.ext".replace("/", File.separator));
    assertThat(IOUtils.createPath(new String[] {"path", "to", "a", "directory"}, "-"))
        .isEqualTo("-path-to-a-directory");
  }

  @Test
  public void testGetFilename() {
    assertThat(IOUtils.getFilename(null)).isNull();
    assertThat(IOUtils.getFilename("")).isEqualTo("");
    assertThat(IOUtils.getFilename("  ")).isEqualTo("  ");
    assertThat(IOUtils.getFilename(File.separator)).isEqualTo("");
    assertThat(IOUtils.getFilename("a.ext")).isEqualTo("a.ext");
    assertThat(IOUtils.getFilename(toPathname("b.ext"))).isEqualTo("b.ext");
    assertThat(IOUtils.getFilename(toPathname("path", "to", "c.ext"))).isEqualTo("c.ext");
    assertThat(IOUtils.getFilename(toPathname("export", "path", "to", "some", "filename.ext")))
        .isEqualTo("filename.ext");
    assertThat(IOUtils.getFilename(toPathname("path", "to", "a", "directory") + File.separator))
        .isEqualTo("");
  }

  @Test
  public void testIsExistingPathname() {
    assertThat(IOUtils.isExistingPathname(System.getProperty("java.home"))).isTrue();
    assertThat(IOUtils.isExistingPathname(System.getProperty("user.home"))).isTrue();
    assertThat(IOUtils.isExistingPathname("/path/to/non_existing/directory/")).isFalse();
    assertThat(IOUtils.isExistingPathname("/path/to/non_existing/file.ext")).isFalse();
  }

  @Test
  public void testObjectSerialization() throws IOException, ClassNotFoundException {
    final Calendar now = Calendar.getInstance();
    assertThat(now).isNotNull();

    final byte[] nowBytes = IOUtils.serializeObject(now);
    assertThat(nowBytes).isNotNull();
    assertThat(nowBytes.length != 0).isTrue();

    final Object nowObj = IOUtils.deserializeObject(nowBytes);
    assertThat(nowObj).isInstanceOf(Calendar.class);
    assertThat(((Calendar) nowObj).getTimeInMillis()).isEqualTo(now.getTimeInMillis());
  }

  @Test
  public void testObjectSerializationWithClassLoader() throws IOException, ClassNotFoundException {
    final BigDecimal pi = new BigDecimal(Math.PI);
    final byte[] piBytes = IOUtils.serializeObject(pi);

    assertThat(piBytes).isNotNull();
    assertThat(piBytes.length != 0).isTrue();

    final Object piObj =
        IOUtils.deserializeObject(piBytes, IOUtilsJUnitTest.class.getClassLoader());
    assertThat(piObj).isInstanceOf(BigDecimal.class);
    assertThat(piObj).isEqualTo(pi);
  }

  @Test
  public void testToByteArray() throws IOException {
    final byte[] expected = new byte[] {(byte) 0xCA, (byte) 0xFE, (byte) 0xBA, (byte) 0xBE};
    final byte[] actual = IOUtils.toByteArray(new ByteArrayInputStream(expected));

    assertThat(actual).isNotNull();
    assertThat(actual.length).isEqualTo(expected.length);
    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testToByteArrayThrowsIOException() throws IOException {
    final InputStream mockIn = mock(InputStream.class, "testToByteArrayThrowsIOException");
    when(mockIn.read(any())).thenThrow(new IOException("Mock IOException"));
    assertThatThrownBy(() -> IOUtils.toByteArray(mockIn)).isInstanceOf(IOException.class)
        .hasMessage("Mock IOException");
  }

  @Test
  public void testToByteArrayWithNull() {
    assertThatThrownBy(() -> IOUtils.toByteArray(null)).isInstanceOf(AssertionError.class)
        .hasMessage("The input stream to read bytes from cannot be null!");
  }

  @Test
  public void testTryGetCanonicalFileElseGetAbsoluteFile() throws IOException {
    final File mockFile = mock(File.class);
    when(mockFile.getCanonicalFile()).thenReturn(mock(File.class));
    final File file = IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(mockFile);

    assertThat(file).isNotNull();
    assertThat(file.exists()).isFalse();
    verify(mockFile, times(1)).getCanonicalFile();
    verify(mockFile, times(0)).getAbsoluteFile();
  }

  @Test
  public void testTryGetCanonicalFileElseGetAbsoluteFileHandlesIOException() throws IOException {
    final File mockFile = mock(File.class);
    when(mockFile.getAbsoluteFile()).thenReturn(mock(File.class));
    when(mockFile.getCanonicalFile()).thenThrow(new IOException("Mock Exception"));
    final File file = IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(mockFile);

    assertThat(file).isNotNull();
    assertThat(file.exists()).isFalse();
    InOrder inOrder = Mockito.inOrder(mockFile);
    inOrder.verify(mockFile).getCanonicalFile();
    inOrder.verify(mockFile).getAbsoluteFile();
  }

  @Test
  public void testVerifyPathnameExists() throws FileNotFoundException {
    assertThat(System.getProperty("java.io.tmpdir"))
        .isEqualTo(IOUtils.verifyPathnameExists(System.getProperty("java.io.tmpdir")));
  }

  @Test
  public void testVerifyPathnameExistsWithNonExistingPathname() {
    assertThatThrownBy(() -> IOUtils.verifyPathnameExists("/path/to/non_existing/file.test"))
        .isInstanceOf(FileNotFoundException.class)
        .hasMessage("Pathname (/path/to/non_existing/file.test) could not be found!");
  }
}
