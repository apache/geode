/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal.filesystem;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class FileSystemJUnitTest {

  private static final int SMALL_CHUNK = 523;
  private static final int LARGE_CHUNK = 1024 * 1024 * 5 + 33;

  private FileSystem system;
  private final Random rand = new Random();
  private ConcurrentHashMap fileAndChunkRegion;
  @Rule
  public TemporaryFolder tempFolderRule = new TemporaryFolder();
  private FileSystemStats fileSystemStats;

  @Before
  public void setUp() {
    fileAndChunkRegion = new ConcurrentHashMap();
    fileSystemStats = mock(FileSystemStats.class);
    system = new FileSystem(fileAndChunkRegion, fileSystemStats);
  }

  /**
   * A test of reading and writing to a file.
   */
  @Test
  public void testReadWriteBytes() throws Exception {
    long start = System.currentTimeMillis();

    File file1 = system.createFile("testFile1");

    assertEquals(0, file1.getLength());

    OutputStream outputStream1 = file1.getOutputStream();

    // Write some random data. Make sure it fills several chunks
    outputStream1.write(2);
    byte[] data = new byte[LARGE_CHUNK];
    rand.nextBytes(data);
    outputStream1.write(data);
    outputStream1.write(44);
    outputStream1.close();

    assertEquals(2 + LARGE_CHUNK, file1.getLength());
    assertTrue(file1.getModified() >= start);

    // Append to the file with a new outputstream
    OutputStream outputStream2 = file1.getOutputStream();
    outputStream2.write(123);
    byte[] data2 = new byte[SMALL_CHUNK];
    rand.nextBytes(data2);
    outputStream2.write(data2);
    outputStream2.close();

    assertEquals(3 + LARGE_CHUNK + SMALL_CHUNK, file1.getLength());

    // Make sure we can read all of the data back and it matches
    InputStream is = file1.getInputStream();

    assertEquals(2, is.read());
    byte[] resultData = new byte[LARGE_CHUNK];
    assertEquals(LARGE_CHUNK, is.read(resultData));
    assertArrayEquals(data, resultData);
    assertEquals(44, is.read());
    assertEquals(123, is.read());


    // Test read to an offset
    Arrays.fill(resultData, (byte) 0);
    assertEquals(SMALL_CHUNK, is.read(resultData, 50, SMALL_CHUNK));

    // Make sure the data read matches
    byte[] expectedData = new byte[LARGE_CHUNK];
    Arrays.fill(expectedData, (byte) 0);
    System.arraycopy(data2, 0, expectedData, 50, data2.length);
    assertArrayEquals(expectedData, resultData);

    assertEquals(-1, is.read());
    assertEquals(-1, is.read(data));
    is.close();

    // Test the skip interface
    is = file1.getInputStream();
    is.skip(LARGE_CHUNK + 3);


    Arrays.fill(resultData, (byte) 0);
    assertEquals(SMALL_CHUNK, is.read(resultData));

    Arrays.fill(expectedData, (byte) 0);
    System.arraycopy(data2, 0, expectedData, 0, data2.length);
    assertArrayEquals(expectedData, resultData);

    assertEquals(-1, is.read());
  }

  /**
   * A test of cloning a a FileInputStream. The clone should start from where the original was
   * positioned, but they should not hurt each other.
   */
  @Test
  public void testCloneReader() throws Exception {
    File file1 = system.createFile("testFile1");

    byte[] data = writeRandomBytes(file1);

    SeekableInputStream in = file1.getInputStream();

    // Read to partway through the file
    byte[] results1 = new byte[data.length];
    in.read(results1, 0, SMALL_CHUNK);


    // Clone the input stream. Both copies should
    // now be positioned partway through the file.
    SeekableInputStream in2 = in.clone();

    byte[] results2 = new byte[data.length];

    // Fill in the beginning of results2 with the data that it missed
    // to make testing easier.
    System.arraycopy(data, 0, results2, 0, SMALL_CHUNK);


    // Read the rest of the file with both copies
    in2.read(results2, SMALL_CHUNK, data.length);
    in.read(results1, SMALL_CHUNK, data.length);

    // Both readers should have started from the same place
    // and copied the rest of the data from the file
    assertArrayEquals(data, results1);
    assertArrayEquals(data, results2);
  }

  /**
   * A test that skip can jump to the correct position in the stream
   */
  @Test
  public void testSeek() throws Exception {
    File file = system.createFile("testFile1");

    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    byte[] data = new byte[SMALL_CHUNK];

    // Write multiple times to the file with a lot of small chunks
    while (expected.size() < FileSystem.CHUNK_SIZE + 1) {
      rand.nextBytes(data);

      expected.write(data);
      writeBytes(file, data);
    }

    byte[] expectedBytes = expected.toByteArray();
    assertContents(expectedBytes, file);

    // Assert that there are only 2 chunks in the system, since we wrote just
    // past the end of the first chunk.
    assertEquals(2, numberOfChunks(fileAndChunkRegion));

    SeekableInputStream in = file.getInputStream();

    // Seek to several positions in the first chunk
    checkByte(5, in, expectedBytes);
    checkByte(50, in, expectedBytes);
    checkByte(103, in, expectedBytes);
    checkByte(1, in, expectedBytes);

    // Seek back and forth between chunks
    checkByte(FileSystem.CHUNK_SIZE + 2, in, expectedBytes);
    checkByte(23, in, expectedBytes);
    checkByte(FileSystem.CHUNK_SIZE + 10, in, expectedBytes);
    checkByte(1023, in, expectedBytes);

    // Read the remaining data after a seek

    in.seek(10);
    byte[] results = new byte[expectedBytes.length];

    // Fill in the initial 10 bytes with the expected value
    System.arraycopy(expectedBytes, 0, results, 0, 10);

    assertEquals(results.length - 10, in.read(results, 10, results.length - 10));
    assertEquals(-1, in.read());

    assertArrayEquals(expectedBytes, results);
  }

  private void checkByte(int i, SeekableInputStream in, byte[] expectedBytes) throws IOException {
    in.seek(i);
    byte result = (byte) in.read();

    assertEquals(expectedBytes[i], result);
  }

  /**
   * Test basic file operations - rename, delete, listFiles.
   */
  @Test
  public void testFileOperations() throws Exception {
    String name1 = "testFile1";
    File file1 = system.createFile(name1);
    byte[] file1Data = writeRandomBytes(file1);

    String name2 = "testFile2";
    File file2 = system.createFile(name2);
    byte[] file2Data = writeRandomBytes(file2);

    file1 = system.getFile(name1);
    file2 = system.getFile(name2);

    assertEquals(Arrays.asList(name1, name2), system.listFileNames());
    assertContents(file1Data, file1);
    assertContents(file2Data, file2);


    try {
      system.renameFile(name1, name2);
      fail("Should have received an exception");
    } catch (IOException expected) {

    }
    assertEquals(Arrays.asList(name1, name2), system.listFileNames());
    assertContents(file1Data, file1);
    assertContents(file2Data, file2);

    String name3 = "testFile3";

    system.renameFile(name1, name3);

    File file3 = system.getFile(name3);

    assertEquals(Arrays.asList(name3, name2), system.listFileNames());
    assertContents(file1Data, file3);
    assertContents(file2Data, file2);

    system.deleteFile(name2);

    assertEquals(Arrays.asList(name3), system.listFileNames());

    system.renameFile(name3, name2);

    assertEquals(Arrays.asList(name2), system.listFileNames());

    file2 = system.getFile(name2);
    assertContents(file1Data, file2);
  }

  /**
   * Test what happens if you have an unclosed stream and you create a new file.
   */
  @Test
  public void testUnclosedStreamSmallFile() throws Exception {
    doUnclosedStream(SMALL_CHUNK);
  }

  /**
   * Test what happens if you have an unclosed stream and you create a new file.
   */
  @Test
  public void testUnclosedStreamLargeFile() throws Exception {
    doUnclosedStream(LARGE_CHUNK);
  }

  private void doUnclosedStream(int size) throws IOException {
    String name1 = "testFile1";
    File file1 = system.createFile(name1);
    byte[] bytes = getRandomBytes(size);
    file1.getOutputStream().write(bytes);

    FileSystem system2 = new FileSystem(fileAndChunkRegion, fileSystemStats);
    File file = system2.getFile(name1);

    assertTrue(file.getLength() <= bytes.length);

    long length = file.getLength();


    byte[] results = new byte[bytes.length];

    if (length == 0) {
      assertEquals(-1, file.getInputStream().read(results));
      assertEquals(0, numberOfChunks(fileAndChunkRegion));
    } else {
      // Make sure the amount of data we can read matches the length
      assertEquals(length, file.getInputStream().read(results));

      if (length != bytes.length) {
        Arrays.fill(bytes, (int) length, bytes.length, (byte) 0);
      }

      assertArrayEquals(bytes, results);
    }
  }

  /**
   * Test what happens a file rename is aborted in the middle due to the a cache closed exception.
   * The next member that uses those files should be able to clean up after the partial rename.
   */
  @Test
  public void testPartialRename() throws Exception {

    final CountOperations countOperations = new CountOperations();
    // Create a couple of mock regions where we count the operations
    // that happen to them. We will then use this to abort the rename
    // in the middle.
    ConcurrentHashMap spyFileAndChunkRegion =
        mock(ConcurrentHashMap.class, new SpyWrapper(countOperations, fileAndChunkRegion));


    system = new FileSystem(spyFileAndChunkRegion, fileSystemStats);

    String name = "file";
    File file = system.createFile(name);

    ByteArrayOutputStream expected = new ByteArrayOutputStream();

    // Make sure the file has a lot of chunks
    for (int i = 0; i < 10; i++) {
      expected.write(writeRandomBytes(file));
    }

    String name2 = "file2";
    countOperations.reset();

    system.renameFile(name, name2);

    // Right now the number of operations is 4.. except if run through a debugger...
    assertTrue(4 <= countOperations.count);

    // This number of operations during a rename actually needs to get to the "putIfAbsent" for the
    // Assertion to be correct. Right now the number of operations is actually 3 so the limit needs
    // to be 3...
    countOperations.after((int) Math.ceil(countOperations.count / 2.0 + 1), new Runnable() {

      @Override
      public void run() {
        throw new CacheClosedException();
      }
    });
    String name3 = "file3";
    countOperations.reset();

    try {
      system.renameFile(name2, name3);
      fail("should have seen an error");
    } catch (CacheClosedException expectedException) {

    }

    system = new FileSystem(fileAndChunkRegion, fileSystemStats);

    // This is not the ideal behavior. We are left
    // with two duplicate files. However, we will still
    // verify that neither file is corrupted.
    assertEquals(2, system.listFileNames().size());
    File sourceFile = system.getFile(name2);
    File destFile = system.getFile(name3);

    byte[] expectedBytes = expected.toByteArray();

    assertContents(expectedBytes, sourceFile);
    assertContents(expectedBytes, destFile);
  }

  @Test
  public void testExport() throws IOException {
    String name1 = "testFile1";
    File file1 = system.createFile(name1);
    byte[] file1Data = writeRandomBytes(file1);

    String name2 = "testFile2";
    File file2 = system.createFile(name2);
    byte[] file2Data = writeRandomBytes(file2);

    java.io.File parentDir = tempFolderRule.getRoot();
    system.export(parentDir);
    String[] foundFiles = parentDir.list();
    Arrays.sort(foundFiles);
    assertArrayEquals(new String[] {"testFile1", "testFile2"}, foundFiles);

    assertExportedFileContents(file1Data, new java.io.File(parentDir, "testFile1"));
    assertExportedFileContents(file2Data, new java.io.File(parentDir, "testFile2"));
  }

  @Test
  public void testIncrementFileCreates() throws IOException {
    File file = system.createFile("file");
    verify(fileSystemStats).incFileCreates(1);
  }

  @Test
  public void testIncrementFileDeletes() throws IOException {
    File file = system.createFile("file");
    system.deleteFile("file");
    verify(fileSystemStats).incFileDeletes(1);
  }

  @Test
  public void testIncrementFileRenames() throws IOException {
    File file = system.createFile("file");
    system.renameFile("file", "dest");
    verify(fileSystemStats).incFileRenames(1);
  }

  @Test
  public void testIncrementTemporaryFileCreates() throws IOException {
    File file = system.createTemporaryFile("file");
    verify(fileSystemStats).incTemporaryFileCreates(1);
  }

  @Test
  public void testIncrementWrittenBytes() throws IOException {
    File file = system.createTemporaryFile("file");
    final byte[] bytes = writeRandomBytes(file);
    ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);
    verify(fileSystemStats, atLeast(1)).incWrittenBytes(captor.capture());
    final int actualByteCount = captor.getAllValues().stream().mapToInt(Integer::intValue).sum();
    assertEquals(bytes.length, actualByteCount);
  }

  @Test
  public void testIncrementReadBytes() throws IOException {
    File file = system.createTemporaryFile("file");
    final byte[] bytes = writeRandomBytes(file);
    file.getInputStream().read(bytes);
    ArgumentCaptor<Integer> captor = ArgumentCaptor.forClass(Integer.class);
    verify(fileSystemStats, atLeast(1)).incReadBytes(captor.capture());
    final int actualByteCount = captor.getAllValues().stream().mapToInt(Integer::intValue).sum();
    assertEquals(bytes.length, actualByteCount);
  }

  @Test
  public void testDeletePossiblyRenamedFileDoesNotDestroyChunks() throws Exception {
    ConcurrentHashMap spyFileRegion = Mockito.spy(fileAndChunkRegion);
    system = new FileSystem(spyFileRegion, fileSystemStats);

    String sourceFileName = "sourceFile";
    File file1 = system.createFile(sourceFileName);
    byte[] data = writeRandomBytes(file1);

    Mockito.doReturn(file1).when(spyFileRegion).remove(any());

    String destFileName = "destFile";
    system.renameFile(sourceFileName, destFileName);
    File destFile = system.getFile(destFileName);

    assertNotNull(system.getFile(sourceFileName));
    system.deleteFile(sourceFileName);
    assertNotNull(system.getChunk(destFile, 0));

  }

  private void assertExportedFileContents(final byte[] expected, final java.io.File exportedFile)
      throws IOException {
    byte[] actual = Files.readAllBytes(exportedFile.toPath());
    assertArrayEquals(expected, actual);
  }

  private void assertContents(byte[] data, File file) throws IOException {
    assertEquals(data.length, file.getLength());

    InputStream is = file.getInputStream();

    if (data.length == 0) {
      assertEquals(-1, is.read());
      return;
    }

    byte[] results = new byte[data.length];
    assertEquals(file.getLength(), is.read(results));
    assertEquals(-1, is.read());
    is.close();

    assertArrayEquals(data, results);
  }

  private byte[] writeRandomBytes(File file) throws IOException {
    byte[] file1Data = getRandomBytes();
    writeBytes(file, file1Data);
    return file1Data;
  }

  private void writeBytes(File file, byte[] data) throws IOException {
    OutputStream outputStream = file.getOutputStream();
    outputStream.write(data);
    outputStream.close();
  }

  private byte[] getRandomBytes() {
    return getRandomBytes(rand.nextInt(LARGE_CHUNK) + SMALL_CHUNK);
  }

  private byte[] getRandomBytes(int length) {
    byte[] data = new byte[length];
    rand.nextBytes(data);

    return data;
  }

  private long numberOfChunks(Map map) {
    return map.keySet().parallelStream().filter(k -> (k instanceof ChunkKey)).count();
  }

  /**
   * A wrapper around an object that will also invoke a callback before applying an operation.
   *
   * This is essentially like Mockito.spy(), except that it allows the implementation of a default
   * answer for all operations.
   *
   * To use, do this Mockito.mock(Interface, new SpyWrapper(Answer, o)
   */
  private static class SpyWrapper implements Answer<Object> {
    private final CountOperations countOperations;
    private final Object region;

    private SpyWrapper(CountOperations countOperations, Object region) {
      this.countOperations = countOperations;
      this.region = region;
    }

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      countOperations.answer(invocation);
      Method m = invocation.getMethod();
      return m.invoke(region, invocation.getArguments());
    }
  }

  private static class CountOperations implements Answer {
    public int count;
    private int limit = Integer.MAX_VALUE;
    private Runnable limitAction;

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      count++;
      if (count > limit) {
        limitAction.run();
      }
      return null;
    }

    public void reset() {
      count = 0;
    }

    public void after(int i, Runnable runnable) {
      limit = i;
      limitAction = runnable;

    }
  }

}
