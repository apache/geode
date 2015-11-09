/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gemstone.gemfire.cache.lucene.internal.filesystem;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class FileSystemJUnitTest {

  private static final int SMALL_CHUNK = 523;
  private static final int LARGE_CHUNK = 1024 * 1024 * 5 + 33;
  private FileSystem system;
  private Random rand = new Random();
  private ConcurrentHashMap<String, File> fileRegion;
  private ConcurrentHashMap<ChunkKey, byte[]> chunkRegion;

  @Before
  public void setUp() {
    fileRegion = new ConcurrentHashMap<String, File>();
    chunkRegion = new ConcurrentHashMap<ChunkKey, byte[]>();
    system = new FileSystem(fileRegion, chunkRegion);
  }
  
  /**
   * A test of reading and writing to a file.
   */
  @Test
  public void testReadWriteBytes() throws IOException {
    long start = System.currentTimeMillis();
    
    File file1= system.createFile("testFile1");
    
    assertEquals(0, file1.getLength());
    
    OutputStream outputStream1 = file1.getOutputStream();

    //Write some random data. Make sure it fills several chunks
    outputStream1.write(2);
    byte[] data = new byte[LARGE_CHUNK];
    rand.nextBytes(data);
    outputStream1.write(data);
    outputStream1.write(44);
    outputStream1.close();
    
    assertEquals(2 + LARGE_CHUNK, file1.getLength());
    assertTrue(file1.getModified() >= start);
    
    //Append to the file with a new outputstream
    OutputStream outputStream2 = file1.getOutputStream();
    outputStream2.write(123);
    byte[] data2 = new byte[SMALL_CHUNK];
    rand.nextBytes(data2);
    outputStream2.write(data2);
    outputStream2.close();
    
    assertEquals(3 + LARGE_CHUNK + SMALL_CHUNK, file1.getLength());

    //Make sure we can read all of the data back and it matches
    InputStream is = file1.getInputStream();
    
    assertEquals(2, is.read());
    byte[] resultData = new byte[LARGE_CHUNK];
    assertEquals(LARGE_CHUNK, is.read(resultData));
    assertArrayEquals(data, resultData);
    assertEquals(44, is.read());
    assertEquals(123, is.read());
    

    //Test read to an offset
    Arrays.fill(resultData, (byte) 0);
    assertEquals(SMALL_CHUNK, is.read(resultData, 50, SMALL_CHUNK));
    
    //Make sure the data read matches
    byte[] expectedData = new byte[LARGE_CHUNK];
    Arrays.fill(expectedData, (byte) 0);
    System.arraycopy(data2, 0, expectedData, 50, data2.length);
    assertArrayEquals(expectedData, resultData);
    
    assertEquals(-1, is.read());
    assertEquals(-1, is.read(data));
    is.close();
    
    //Test the skip interface
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
   * A test of cloning a a FileInputStream. The
   * clone should start from where the original was positioned,
   * but they should not hurt each other.
   */
  @Test
  public void testCloneReader() throws IOException {
    File file1= system.createFile("testFile1");
    
    byte[] data = writeRandomBytes(file1);
    
    SeekableInputStream in = file1.getInputStream();
    
    //Read to partway through the file
    byte[] results1 = new byte[data.length];
    in.read(results1, 0, SMALL_CHUNK);
    
    
    //Clone the input stream. Both copies should
    //now be positioned partway through the file.
    SeekableInputStream in2 = in.clone();
    
    byte[] results2 = new byte[data.length];
    
    //Fill in the beginning of results2 with the data that it missed
    //to make testing easier.
    System.arraycopy(data, 0, results2, 0, SMALL_CHUNK);
    
    
    //Read the rest of the file with both copies
    in2.read(results2, SMALL_CHUNK, data.length);
    in.read(results1, SMALL_CHUNK, data.length);
    
    //Both readers should have started from the same place
    //and copied the rest of the data from the file
    assertArrayEquals(data,results1);
    assertArrayEquals(data,results2);
  }
  
  /**
   * A test that skip can jump to the correct position in the stream
   */
  @Test
  public void testSeek() throws IOException {
    File file= system.createFile("testFile1");

    ByteArrayOutputStream expected = new ByteArrayOutputStream();
    byte[] data = new byte[SMALL_CHUNK];
    
    //Write multiple times to the file with a lot of small chunks
    while(expected.size() < FileSystem.CHUNK_SIZE + 1) {
      rand.nextBytes(data);

      expected.write(data);
      writeBytes(file, data);
    }
    
    byte[] expectedBytes = expected.toByteArray();
    assertContents(expectedBytes, file);
    
    //Assert that there are only 2 chunks in the system, since we wrote just
    //past the end of the first chunk.
    assertEquals(2, chunkRegion.size());
    
    SeekableInputStream in = file.getInputStream();
    
    //Seek to several positions in the first chunk
    checkByte(5, in, expectedBytes);
    checkByte(50, in, expectedBytes);
    checkByte(103, in, expectedBytes);
    checkByte(1, in, expectedBytes);
    
    //Seek back and forth between chunks
    checkByte(FileSystem.CHUNK_SIZE + 2, in, expectedBytes);
    checkByte(23, in, expectedBytes);
    checkByte(FileSystem.CHUNK_SIZE + 10, in, expectedBytes);
    checkByte(1023, in, expectedBytes);
    
    //Read the remaining data after a seek
    
    in.seek(10);
    byte[] results = new byte[expectedBytes.length];
    
    //Fill in the initial 10 bytes with the expected value
    System.arraycopy(expectedBytes, 0, results, 0, 10);
    
    assertEquals(results.length - 10, in.read(results, 10, results.length-10));
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
   * @throws IOException
   */
  @Test
  public void testFileOperations() throws IOException {
    String name1 = "testFile1";
    File file1= system.createFile(name1);
    byte[] file1Data = writeRandomBytes(file1);
    
    String name2 = "testFile2";
    File file2= system.createFile(name2);
    byte[] file2Data = writeRandomBytes(file2);
    
    file1 = system.getFile(name1);
    file2 = system.getFile(name2);
    
    assertEquals(new HashSet(Arrays.asList(name1, name2)), system.listFileNames());
    assertContents(file1Data, file1);
    assertContents(file2Data, file2);
    
    
    try {
      system.renameFile(name1, name2);
      fail("Should have received an exception");
    } catch(IOException expected) {
      
    }
    assertEquals(new HashSet(Arrays.asList(name1, name2)), system.listFileNames());
    assertContents(file1Data, file1);
    assertContents(file2Data, file2);
    
    String name3 = "testFile3";
    
    system.renameFile(name1, name3);
    
    File file3 = system.getFile(name3);
    
    assertEquals(new HashSet(Arrays.asList(name3, name2)), system.listFileNames());
    assertContents(file1Data, file3);
    assertContents(file2Data, file2);
    
    system.deleteFile(name2);
    
    assertEquals(new HashSet(Arrays.asList(name3)), system.listFileNames());
    
    system.renameFile(name3, name2);
    
    assertEquals(new HashSet(Arrays.asList(name2)), system.listFileNames());
    
    file2 = system.getFile(name2);
    assertContents(file1Data, file2);
  }
  
  /**
   * Test what happens if you have an unclosed stream and you create a new file.
   * @throws IOException
   */
  @Test
  public void testUnclosedStreamSmallFile() throws IOException {
    doUnclosedStream(SMALL_CHUNK);
  }
  
  /**
   * Test what happens if you have an unclosed stream and you create a new file.
   * @throws IOException
   */
  @Test
  public void testUnclosedStreamLargeFile() throws IOException {
    doUnclosedStream(LARGE_CHUNK);
  }
  
  private void doUnclosedStream(int size) throws IOException {
    String name1 = "testFile1";
    File file1= system.createFile(name1);
    byte[] bytes = getRandomBytes(size );
    file1.getOutputStream().write(bytes);
    
    FileSystem system2 = new FileSystem(fileRegion, chunkRegion);
    File file = system2.getFile(name1);
    
    assertTrue(file.getLength() <= bytes.length);
    
    long length = file.getLength();
    
    
    byte[] results = new byte[bytes.length];
    
    if(length == 0) {
      assertEquals(-1, file.getInputStream().read(results));
      assertTrue(chunkRegion.isEmpty());
    } else {
      //Make sure the amount of data we can read matches the length
      assertEquals(length, file.getInputStream().read(results));

      if(length != bytes.length) {
        Arrays.fill(bytes, (int) length, bytes.length, (byte) 0);
      }
      
      assertArrayEquals(bytes, results);
    }
  }
  
  /**
   * Test what happens a file rename is aborted in the middle
   * due to the a cache closed exception. The next member
   * that uses those files should be able to clean up after
   * the partial rename.
   */
  @Test
  public void testPartialRename() throws IOException {

    final CountOperations countOperations = new CountOperations();
    //Create a couple of mock regions where we count the operations
    //that happen to them. We will then use this to abort the rename
    //in the middle.
    ConcurrentHashMap<String, File> spyFileRegion = Mockito.mock(ConcurrentHashMap.class, new SpyWrapper(countOperations, fileRegion));
    ConcurrentHashMap<ChunkKey, byte[]> spyChunkRegion = Mockito.mock(ConcurrentHashMap.class, new SpyWrapper(countOperations, chunkRegion));
    
    system = new FileSystem(spyFileRegion, spyChunkRegion);
    
    String name = "file";
    File file = system.createFile(name);
    
    ByteArrayOutputStream expected = new ByteArrayOutputStream();

    //Make sure the file has a lot of chunks
    for(int i=0; i < 10; i++) {
      expected.write(writeRandomBytes(file));
    }
    
    String name2 = "file2";
    countOperations.reset();
    
    system.renameFile(name, name2);
    
    assertTrue(2 <= countOperations.count);
    
    countOperations.after((int) Math.ceil(countOperations.count / 2.0), new Runnable() {

      @Override
      public void run() {
        throw new CacheClosedException();
      }
    });
    countOperations.reset();
    
    String name3 = "file3";
    try {
      system.renameFile(name2, name3);
      fail("should have seen an error");
    } catch(CacheClosedException e) {
      
    }
    
    system = new FileSystem(fileRegion, chunkRegion);
    
    //This is not the ideal behavior. We are left
    //with two duplicate files. However, we will still
    //verify that neither file is corrupted.
    assertEquals(2, system.listFileNames().size());
    File sourceFile = system.getFile(name2);
    File destFile = system.getFile(name3);
    
    byte[] expectedBytes = expected.toByteArray();
    
    assertContents(expectedBytes, sourceFile);
    assertContents(expectedBytes, destFile);
  }
  
  /**
   * Test what happens a file delete is aborted in the middle
   * due to the a cache closed exception. The next member
   * that uses those files should be able to clean up after
   * the partial rename.
   */
  @Test
  public void testPartialDelete() throws IOException {

    final CountOperations countOperations = new CountOperations();
    //Create a couple of mock regions where we count the operations
    //that happen to them. We will then use this to abort the rename
    //in the middle.
    ConcurrentHashMap<String, File> spyFileRegion = Mockito.mock(ConcurrentHashMap.class, new SpyWrapper(countOperations, fileRegion));
    ConcurrentHashMap<ChunkKey, byte[]> spyChunkRegion = Mockito.mock(ConcurrentHashMap.class, new SpyWrapper(countOperations, chunkRegion));
    
    system = new FileSystem(spyFileRegion, spyChunkRegion);
    
    String name1 = "file1";
    String name2 = "file2";
    File file1 = system.createFile(name1);
    File file2 = system.createFile(name2);
    
    ByteArrayOutputStream expected = new ByteArrayOutputStream();

    //Make sure the file has a lot of chunks
    for(int i=0; i < 10; i++) {
      byte[] bytes = writeRandomBytes(file1);
      writeBytes(file2, bytes);
      expected.write(bytes);
    }
    
    countOperations.reset();
    
    system.deleteFile(name1);
    
    assertTrue(2 <= countOperations.count);
    
    countOperations.after(countOperations.count / 2, new Runnable() {

      @Override
      public void run() {
        throw new CacheClosedException();
      }
    });
    countOperations.reset();
    
    try {
      system.deleteFile(name2);
      fail("should have seen an error");
    } catch(CacheClosedException e) {
      
    }
    
    system = new FileSystem(fileRegion, chunkRegion);
    
    if(system.listFileNames().size() == 0) {
      //File was deleted, but shouldn't have any dangling chunks at this point
      assertEquals(Collections.EMPTY_SET, fileRegion.keySet());
      //TODO - need to purge chunks of deleted files somehow.
//      assertEquals(Collections.EMPTY_SET, chunkRegion.keySet());
    } else {
      file2 = system.getFile(name2);
      assertContents(expected.toByteArray(), file2);
    }
    
  }

  private File getOrCreateFile(String name) throws IOException {
    try {
      return system.getFile(name);
    } catch(FileNotFoundException e) {
      return system.createFile(name);
    }
  }
  
  private void assertContents(byte[] data, File file) throws IOException {
    assertEquals(data.length, file.getLength());
    
    InputStream is = file.getInputStream();
    
    if(data.length == 0) {
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
  
  public byte[] getRandomBytes() {
    return getRandomBytes(rand.nextInt(LARGE_CHUNK) + SMALL_CHUNK);
  }
  
  public byte[] getRandomBytes(int length) {
    byte[] data = new byte[length];
    rand.nextBytes(data);
    
    return data;
  }
  
  /**
   * A wrapper around an object that will also invoke
   * a callback before applying an operation. 
   *
   * This is essentially like Mockito.spy(), except that it
   * allows the implementation of a default answer for all operations.
   * 
   * To use, do this
   * Mockito.mock(Interface, new SpyWrapper(Answer, o)
   */
  private static final class SpyWrapper implements Answer<Object> {
    private final CountOperations countOperations;
    private Object region;

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

  private static final class CountOperations implements Answer {
    public int count;
    private int limit = Integer.MAX_VALUE;
    private Runnable limitAction;

    @Override
    public Object answer(InvocationOnMock invocation) throws Throwable {
      count++;
      if(count > limit) {
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
