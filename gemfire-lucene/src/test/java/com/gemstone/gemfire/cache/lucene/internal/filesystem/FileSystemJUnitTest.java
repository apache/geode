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

    //Make sure we can read all fo the data back and it matches
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
      if(count >= limit) {
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
