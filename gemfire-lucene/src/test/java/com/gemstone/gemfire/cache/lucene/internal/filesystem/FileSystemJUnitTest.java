package com.gemstone.gemfire.cache.lucene.internal.filesystem;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class FileSystemJUnitTest {

  private static final int SMALL_CHUNK = 523;
  private static final int LARGE_CHUNK = 1024 * 1024 * 5;
  private FileSystem system;
  private Random rand = new Random();

  @Before
  public void setUp() {
    ConcurrentHashMap<String, File> fileRegion = new ConcurrentHashMap<String, File>();
    ConcurrentHashMap<ChunkKey, byte[]> chunkRegion = new ConcurrentHashMap<ChunkKey, byte[]>();
    system = new FileSystem(fileRegion, chunkRegion);
  }
  
  @Test
  public void testReadWriteBytes() throws IOException {
    long start = System.currentTimeMillis();
    
    File file1= system.createFile("testFile1");
    
    assertEquals(0, file1.getLength());
    
    OutputStream outputStream1 = file1.getOutputStream();
    
    outputStream1.write(2);
    byte[] data = new byte[LARGE_CHUNK];
    rand.nextBytes(data);
    outputStream1.write(data);
    outputStream1.write(44);
    outputStream1.close();
    
    assertEquals(2 + LARGE_CHUNK, file1.getLength());
    assertTrue(file1.getModified() >= start);
    
    OutputStream outputStream2 = file1.getOutputStream();
    
    outputStream2.write(123);
    byte[] data2 = new byte[SMALL_CHUNK];
    rand.nextBytes(data2);
    outputStream2.write(data2);
    outputStream2.close();
    
    assertEquals(3 + LARGE_CHUNK + SMALL_CHUNK, file1.getLength());
    
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

  private void assertContents(byte[] data, File file) throws IOException {
    assertEquals(data.length, file.getLength());
    InputStream is = file.getInputStream();
    byte[] results = new byte[data.length];
    assertEquals(file.getLength(), is.read(results));
    assertEquals(-1, is.read());
    is.close();
    
    assertArrayEquals(data, results);
  }

  private byte[] writeRandomBytes(File file) throws IOException {
    byte[] file1Data = getRandomBytes();
    OutputStream outputStream = file.getOutputStream();
    outputStream.write(file1Data);
    outputStream.close();
    return file1Data;
  }
  
  public byte[] getRandomBytes() {
    byte[] data = new byte[rand.nextInt(LARGE_CHUNK) + SMALL_CHUNK];
    rand.nextBytes(data);
    
    return data;
  }

}
