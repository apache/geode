/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SortedIterator;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SortedStatistics;

public abstract class SortedReaderTestCase extends TestCase {
  private NavigableMap<byte[], byte[]> data;
  protected SortedReader<ByteBuffer> reader;

  public static void assertEquals(byte[] expected, ByteBuffer actual) {
    assertEquals(expected.length, actual.remaining());
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], actual.get(actual.position() + i));
    }
  }
  
  public void testComparator() {
    assertNotNull(reader.getComparator());
  }
  
  public void testStatistics() throws IOException {
    SortedStatistics stats = reader.getStatistics();
    try {
      assertEquals(data.size(), stats.keyCount());
      assertTrue(Arrays.equals(data.firstKey(), stats.firstKey()));
      assertTrue(Arrays.equals(data.lastKey(), stats.lastKey()));
      
      int keySize = 0;
      int valSize = 0;
      for (Entry<byte[], byte[]> entry : data.entrySet()) {
        keySize += entry.getKey().length;
        valSize += entry.getValue().length;
      }

      double avgKey = keySize / data.size();
      double avgVal = valSize / data.size();
          
      assertEquals(avgVal, stats.avgValueSize());
      assertEquals(avgKey, stats.avgKeySize());
      
    } finally {
      stats.close();
    }
  }

  public void testMightContain() throws IOException {
    for (byte[] key : data.keySet()) {
      assertTrue(reader.mightContain(key));
    }
  }

  public void testRead() throws IOException {
    for (byte[] key : data.keySet()) {
      assertEquals(data.get(key), reader.read(key));
    }
  }

  public void testScan() throws IOException {
    SortedIterator<ByteBuffer> scan = reader.scan();
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.entrySet().iterator();
      doIter(scan, iter);
      
    } finally {
      scan.close();
    }
  }
  
  public void testMultithreadScan() throws Exception {
    int threads = 10;
    ExecutorService exec = Executors.newFixedThreadPool(threads);
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (int i = 0; i < threads; i++) {
      tasks.add(new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          testScan();
          return true;
        }
      });
    }

    int i = 0;
    while (i++ < 1000) {
      for (Future<Boolean> ft : exec.invokeAll(tasks)) {
        assertTrue(ft.get());
      }
    }
  }
  
  public void testScanReverse() throws IOException {
    SortedIterator<ByteBuffer> scan = reader.withAscending(false).scan();
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.descendingMap().entrySet().iterator();
      doIter(scan, iter);
    } finally {
      scan.close();
    }
  }
  
  public void testHead() throws IOException {
    byte[] split = wrapInt(50);
    SortedIterator<ByteBuffer> scan = reader.head(split, true);
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.headMap(split, true).entrySet().iterator();
      doIter(scan, iter);
    } finally {
      scan.close();
    }
    
    scan = reader.head(split, false);
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.headMap(split, false).entrySet().iterator();
      doIter(scan, iter);
    } finally {
      scan.close();
    }
  }
  
  public void testTail() throws IOException {
    byte[] split = wrapInt(50);
    SortedIterator<ByteBuffer> scan = reader.tail(split, true);
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.tailMap(split, true).entrySet().iterator();
      doIter(scan, iter);
    } finally {
      scan.close();
    }
    
    scan = reader.tail(split, false);
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.tailMap(split, false).entrySet().iterator();
      doIter(scan, iter);
    } finally {
      scan.close();
    }
  }
  
  public void testScanWithBounds() throws IOException {
    byte[] lhs = wrapInt(10);
    byte[] rhs = wrapInt(90);

    // [lhs,rhs)
    SortedIterator<ByteBuffer> scan = reader.scan(lhs, rhs);
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.subMap(lhs, rhs).entrySet().iterator();
      doIter(scan, iter);
    } finally {
      scan.close();
    }
    
    // (lhs,rhs)
    scan = reader.scan(lhs, false, rhs, false);
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.subMap(lhs, false, rhs, false).entrySet().iterator();
      doIter(scan, iter);
    } finally {
      scan.close();
    }
  
    // [lhs,rhs]
    scan = reader.scan(lhs, true, rhs, true);
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.subMap(lhs, true, rhs, true).entrySet().iterator();
      doIter(scan, iter);
    } finally {
      scan.close();
    }
  }
  
  public void testReverseScanWithBounds() throws IOException {
    data = data.descendingMap();
    byte[] rhs = wrapInt(10);
    byte[] lhs = wrapInt(90);

    SortedReader<ByteBuffer> rev = reader.withAscending(false);
    
    // [rhs,lhs)
    SortedIterator<ByteBuffer> scan = rev.scan(lhs, rhs);
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.subMap(lhs, rhs).entrySet().iterator();
      doIter(scan, iter);
    } finally {
      scan.close();
    }
      
    // (rhs,lhs)
    scan = rev.scan(lhs, false, rhs, false);
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.subMap(lhs, false, rhs, false).entrySet().iterator();
      doIter(scan, iter);
    } finally {
      scan.close();
    }
  
    // [rhs,lhs]
    scan = rev.scan(lhs, true, rhs, true);
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.subMap(lhs, true, rhs, true).entrySet().iterator();
      doIter(scan, iter);
    } finally {
      scan.close();
    }
  }
  
  public void testScanEquality() throws IOException {
    byte[] val = wrapInt(10);
    
    // [val,val]
    SortedIterator<ByteBuffer> scan = reader.scan(val);
    try {
      Iterator<Entry<byte[], byte[]>> iter = data.subMap(val, true, val, true).entrySet().iterator();
      doIter(scan, iter);
    } finally {
      scan.close();
    }
  }
  
  public static byte[] wrapInt(int n) {
    ByteBuffer buf = (ByteBuffer) ByteBuffer.allocate(4).putInt(n).flip();
    return buf.array();
  }
  
  public static File[] getSoplogsToDelete() {
    return new File(".").listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith("soplog") || name.endsWith("crc");
      }
    });
  }

  private void doIter(SortedIterator<ByteBuffer> scan, Iterator<Entry<byte[], byte[]>> iter) {
    while (scan.hasNext() || iter.hasNext()) {
      Entry<byte[], byte[]> expected = iter.next();
      assertEquals(expected.getKey(), scan.next());
      assertEquals(expected.getValue(), scan.value());
    }
  }

  @Override
  protected final void setUp() throws IOException {
    data = new TreeMap<byte[], byte[]>(new ByteComparator());
    
    for (int i = 0; i < 100; i++) {
      data.put(wrapInt(i), wrapInt(i));
    }
    reader = createReader(data);
  }
  
  @Override 
  protected void tearDown() throws IOException {
    reader.close();
    for (File f : getSoplogsToDelete()) {
      f.delete();
    }
  }
  
  protected abstract SortedReader<ByteBuffer> createReader(NavigableMap<byte[], byte[]> data) 
      throws IOException;
}
