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
package com.gemstone.gemfire.internal.offheap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Test;

import static org.junit.Assert.*;

public abstract class MemoryChunkJUnitTestBase {

  
  protected abstract MemoryChunk createChunk(int size);
  
  @Test
  public void testByteReadWrite() {
    int CHUNK_SIZE = 1024;
    MemoryChunk mc = createChunk(CHUNK_SIZE);
    try {
    for (int i=0; i<CHUNK_SIZE; i++) {
      mc.writeByte(i, (byte)(i%128));
    }
    for (int i=0; i<CHUNK_SIZE; i++) {
      assertEquals(i%128, mc.readByte(i));
    }
    } finally {
      mc.release();
    }
  }
  
  @Test
  public void testByteArrayReadWrite() {
    byte[] writeBytes = new byte[256];
    int v = Byte.MIN_VALUE;
    for (int i=0; i < writeBytes.length; i++) {
      writeBytes[i] = (byte)v++;
    }
    int ARRAYS_PER_CHUNK = 100;
    int CHUNK_SIZE = ARRAYS_PER_CHUNK * writeBytes.length;
    MemoryChunk mc = createChunk(CHUNK_SIZE);
    try {
    for (int i=0; i<CHUNK_SIZE; i+=writeBytes.length) {
      mc.writeBytes(i, writeBytes);
    }
    byte[] readBytes = new byte[writeBytes.length];
    for (int i=0; i<CHUNK_SIZE; i+=writeBytes.length) {
      mc.readBytes(i, readBytes);
      assertTrue("expected " + Arrays.toString(writeBytes) + " but found " + Arrays.toString(readBytes), Arrays.equals(writeBytes, readBytes));
    }
    } finally {
      mc.release();
    }
  }
  public void DISABLEtestBytePerf() throws InterruptedException {
    final int ITEM_SIZE = 1;
    final int ITEMS_PER_CHUNK = 100000;
    final int CHUNK_SIZE = ITEMS_PER_CHUNK * ITEM_SIZE;
    final MemoryChunk mc = createChunk(CHUNK_SIZE);
    try {
      final int WRITE_ITERATIONS = 90000;
      final Runnable writeRun = new Runnable() {
        public void run() {
          for (int j=0; j<WRITE_ITERATIONS; j++) {
            for (int i=0; i<CHUNK_SIZE; i+=ITEM_SIZE) {
              mc.writeByte(i, (byte)1);
            }
          }
        }
      };
      long startWrite = System.nanoTime();
      writeRun.run();
      long endWrite = System.nanoTime();
      final int READ_ITERATIONS = 90000/10;
      final AtomicIntegerArray readTimes = new AtomicIntegerArray(READ_ITERATIONS);
      final int THREAD_COUNT = 3;
      final Thread[] threads = new Thread[THREAD_COUNT];
      final ReadWriteLock rwl = new ReentrantReadWriteLock();
      final Lock rl = rwl.readLock();
      final AtomicLong longHolder = new AtomicLong();
      final Runnable r = new Runnable() {
        public void run() {
          long c = 0;
          long lastTs = System.nanoTime();
          long startTs;
          for (int j=0; j<READ_ITERATIONS; j++) {
            startTs = lastTs;
            for (int i=0; i<CHUNK_SIZE; i+=ITEM_SIZE) {
//              c += mc.readByte(i);
              rl.lock();
              try {
                c+= mc.readByte(i);
              } finally {
                rl.unlock();
              }
            }
            lastTs = System.nanoTime();
            readTimes.addAndGet(j, (int) (lastTs-startTs));
          }
          longHolder.addAndGet(c);
          //System.out.println("c="+c);
        }
      };
      for (int t=0; t < THREAD_COUNT; t++) {
        threads[t] = new Thread(r);
      }
      long start = System.nanoTime();
      for (int t=0; t < THREAD_COUNT; t++) {
        threads[t].start();
      }
      for (int t=0; t < THREAD_COUNT; t++) {
        threads[t].join();
      }
//      long start = System.nanoTime();
//      r.run();
      long end = System.nanoTime();
      System.out.println("longHolder=" + longHolder.get());
      System.out.println(computeHistogram(readTimes, 1000000));
//      for (int i=0; i < 30; i++) {
//        System.out.print(readTimes[i]);
//        System.out.print(' ');
//      }
//      System.out.println();
//      for (int i=readTimes.length-30; i < readTimes.length; i++) {
//        System.out.print(readTimes[i]);
//        System.out.print(' ');
//      }
//      System.out.println();
      System.out.println((end-start) / READ_ITERATIONS);
      System.out.println("BytePerfReads:  " + (double)((long)CHUNK_SIZE*READ_ITERATIONS*THREAD_COUNT)/(double)((end-start)/1000000) + " bytes/ms");
      System.out.println("BytePerfWrites: " + (double)((long)CHUNK_SIZE*WRITE_ITERATIONS)/(double)((endWrite-startWrite)/1000000) + " bytes/ms");
    } finally {
      mc.release();
    }
  }
  static private ArrayList<Bucket> computeHistogram(AtomicIntegerArray originalValues, final int granualarity) {
    int[] values = new int[originalValues.length()];
    for (int i=0; i < values.length; i++) {
      values[i] = originalValues.get(i);
    }
    Arrays.sort(values);
    ArrayList<Bucket> result = new ArrayList<Bucket>();
    Bucket curBucket = new Bucket(values[0]);
    result.add(curBucket);
    for (int i=1; i < values.length; i++) {
      int curVal = values[i];
      if (!curBucket.addValue(curVal, granualarity)) {
        curBucket = new Bucket(curVal);
        result.add(curBucket);
      }
    }
    return result;
  }
  static private class Bucket {
    public Bucket(long l) {
      base = l;
      total = l;
      count = 1;
    }
    public boolean addValue(long curVal, int granualarity) {
      if (curVal < base || (curVal-base) > granualarity) {
        return false;
      }
      total += curVal;
      count++;
      return true;
    }
    private final long base;
    private long total;
    private int count;
    
    @Override
    public String toString() {
      return "" + (total/count) + ":" + count;
    }
  }
  public void DISABLEtest256ByteArrayPerf() {
    byte[] writeBytes = new byte[256];
    for (int i=0; i < writeBytes.length; i++) {
      writeBytes[i] = 1;
    }
    int ARRAYS_PER_CHUNK = 100000;
    int CHUNK_SIZE = ARRAYS_PER_CHUNK * writeBytes.length;
    MemoryChunk mc = createChunk(CHUNK_SIZE);
    try {
      int WRITE_ITERATIONS = 2000;
      long startWrite = System.nanoTime();
      for (int j=0; j<WRITE_ITERATIONS; j++) {
        for (int i=0; i<CHUNK_SIZE; i+=writeBytes.length) {
          mc.writeBytes(i, writeBytes);
        }
      }
      long endWrite = System.nanoTime();
      byte[] readBytes = new byte[writeBytes.length];
      int READ_ITERATIONS = 2000;
      long start = System.nanoTime();
      for (int j=0; j<READ_ITERATIONS; j++) {
        for (int i=0; i<CHUNK_SIZE; i+=writeBytes.length) {
          mc.readBytes(i, readBytes);
        }
      }
      long end = System.nanoTime();
      System.out.println("ByteArray("+writeBytes.length+")PerfReads: " + (double)((long)CHUNK_SIZE*(long)READ_ITERATIONS)/(double)((end-start)/1000000) + " bytes/ms");
      System.out.println("ByteArray("+writeBytes.length+")PerfWrites: " + (double)((long)CHUNK_SIZE*(long)WRITE_ITERATIONS)/(double)((endWrite-startWrite)/1000000) + " bytes/ms");
    } finally {
      mc.release();
    }
  }
}
