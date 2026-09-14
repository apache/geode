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
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.management.ThreadMXBean;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.LuceneTest;

@Category({LuceneTest.class})
public class FileOutputStreamJUnitTest {

  private static final int CHUNK_SIZE = FileSystem.CHUNK_SIZE;

  private FileSystem system;

  @Before
  public void setUp() {
    system = new FileSystem(new ConcurrentHashMap<>(), mock(FileSystemStats.class));
  }

  /**
   * A test that writing a small file allocates far less than one chunk.
   */
  @Test
  public void testSmallFileAllocatesLessThanChunkSize() throws IOException {
    ThreadMXBean threadBean = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    assumeTrue(threadBean.isThreadAllocatedMemorySupported()
        && threadBean.isThreadAllocatedMemoryEnabled());

    // Load classes and warm up the mocks before measuring
    writeSmallFile(system.createFile("warmup"));

    File file = system.createFile("small");
    long threadId = Thread.currentThread().getId();
    long before = threadBean.getThreadAllocatedBytes(threadId);
    writeSmallFile(file);
    long allocated = threadBean.getThreadAllocatedBytes(threadId) - before;

    assertTrue("Allocated " + allocated + " bytes to write a small file",
        allocated < CHUNK_SIZE / 4);
  }

  /**
   * A test that files written with a random mix of single bytes, arrays and appends read back
   * correctly, with every chunk except the last one full.
   */
  @Test
  public void testRandomWritesReadBackWithFullChunks() throws IOException {
    long seed = System.nanoTime();
    Random random = new Random(seed);

    for (int iteration = 0; iteration < 50; iteration++) {
      FileSystem fileSystem =
          new FileSystem(new ConcurrentHashMap<>(), mock(FileSystemStats.class));
      File file = fileSystem.createFile("random");
      ByteArrayOutputStream expected = new ByteArrayOutputStream();

      int sessions = 1 + random.nextInt(4);
      for (int session = 0; session < sessions; session++) {
        OutputStream outputStream = file.getOutputStream();
        int writes = 1 + random.nextInt(12);
        for (int i = 0; i < writes; i++) {
          writeRandomly(random, outputStream, expected);
        }
        outputStream.close();

        assertFileContents("seed " + seed + ", iteration " + iteration + ", session " + session,
            fileSystem, file, expected.toByteArray());
      }
    }
  }

  private void writeSmallFile(File file) throws IOException {
    OutputStream outputStream = file.getOutputStream();
    outputStream.write(new byte[100]);
    outputStream.close();
  }

  /**
   * Writes at least one byte to both streams, choosing among single bytes, arrays with offsets,
   * zero-length writes and writes that end on or next to a chunk boundary.
   */
  private void writeRandomly(Random random, OutputStream outputStream,
      ByteArrayOutputStream expected) throws IOException {
    int toBoundary = CHUNK_SIZE - expected.size() % CHUNK_SIZE;
    boolean large = expected.size() < 4 * CHUNK_SIZE;
    int kind = random.nextInt(8);
    if (!large && kind >= 4) {
      kind = random.nextInt(4);
    }

    int length;
    switch (kind) {
      case 0:
        int b = random.nextInt(256);
        outputStream.write(b);
        expected.write(b);
        return;
      case 1:
        outputStream.write(new byte[10], 3, 0);
        length = 1;
        break;
      case 2:
      case 3:
        length = 1 + random.nextInt(20_000);
        break;
      case 4:
        length = toBoundary;
        break;
      case 5:
        length = toBoundary > 1 ? toBoundary - 1 : 1;
        break;
      case 6:
        length = toBoundary + 1;
        break;
      default:
        length = 1 + random.nextInt(CHUNK_SIZE + 20_000);
        break;
    }

    int offset = random.nextInt(100);
    byte[] data = new byte[offset + length + random.nextInt(100)];
    random.nextBytes(data);
    outputStream.write(data, offset, length);
    expected.write(data, offset, length);
  }

  private void assertFileContents(String context, FileSystem fileSystem, File file,
      byte[] expected) throws IOException {
    assertEquals(context, expected.length, file.getLength());

    int expectedChunks = Math.max(1, (expected.length + CHUNK_SIZE - 1) / CHUNK_SIZE);
    assertEquals(context, expectedChunks, file.chunks);
    for (int i = 0; i < file.chunks; i++) {
      int expectedLength =
          i < file.chunks - 1 ? CHUNK_SIZE : expected.length - (file.chunks - 1) * CHUNK_SIZE;
      assertEquals(context + ", chunk " + i, expectedLength, fileSystem.getChunk(file, i).length);
    }

    byte[] actual = new byte[expected.length];
    try (InputStream inputStream = file.getInputStream()) {
      int read = 0;
      int count;
      while (read < actual.length
          && (count = inputStream.read(actual, read, actual.length - read)) > 0) {
        read += count;
      }
      assertEquals(context, expected.length, read);
      assertEquals(context, -1, inputStream.read());
    }
    assertArrayEquals(context, expected, actual);
  }
}
