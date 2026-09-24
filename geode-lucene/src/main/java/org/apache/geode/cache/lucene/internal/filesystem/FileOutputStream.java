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

import java.io.IOException;
import java.io.OutputStream;

/**
 * Buffers the current chunk in fixed-size segments that are allocated as they are first needed and
 * reused for every later chunk of the stream. A chunk is copied into an array of its exact size
 * when it is written.
 */
class FileOutputStream extends OutputStream {

  private static final int SEGMENT_SIZE = 8 * 1024;

  private final File file;
  private final int chunkSize;
  private final int segmentSize;
  private byte[][] segments;
  private int position;
  private boolean open = true;
  private long length;
  private int chunks;

  public FileOutputStream(final File file) {
    this.file = file;
    chunkSize = file.getChunkSize();
    segmentSize = Math.min(SEGMENT_SIZE, chunkSize);
    segments = new byte[(chunkSize + segmentSize - 1) / segmentSize][];
    length = file.length;
    chunks = file.chunks;
    if (chunks > 0 && file.length % chunkSize != 0) {
      // If the last chunk was incomplete, we're going to update it
      // rather than add a new chunk. This guarantees that all chunks
      // are full except for the last chunk.
      chunks--;
      byte[] previousChunkData = file.getFileSystem().getChunk(file, chunks);
      buffer(previousChunkData, 0, previousChunkData.length);
    }
  }

  @Override
  public void write(final int b) throws IOException {
    assertOpen();

    if (position == chunkSize) {
      flushBuffer();
    }

    segment(position / segmentSize)[position % segmentSize] = (byte) b;
    position++;
    length++;
  }

  @Override
  public void write(final byte[] b, int off, int len) throws IOException {
    assertOpen();

    while (len > 0) {
      if (position == chunkSize) {
        flushBuffer();
      }

      final int copied = buffer(b, off, len);
      off += copied;
      len -= copied;
      length += copied;
    }
  }

  @Override
  public void close() throws IOException {
    if (open) {
      flushBuffer();
      file.modified = System.currentTimeMillis();
      file.length = length;
      file.chunks = chunks;
      file.getFileSystem().updateFile(file);
      open = false;
      segments = null;
    }
  }

  /**
   * Copies bytes into the current chunk, up to the end of the chunk.
   *
   * @return the number of bytes copied
   */
  private int buffer(final byte[] b, int off, final int len) {
    final int limit = Math.min(len, chunkSize - position);
    int copied = 0;
    while (copied < limit) {
      final int offsetInSegment = position % segmentSize;
      final int count = Math.min(limit - copied, segmentSize - offsetInSegment);
      System.arraycopy(b, off, segment(position / segmentSize), offsetInSegment, count);
      off += count;
      copied += count;
      position += count;
    }
    return copied;
  }

  private byte[] segment(final int index) {
    byte[] segment = segments[index];
    if (segment == null) {
      segment = new byte[segmentSize];
      segments[index] = segment;
    }
    return segment;
  }

  private void flushBuffer() {
    final byte[] chunk = new byte[position];
    for (int copied = 0; copied < position; copied += segmentSize) {
      System.arraycopy(segments[copied / segmentSize], 0, chunk, copied,
          Math.min(segmentSize, position - copied));
    }
    file.getFileSystem().putChunk(file, chunks++, chunk);
    position = 0;
  }

  private void assertOpen() throws IOException {
    if (!open) {
      throw new IOException("Closed");
    }
  }
}
