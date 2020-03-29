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

import java.io.EOFException;
import java.io.IOException;

/**
 * An input stream that reads chunks from a File saved in the region. This input stream will keep
 * going back to the region to look for chunks until nothing is found.
 */
class FileInputStream extends SeekableInputStream {

  private final File file;
  private byte[] chunk = null;
  private int chunkPosition = 0;
  private int chunkId = 0;
  private boolean open = true;

  public FileInputStream(File file) {
    this.file = file;
    nextChunk();
  }

  public FileInputStream(FileInputStream other) {
    this.file = other.file;
    this.chunk = other.chunk;
    this.chunkId = other.chunkId;
    this.chunkPosition = other.chunkPosition;
    this.open = other.open;
  }

  @Override
  public int read() throws IOException {
    assertOpen();

    checkAndFetchNextChunk();

    if (null == chunk) {
      return -1;
    }

    return chunk[chunkPosition++] & 0xff;
  }

  @Override
  public void seek(long position) throws IOException {
    if (position > file.length) {
      throw new EOFException();
    }
    int targetChunk = (int) (position / file.getChunkSize());
    int targetPosition = (int) (position % file.getChunkSize());

    if (targetChunk != (this.chunkId - 1)) {
      chunk = file.getFileSystem().getChunk(this.file, targetChunk);
      chunkId = targetChunk + 1;
      chunkPosition = targetPosition;
    } else {
      chunkPosition = targetPosition;
    }
  }



  @Override
  public long skip(long n) throws IOException {
    int currentPosition = (chunkId - 1) * file.getChunkSize() + chunkPosition;
    seek(currentPosition + n);
    return n;
  }

  @Override
  public synchronized void reset() throws IOException {
    seek(0);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    assertOpen();

    checkAndFetchNextChunk();

    if (null == chunk) {
      return -1;
    }

    int read = 0;
    while (len > 0) {
      final int min = Math.min(remaining(), len);
      System.arraycopy(chunk, chunkPosition, b, off, min);
      off += min;
      len -= min;
      chunkPosition += min;
      read += min;

      if (len > 0) {
        // we read to the end of the chunk, fetch another.
        nextChunk();
        if (null == chunk) {
          break;
        }
      }
    }

    return read;
  }

  @Override
  public int available() throws IOException {
    assertOpen();

    return remaining();
  }

  @Override
  public void close() throws IOException {
    if (open) {
      open = false;
    }
  }

  private int remaining() {
    return chunk.length - chunkPosition;
  }

  private void checkAndFetchNextChunk() {
    if (null != chunk && remaining() <= 0) {
      nextChunk();
    }
  }

  private void nextChunk() {
    chunk = file.getFileSystem().getChunk(this.file, chunkId++);
    chunkPosition = 0;
  }

  private void assertOpen() throws IOException {
    if (!open) {
      throw new IOException("Closed");
    }
  }

  @Override
  public FileInputStream clone() {
    return new FileInputStream(this);
  }
}
