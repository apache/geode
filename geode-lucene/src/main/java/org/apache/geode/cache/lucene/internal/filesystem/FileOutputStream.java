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
import java.nio.ByteBuffer;
import java.util.Arrays;

class FileOutputStream extends OutputStream {

  private final File file;
  private ByteBuffer buffer;
  private boolean open = true;
  private long length;
  private int chunks;

  public FileOutputStream(final File file) {
    this.file = file;
    buffer = ByteBuffer.allocate(file.getChunkSize());
    length = file.length;
    chunks = file.chunks;
    if (chunks > 0 && file.length % file.getChunkSize() != 0) {
      // If the last chunk was incomplete, we're going to update it
      // rather than add a new chunk. This guarantees that all chunks
      // are full except for the last chunk.
      chunks--;
      byte[] previousChunkData = file.getFileSystem().getChunk(file, chunks);
      buffer.put(previousChunkData);
    }
  }

  @Override
  public void write(final int b) throws IOException {
    assertOpen();

    if (buffer.remaining() == 0) {
      flushBuffer();
    }

    buffer.put((byte) b);
    length++;
  }

  @Override
  public void write(final byte[] b, int off, int len) throws IOException {
    assertOpen();

    while (len > 0) {
      if (buffer.remaining() == 0) {
        flushBuffer();
      }

      final int min = Math.min(buffer.remaining(), len);
      buffer.put(b, off, min);
      off += min;
      len -= min;
      length += min;
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
      buffer = null;
    }
  }

  private void flushBuffer() {
    byte[] chunk = Arrays.copyOfRange(buffer.array(), buffer.arrayOffset(), buffer.position());
    file.getFileSystem().putChunk(file, chunks++, chunk);
    buffer.rewind();
  }

  private void assertOpen() throws IOException {
    if (!open) {
      throw new IOException("Closed");
    }
  }
}
