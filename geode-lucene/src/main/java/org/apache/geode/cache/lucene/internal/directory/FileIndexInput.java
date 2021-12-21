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

package org.apache.geode.cache.lucene.internal.directory;

import java.io.EOFException;
import java.io.IOException;

import org.apache.lucene.store.IndexInput;

import org.apache.geode.cache.lucene.internal.filesystem.File;
import org.apache.geode.cache.lucene.internal.filesystem.SeekableInputStream;

class FileIndexInput extends IndexInput {

  private final File file;
  SeekableInputStream in;
  private long position;

  // Used for slice operations
  private final long sliceOffset;
  private final long sliceLength;

  FileIndexInput(String resourceDesc, File file) {
    this(resourceDesc, file, 0L, file.getLength());
  }

  /**
   * Constructor for a slice.
   */
  private FileIndexInput(String resourceDesc, File file, long offset, long length) {
    super(resourceDesc);
    this.file = file;
    in = file.getInputStream();
    sliceOffset = offset;
    sliceLength = length;
  }

  @Override
  public long length() {
    return sliceLength;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }

  @Override
  public FileIndexInput clone() {
    FileIndexInput clone = (FileIndexInput) super.clone();
    clone.in = in.clone();
    return clone;
  }

  @Override
  public long getFilePointer() {
    return position;
  }

  @Override
  public void seek(long pos) throws IOException {
    in.seek(pos + sliceOffset);
    position = pos;
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    if (length > (sliceLength - offset)) {
      throw new IllegalArgumentException("Slice length is to large. Asked for " + length
          + " file length is " + sliceLength + ": " + file.getName());
    }
    if (offset < 0 || offset >= sliceLength) {
      throw new IllegalArgumentException("Slice offset is invalid: " + file.getName());
    }

    FileIndexInput result =
        new FileIndexInput(sliceDescription, file, sliceOffset + offset, length);
    result.seek(0);
    return result;
  }

  @Override
  public byte readByte() throws IOException {
    if (++position > sliceLength) {
      throw new EOFException("Read past end of file " + file.getName());
    }

    int result = in.read();
    if (result == -1) {
      throw new EOFException("Read past end of file " + file.getName());
    } else {
      return (byte) result;
    }
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) throws IOException {
    if (len == 0) {
      return;
    }

    if (position + len > sliceLength) {
      throw new EOFException("Read past end of file " + file.getName());
    }

    // For the FileSystemInputStream, it will always read all bytes, up
    // until the end of the file. So if we didn't get enough bytes, it's
    // because we reached the end of the file.
    int numRead = in.read(b, offset, len);
    if (numRead < len) {
      throw new EOFException("Read past end of file " + file.getName());
    }

    position += len;
  }
}
