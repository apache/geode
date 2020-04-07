/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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
package org.apache.geode.internal.tcp;

import java.nio.ByteBuffer;

import org.apache.geode.internal.offheap.StoredObject;

/**
 * You should only create an instance of this class if the bytes this buffer reads will never
 * change. If you want a buffer than can be refilled with other bytes then create an instance of
 * ByteBufferInputStream instead. Note that even though this class is immutable the position on its
 * ByteBuffer can change.
 *
 * @since GemFire 6.6
 */
public class ImmutableByteBufferInputStream extends ByteBufferInputStream {

  /**
   * Create an immutable input stream by whose contents are the first length bytes from the given
   * input stream.
   *
   * @param existing the input stream whose content will go into this stream. Note that this
   *        existing stream will be read by this class (a copy is not made) so it should not be
   *        changed externally.
   * @param length the number of bytes to put in this stream
   */
  public ImmutableByteBufferInputStream(ByteBufferInputStream existing, int length) {
    setBuffer(existing.slice(length));
  }

  /**
   * Create an immutable input stream whose contents are the given bytes
   *
   * @param bytes the content of this stream. Note that this byte array will be read by this class
   *        (a copy is not made) so it should not be changed externally.
   */
  public ImmutableByteBufferInputStream(byte[] bytes) {
    setBuffer(ByteBuffer.wrap(bytes));
  }

  /**
   * Create an immutable input stream whose contents are the given bytes
   *
   * @param bb the content of this stream. Note that bb will be read by this class (a copy is not
   *        made) so it should not be changed externally.
   */
  public ImmutableByteBufferInputStream(ByteBuffer bb) {
    setBuffer(bb.slice());
  }

  /**
   * Create an immutable input stream by copying another. A somewhat shallow copy is made.
   *
   * @param copy the input stream to copy. Note that this copy stream will be read by this class (a
   *        copy is not made) so it should not be changed externally.
   */
  public ImmutableByteBufferInputStream(ImmutableByteBufferInputStream copy) {
    super(copy);
  }

  public ImmutableByteBufferInputStream() {
    // for serialization
  }

  public ImmutableByteBufferInputStream(StoredObject blob) {
    super(blob);
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public synchronized void mark(int limit) {
    // unsupported but exception thrown by reset
  }

  @Override
  public synchronized void reset() {
    throw new UnsupportedOperationException();
  }
}
