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
package org.apache.geode.internal;

import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * An OutputStream that wraps to a ByteBuffer
 *
 * @since GemFire 3.5
 */

public class ByteBufferOutputStream extends OutputStream {
  private ByteBuffer buffer;
  private static final int DEFAULT_SIZE = 1024;

  public ByteBufferOutputStream() {
    buffer = ByteBuffer.allocate(DEFAULT_SIZE);
  }

  public ByteBufferOutputStream(int initialSize) {
    buffer = ByteBuffer.allocate(initialSize);
  }

  /** write the low-order 8 bits of the given int */
  @Override
  public void write(int b) {
    try {
      buffer.put((byte) (b & 0xff));
    } catch (BufferOverflowException e) {
      expand(1);
      buffer.put((byte) (b & 0xff));
    } catch (BufferUnderflowException e) {
      expand(1);
      buffer.put((byte) (b & 0xff));
    }
  }

  private void expand(int amount) {
    int oldcap = buffer.capacity();
    int newcap = oldcap + amount + 1024;
    // System.out.println("buffer.capacity=" + buffer.capacity() + " expand amt = " + amount);
    // System.out.println("reallocating buffer size to " + newcap);
    ByteBuffer tmp = ByteBuffer.allocate(newcap);
    buffer.flip();
    tmp.put(buffer);
    buffer = tmp;
  }

  /** override OutputStream's write() */
  @Override
  public void write(byte[] source, int offset, int len) {
    try {
      // System.out.println("writing len="+len + " cap=" + buffer.capacity() + "
      // pos="+buffer.position());
      // Thread.dumpStack();
      buffer.put(source, offset, len);
    } catch (BufferOverflowException e) {
      expand(len - (buffer.capacity() - buffer.position()));
      buffer.put(source, offset, len);
    } catch (BufferUnderflowException e) {
      expand(len - (buffer.capacity() - buffer.position()));
      buffer.put(source, offset, len);
    }
  }

  public int size() {
    return buffer.position();
  }

  public void reset() {
    buffer.clear();
  }

  /**
   * gets the content ByteBuffer, ready for reading. The stream should not be written to past this
   * point until it has been reset.
   */
  public ByteBuffer getContentBuffer() {
    buffer.flip();
    return buffer;
  }

  /**
   * Gets a duplicate of the current content buffer.
   */
  public ByteBuffer getDuplicateBuffer() {
    return buffer.duplicate();
  }
}
