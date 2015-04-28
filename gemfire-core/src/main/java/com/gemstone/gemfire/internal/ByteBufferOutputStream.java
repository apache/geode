/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import java.io.*;
import java.nio.*;

/** An OutputStream that wraps to a ByteBuffer
 * @author Darrel
 * @since 3.5
 */

public class ByteBufferOutputStream extends OutputStream {
  private ByteBuffer buffer;
  private final static int DEFAULT_SIZE = 1024;

  public ByteBufferOutputStream() {
    this.buffer = ByteBuffer.allocate(DEFAULT_SIZE);
  }
  public ByteBufferOutputStream(int initialSize) {
    this.buffer = ByteBuffer.allocate(initialSize);
  }

  /** write the low-order 8 bits of the given int */
  @Override
  public final void write(int b) {
    try {
      this.buffer.put((byte)(b & 0xff));
    }
    catch (BufferOverflowException e) {
      expand(1);
      this.buffer.put((byte)(b & 0xff));
    } catch (BufferUnderflowException e) {
      expand(1);
      this.buffer.put((byte)(b & 0xff));
    }
  }

  private void expand(int amount) {
    int oldcap = this.buffer.capacity();
    int newcap = oldcap + amount + 1024;
    //System.out.println("buffer.capacity=" + buffer.capacity() + " expand amt = " + amount);
    //System.out.println("reallocating buffer size to " + newcap);
    ByteBuffer tmp = ByteBuffer.allocate(newcap);
    this.buffer.flip();
    tmp.put(this.buffer);
    this.buffer = tmp;
  }

  /** override OutputStream's write() */
  @Override
  public final void write(byte[] source, int offset, int len) {
    try {
      //System.out.println("writing len="+len + " cap=" + buffer.capacity() + " pos="+buffer.position());
      //Thread.dumpStack();
      buffer.put(source, offset, len);
    }
    catch (BufferOverflowException e) {
      expand(len - (buffer.capacity()-buffer.position()));
      buffer.put(source, offset, len);
    }
    catch (BufferUnderflowException e) {
      expand(len - (buffer.capacity()-buffer.position()));
      buffer.put(source, offset, len);
    }
  }

  public final int size() {
    return buffer.position();
  }
  
  public final void reset() {
    buffer.clear();
  }

  /** gets the content ByteBuffer, ready for reading.  The stream should
      not be written to past this point until it has been reset. */
  public final ByteBuffer getContentBuffer() {
    buffer.flip();
    return buffer;
  }

  /** Gets a duplicate of the current content buffer.
   */
  public final ByteBuffer getDuplicateBuffer() {
    return buffer.duplicate();
  }
}
