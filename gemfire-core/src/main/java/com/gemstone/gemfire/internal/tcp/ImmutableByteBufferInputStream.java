/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.tcp;

import java.nio.ByteBuffer;

/**
 * You should only create an instance of this class if the bytes this buffer reads
 * will never change. If you want a buffer than can be refilled with other bytes then
 * create an instance of ByteBufferInputStream instead.
 * Note that even though this class is immutable the position on its ByteBuffer can change.
 * 
 * @author darrel
 * @since 6.6
 */
public class ImmutableByteBufferInputStream extends ByteBufferInputStream {

  /**
   * Create an immutable input stream by whose contents are the first length
   * bytes from the given input stream.
   * @param existing the input stream whose content will go into this stream. Note that this existing stream will be read by this class (a copy is not made) so it should not be changed externally.
   * @param length the number of bytes to put in this stream
   */
  public ImmutableByteBufferInputStream(ByteBufferInputStream existing,
      int length) {
    ByteBuffer bb = existing.slice();
    bb.limit(length);
    setBuffer(bb);
  }
  /**
   * Create an immutable input stream whose contents are the given bytes
   * @param bytes the content of this stream. Note that this byte array will be read by this class (a copy is not made) so it should not be changed externally.
   */
  public ImmutableByteBufferInputStream(byte[] bytes) {
    setBuffer(ByteBuffer.wrap(bytes));
  }

  /**
   * Create an immutable input stream whose contents are the given bytes
   * @param bb the content of this stream. Note that bb will be read by this class (a copy is not made) so it should not be changed externally.
   */
  public ImmutableByteBufferInputStream(ByteBuffer bb) {
    setBuffer(bb.slice());
  }
  /**
   * Create an immutable input stream by copying another. A somewhat shallow copy is made.
   * @param copy the input stream to copy. Note that this copy stream will be read by this class (a copy is not made) so it should not be changed externally.
   */
  public ImmutableByteBufferInputStream(ImmutableByteBufferInputStream copy) {
    super(copy);
  }
  public ImmutableByteBufferInputStream() {
    // for serialization
  }
  
  @Override
  public boolean markSupported() {
    return false;
  }
  @Override
  public void mark(int limit) {
    // unsupported but exception thrown by reset
  }
  @Override
  public void reset() {
    throw new UnsupportedOperationException();
  }
}
