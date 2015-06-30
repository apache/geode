package com.gemstone.gemfire.internal;

import java.nio.ByteBuffer;

/**
 * Used by a couple of our classes to say they can have
 * a ByteBuffer written to them. 
 * @author dschneider
 */
public interface ByteBufferWriter {
  /**
   * Writes bb.position()..bb.limit() bytes to this writer.
   * Note that some implementations of this interface will
   * keep a reference to bb so callers should expect to give
   * up ownership of bb and should not modify it after calling
   * this method.
   */
  public void write(ByteBuffer bb);
}
