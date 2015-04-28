/*=========================================================================
 * Copyright (c) 2009-2014 Pivotal Software, Inc. All Rights Reserved. This product
 * is protected by U.S. and international copyright and intellectual
 * property laws. Pivotal products are covered by one or more patents listed
 * at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.tcp;

import java.nio.ByteBuffer;

import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataStream;

/**
 * An extension to {@link ByteBufferInputStream} that implements
 * {@link VersionedDataStream} for a stream coming from a different product
 * version.
 * 
 * @author swale
 * @since 7.1
 */
public class VersionedByteBufferInputStream extends ByteBufferInputStream
    implements VersionedDataStream {

  private final Version version;

  /**
   * Create a ByteBuffer input stream whose contents are null at given product
   * {@link Version}.
   * 
   * @param version
   *          the product version for which this stream was created
   */
  public VersionedByteBufferInputStream(Version version) {
    super();
    this.version = version;
  }

  /**
   * Create a ByteBuffer input stream whose contents are the given
   * {@link ByteBuffer} at given product {@link Version}.
   * 
   * @param buffer
   *          the byte buffer to read
   * @param version
   *          the product version for which this stream was created
   */
  public VersionedByteBufferInputStream(ByteBuffer buffer, Version version) {
    super(buffer);
    this.version = version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Version getVersion() {
    return this.version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return super.toString() + " (" + this.version + ')';
  }
}
