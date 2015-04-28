/*=========================================================================
 * Copyright (c) 2009-2014 Pivotal Software, Inc. All Rights Reserved. This product
 * is protected by U.S. and international copyright and intellectual
 * property laws. Pivotal products are covered by one or more patents listed
 * at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal;

import java.io.DataInputStream;
import java.io.InputStream;


/**
 * An extension to {@link DataInputStream} that implements
 * {@link VersionedDataStream} for a stream coming from a different product
 * version.
 * 
 * @author swale
 * @since 7.1
 */
public final class VersionedDataInputStream extends DataInputStream implements
    VersionedDataStream {

  private final Version version;

  /**
   * Creates a VersionedDataInputStream that uses the specified underlying
   * InputStream.
   * 
   * @param in
   *          the specified input stream
   * @param version
   *          the product version that serialized object on the given input
   *          stream
   */
  public VersionedDataInputStream(InputStream in, Version version) {
    super(in);
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
