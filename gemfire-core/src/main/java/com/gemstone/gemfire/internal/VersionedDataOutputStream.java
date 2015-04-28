/*=========================================================================
 * Copyright (c) 2009-2014 Pivotal Software, Inc. All Rights Reserved. This product
 * is protected by U.S. and international copyright and intellectual
 * property laws. Pivotal products are covered by one or more patents listed
 * at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal;

import java.io.DataOutputStream;
import java.io.OutputStream;

/**
 * An extension of {@link DataOutputStream} that implements
 * {@link VersionedDataStream}.
 * 
 * @author swale
 * @since 7.1
 */
public final class VersionedDataOutputStream extends DataOutputStream implements
    VersionedDataStream {

  private final Version version;

  /**
   * Creates a VersionedDataOutputStream that wraps the specified underlying
   * OutputStream.
   * 
   * @param out
   *          the underlying output stream
   * @param version
   *          the product version that serialized object on the given
   *          {@link OutputStream}
   */
  public VersionedDataOutputStream(OutputStream out, Version version) {
    super(out);
    this.version = version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final Version getVersion() {
    return this.version;
  }
}
