/*=========================================================================
 * Copyright (c) 2009-2014 Pivotal Software, Inc. All Rights Reserved. This product
 * is protected by U.S. and international copyright and intellectual
 * property laws. Pivotal products are covered by one or more patents listed
 * at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal;

import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.internal.InternalDataSerializer.Sendable;

/**
 * @author kneeraj
 * @since gfxd 1.0.1
 */
public class InsufficientDiskSpaceException extends DiskAccessException implements Sendable {
  private static final long serialVersionUID = -6167707908956900841L;

  public InsufficientDiskSpaceException(String msg, Throwable cause, DiskStore ds) {
    super(msg, cause, ds);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void sendTo(DataOutput out) throws IOException {
    // send base DiskAccessException to older versions
    Version peerVersion = InternalDataSerializer.getVersionForDataStream(out);
    if (Version.GFE_80.compareTo(peerVersion) > 0) {
      DiskAccessException dae = new DiskAccessException(getMessage(),
          getCause());
      InternalDataSerializer.writeSerializableObject(dae, out);
    }
    else {
      InternalDataSerializer.writeSerializableObject(this, out);
    }
  }
}
