/*=========================================================================
 * Copyright (c) 2009-2014 Pivotal Software, Inc. All Rights Reserved. This product
 * is protected by U.S. and international copyright and intellectual
 * property laws. Pivotal products are covered by one or more patents listed
 * at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.tcp;

import java.util.List;

import com.gemstone.gemfire.distributed.internal.DMStats;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedDataStream;

/**
 * An extension of {@link MsgStreamer} that implements
 * {@link VersionedDataStream}.
 * 
 * @author swale
 * @since 7.1
 */
final class VersionedMsgStreamer extends MsgStreamer implements
    VersionedDataStream {

  private final Version version;

  VersionedMsgStreamer(List<?> cons, DistributionMessage msg,
      boolean directReply, DMStats stats, int sendBufferSize, Version version) {
    super(cons, msg, directReply, stats, sendBufferSize);
    this.version = version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final Version getVersion() {
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
