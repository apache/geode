/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
//import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
import java.io.*;


/**
 * A message to cause a remote application to release any snapshot info it
 * was holding on behalf of a console.
 */
public final class FlushAppCacheSnapshotMessage extends PooledDistributionMessage {

  public static FlushAppCacheSnapshotMessage create() {
    FlushAppCacheSnapshotMessage m = new FlushAppCacheSnapshotMessage();
    return m;
  }


  @Override
  protected void process(DistributionManager dm) {
//     try {
//       AppCacheSnapshotMessage.flushSnapshots(this.getSender());
//     } catch (Exception ex) {
//       LogWriterI18n logger = dm.getLogger();
//       if (logger != null)
//         logger.warning("Failed " + this, ex);
//     }
  }

  public int getDSFID() {
    return FLUSH_APP_CACHE_SNAPSHOT_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public String toString() {
    return "FlushAppCacheSnapshotMessage from " + this.getSender();
  }
}
