/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.cache.*;
import java.io.*;
//import java.util.*;

public final class SnapshotResultMessage extends PooledDistributionMessage
    implements AdminMessageType {
  private CacheSnapshot results;
  private int snapshotId;
  
  public static SnapshotResultMessage create(Region region, int snapshotId)
      throws CacheException {
    SnapshotResultMessage m = new SnapshotResultMessage();
    m.results = new RemoteRegionSnapshot(region);
    m.snapshotId = snapshotId;
    return m;
  }

  @Override
  public void process(DistributionManager dm){
    RemoteGfManagerAgent agent = dm.getAgent();
    if (agent != null){
      agent.enqueueSnapshotResults(this);
    }
  }

  @Override
  public boolean sendViaJGroups() {
    return true;
  }

  public CacheSnapshot getSnapshot() {
    return this.results;
  }

  //called by console to verify these results are for the snapshot
  //currently being processed
  public int getSnapshotId() {
    return this.snapshotId;
  }

  public int getDSFID() {
    return SNAPSHOT_RESULT_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.results, out);
    out.writeInt(this.snapshotId);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.results = (CacheSnapshot)DataSerializer.readObject(in);
    this.snapshotId = in.readInt();
  }

  
}
