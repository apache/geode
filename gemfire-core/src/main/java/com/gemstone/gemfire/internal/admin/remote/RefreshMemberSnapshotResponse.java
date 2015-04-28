/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.admin.GemFireMemberStatus;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 * A message that is sent to a particular distribution manager to get its
 * current {@link com.gemstone.gemfire.admin.GemFireMemberStatus}.
 * 
 * @author Harsh Khanna
 */
public class RefreshMemberSnapshotResponse extends AdminResponse {
  // instance variables
  GemFireMemberStatus snapshot;

  /**
   * Returns a <code>FetchSysCfgResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local
   * manager's config.
   */
  public static RefreshMemberSnapshotResponse create(DistributionManager dm,
      InternalDistributedMember recipient) {
    RefreshMemberSnapshotResponse m = new RefreshMemberSnapshotResponse();
    m.setRecipient(recipient);

    try {
      DistributedSystem sys = dm.getSystem();
      GemFireCacheImpl c = (GemFireCacheImpl)CacheFactory.getInstance(sys);
      m.snapshot = new GemFireMemberStatus(c);
    }
    catch (Exception ex) {
      m.snapshot = null;
    }
    return m;
  }

  /**
   * @return return the snapshot of Gemfire member vm
   */
  public GemFireMemberStatus getSnapshot() {
    return this.snapshot;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.snapshot, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.snapshot = (GemFireMemberStatus)DataSerializer.readObject(in);
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  public int getDSFID() {    
    return REFRESH_MEMBER_SNAP_RESPONSE;
  }


  @Override
  public String toString() {
    return "RefreshMemberSnapshotResponse from " + this.getRecipient()
        + " snapshot=" + this.snapshot;
  }
}
