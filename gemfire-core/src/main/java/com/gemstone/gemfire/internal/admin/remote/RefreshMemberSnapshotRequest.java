/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;

/**
 * A message that is sent to a particular distribution manager to get its
 * current {@link com.gemstone.gemfire.admin.GemFireMemberStatus}
 * 
 * @author Harsh Khanna
 */
public class RefreshMemberSnapshotRequest extends AdminRequest {
  /**
   * Returns a <code>RefreshMemberSnapshotRequest</code> to be sent to the
   * specified recipient.
   */
  public static RefreshMemberSnapshotRequest create() {
    RefreshMemberSnapshotRequest m = new RefreshMemberSnapshotRequest();
    return m;
  }

  public RefreshMemberSnapshotRequest() {
    friendlyName = "Refresh the Member's Status";
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return RefreshMemberSnapshotResponse.create(dm, this.getSender());
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  public int getDSFID() {    
    return REFRESH_MEMBER_SNAP_REQUEST;
  }


  @Override
  public String toString() {
    return "RefreshMemberSnapshotRequest sent to " + this.getRecipient()
        + " from " + this.getSender();
  }
}
