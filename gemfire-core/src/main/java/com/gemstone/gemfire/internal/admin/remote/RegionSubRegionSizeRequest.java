/*
 * ========================================================================= 
 * (c)Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.DistributionManager;

/**
 * Admin request to transfer region info for a member
 * 
 * @author Harsh Khanna
 */
public class RegionSubRegionSizeRequest extends AdminRequest implements
    Cancellable {
  public RegionSubRegionSizeRequest() {
    friendlyName = "Refresh the Member's Regions' Statuses";
  }

  /**
   * Returns a <code>RegionSubRegionSizeRequest</code> to be sent to the
   * specified recipient.
   */
  public static RegionSubRegionSizeRequest create() {
    RegionSubRegionSizeRequest m = new RegionSubRegionSizeRequest();
    return m;
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    CancellationRegistry.getInstance().registerMessage(this);

    resp = RegionSubRegionsSizeResponse.create(dm, this.getSender());
    if (cancelled) {
      return null;
    }

    resp.populateSnapshot(dm);

    CancellationRegistry.getInstance().deregisterMessage(this);
    return resp;
  }

  public synchronized void cancel() {
    cancelled = true;
    if (resp != null) {
      resp.cancel();
    }
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
    return REGION_SUB_SIZE_REQUEST;
  }


  @Override
  public String toString() {
    return "RegionSubRegionSizeRequest sent to " + this.getRecipient()
        + " from " + this.getSender();
  }

  private transient boolean cancelled = false;

  private transient RegionSubRegionsSizeResponse resp = null;
}
