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
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular app vm to request all the subregions
 * of a given parent region.
 */
public final class RegionSizeRequest extends RegionAdminRequest  implements Cancellable {
  private transient boolean cancelled;
  private transient RegionSizeResponse resp;

  /**
   * Returns a <code>ObjectNamesRequest</code> to be sent to the specified recipient.
   */
  public static RegionSizeRequest create() {
    RegionSizeRequest m = new RegionSizeRequest();
    return m;
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    Assert.assertTrue(this.getSender() != null);
    CancellationRegistry.getInstance().registerMessage(this);
    resp = RegionSizeResponse.create(dm, this.getSender());
    if (cancelled) { return null; }
    resp.calcSize(this.getRegion(dm.getSystem()));
    if (cancelled) { return null; }
    CancellationRegistry.getInstance().deregisterMessage(this);
    return resp;
  }

  public RegionSizeRequest() {
    friendlyName = LocalizedStrings.RegionSizeRequest_FETCH_REGION_SIZE.toLocalizedString();
  }

  public synchronized void cancel() {
    cancelled = true;
    if (resp != null) {
      resp.cancel();
    }
  }

  public int getDSFID() {
    return REGION_SIZE_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public String toString() {
    return "RegionSizeRequest from " + getRecipient() + " region=" + getRegionName();
  }
}
