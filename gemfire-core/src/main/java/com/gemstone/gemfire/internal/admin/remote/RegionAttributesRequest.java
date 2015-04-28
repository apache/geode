/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
//import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular app vm to request the region
 * statistics of a given region.
 */
public final class RegionAttributesRequest extends RegionAdminRequest {
  // instance variables

  /**
   * Returns a <code>RegionAttributesRequest</code> to be sent to the specified recipient.
   */
  public static RegionAttributesRequest create() {
    RegionAttributesRequest m = new RegionAttributesRequest();
    return m;
  }

  public RegionAttributesRequest() {
    friendlyName = LocalizedStrings.RegionAttributesRequest_FETCH_REGION_ATTRIBUTES.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return RegionAttributesResponse.create(dm, this.getSender(), this.getRegion(dm.getSystem())); 
  }

  public int getDSFID() {
    return REGION_ATTRIBUTES_REQUEST;
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
    return "RegionAttributesRequest from " + getRecipient() + " region=" + getRegionName();
  }
}
