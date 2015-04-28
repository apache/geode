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
public final class RegionStatisticsRequest extends RegionAdminRequest {
  // instance variables

  /**
   * Returns a <code>RegionStatisticsRequest</code> to be sent to the specified recipient.
   */
  public static RegionStatisticsRequest create() {
    RegionStatisticsRequest m = new RegionStatisticsRequest();
    return m;
  }

  public RegionStatisticsRequest() {
    friendlyName = LocalizedStrings.RegionStatisticsRequest_FETCH_REGION_STATISTICS.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return RegionStatisticsResponse.create(dm, this.getSender(), this.getRegion(dm.getSystem())); 
  }

  public int getDSFID() {
    return REGION_STATISTICS_REQUEST;
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
    return "RegionStatisticsRequest from " + getRecipient() + " region=" + getRegionName();
  }
}
