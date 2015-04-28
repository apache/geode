/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Responds to {@link RegionStatisticsResponse}.
 */
public final class RegionStatisticsResponse extends AdminResponse {
  // instance variables
  RemoteCacheStatistics regionStatistics;
  
  /**
   * Returns a <code>RegionStatisticsResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * system config.
   */
  public static RegionStatisticsResponse create(DistributionManager dm, InternalDistributedMember recipient, Region r) {
    RegionStatisticsResponse m = new RegionStatisticsResponse();
    m.setRecipient(recipient);
    m.regionStatistics = new RemoteCacheStatistics(r.getStatistics());
    return m;
  }

  // instance methods
  public CacheStatistics getRegionStatistics() {
    return this.regionStatistics;
  }
  
  public int getDSFID() {
    return REGION_STATISTICS_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.regionStatistics, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.regionStatistics = (RemoteCacheStatistics)DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "RegionStatisticsResponse from " + this.getRecipient();
  }
}
