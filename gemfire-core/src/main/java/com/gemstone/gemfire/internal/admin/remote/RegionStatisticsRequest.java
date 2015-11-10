/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
