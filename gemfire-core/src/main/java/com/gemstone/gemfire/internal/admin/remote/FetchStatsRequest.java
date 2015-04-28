/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.*;
//import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
import java.io.*;
//import java.util.*;

public final class FetchStatsRequest extends AdminRequest {
  
  private String statisticsTypeName;
  
  /**
   * Returns a <code>FetchStatsRequest</code> to be sent to the specified recipient.
   */
  public static FetchStatsRequest create(String statisticsTypeName) {
    FetchStatsRequest m = new FetchStatsRequest();
    m.statisticsTypeName = statisticsTypeName;
    return m;
  }

  public FetchStatsRequest() {
    friendlyName = "List statistic resources";
    statisticsTypeName = null;
  }

  @Override  
  public AdminResponse createResponse(DistributionManager dm){
    return FetchStatsResponse.create(dm, this.getSender(), this.statisticsTypeName);
  }

  public int getDSFID() {
    return FETCH_STATS_REQUEST;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.statisticsTypeName, out);
  }

  @Override  
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.statisticsTypeName =DataSerializer.readString(in);
  }

  @Override  
  public String toString(){
    return "FetchStatsRequest from " + this.getRecipient();
  }
}
