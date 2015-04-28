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
import java.io.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

public final class FetchResourceAttributesResponse extends AdminResponse {
  
  // instance variables
  private RemoteStat[] stats;

  public static FetchResourceAttributesResponse create(DistributionManager dm, InternalDistributedMember recipient, long rsrcUniqueId) {
    FetchResourceAttributesResponse m = new FetchResourceAttributesResponse();
    m.setRecipient(recipient);
    Statistics s = null;
    InternalDistributedSystem ds = dm.getSystem();
    s = ds.findStatisticsByUniqueId(rsrcUniqueId);
    if (s != null) {
      StatisticsType type = s.getType();
      StatisticDescriptor[] tmp = type.getStatistics();
      m.stats = new RemoteStat[tmp.length];
      for (int i=0; i < tmp.length; i++) {
        m.stats[i] = new RemoteStat(s, tmp[i]);
      }
    }
    if (m.stats == null) {
      m.stats = new RemoteStat[0];
    }
    return m;
  }

  public RemoteStat[] getStats(){
    return stats;
  }

  /**
   * Constructor required by <code>DataSerializable</code>
   */
  public FetchResourceAttributesResponse() { }

  public int getDSFID() {
    return FETCH_RESOURCE_ATTRIBUTES_RESPONSE;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(stats, out);
  }

  @Override  
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    stats = (RemoteStat[])DataSerializer.readObject(in);
  }

  @Override  
  public String toString(){
    return "FetchResourceAttributesResponse from " + this.getRecipient();
  }
}
