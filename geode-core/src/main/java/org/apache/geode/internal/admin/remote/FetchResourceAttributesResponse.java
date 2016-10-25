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
   
   
package org.apache.geode.internal.admin.remote;

import org.apache.geode.distributed.internal.*;
import org.apache.geode.*;
import java.io.*;
import org.apache.geode.distributed.internal.membership.*;

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
