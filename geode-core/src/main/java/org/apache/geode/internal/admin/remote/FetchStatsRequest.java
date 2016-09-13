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
