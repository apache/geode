/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */


package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class FetchStatsRequest extends AdminRequest {

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
  public AdminResponse createResponse(DistributionManager dm) {
    return FetchStatsResponse.create(dm, getSender(), statisticsTypeName);
  }

  @Override
  public int getDSFID() {
    return FETCH_STATS_REQUEST;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeString(statisticsTypeName, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    statisticsTypeName = DataSerializer.readString(in);
  }

  @Override
  public String toString() {
    return "FetchStatsRequest from " + getRecipient();
  }
}
