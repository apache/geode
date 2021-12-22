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
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.DataSerializer;
import org.apache.geode.Statistics;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * Provides a response of remote statistic resources for a {@code FetchStatsRequest}
 */
public class FetchStatsResponse extends AdminResponse {

  private RemoteStatResource[] stats;

  /**
   * Generate a complete response to request for stats.
   *
   * @param dm DistributionManager that is responding
   * @param recipient the recipient who made the original request
   * @return response containing all remote stat resources
   */
  public static FetchStatsResponse create(DistributionManager dm,
      InternalDistributedMember recipient, final String statisticsTypeName) {
    FetchStatsResponse response = new FetchStatsResponse();
    response.setRecipient(recipient);

    List<Statistics> statsList = dm.getSystem().getStatisticsManager().getStatsList();
    if (statisticsTypeName == null) {
      response.stats = statsList.stream()
          .map(RemoteStatResource::new)
          .toArray(RemoteStatResource[]::new);
    } else {
      response.stats = statsList.stream()
          .filter(s -> s.getType().getName().equals(statisticsTypeName))
          .map(RemoteStatResource::new)
          .toArray(RemoteStatResource[]::new);
    }
    return response;
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  @Override
  public int getDSFID() {
    return FETCH_STATS_RESPONSE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    DataSerializer.writeObject(stats, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    stats = DataSerializer.readObject(in);
  }

  /**
   * Retrieves all statistic resources from the specified VM.
   *
   * @param vm local representation of remote vm that stats came from
   * @return array of all statistic resources
   */
  public RemoteStatResource[] getAllStats(RemoteGemFireVM vm) {
    for (final RemoteStatResource stat : stats) {
      stat.setGemFireVM(vm);
    }
    return stats;
  }

  /**
   * Retrieves all statistic resources from the specified VM except for those involving SharedClass.
   * This is used by the GUI Console.
   *
   * @param vm local representation of remote vm that stats came from
   * @return array of non-SharedClass statistic resources
   */
  public RemoteStatResource[] getStats(RemoteGemFireVM vm) {
    List statList = new ArrayList();
    for (final RemoteStatResource stat : stats) {
      stat.setGemFireVM(vm);
      statList.add(stat);
    }
    return (RemoteStatResource[]) statList.toArray(new RemoteStatResource[0]);
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    return "FetchStatsResponse from " + getRecipient() + " stats.length=" + stats.length;
  }

}
