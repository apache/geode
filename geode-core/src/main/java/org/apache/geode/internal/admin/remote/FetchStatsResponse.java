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

import org.apache.geode.DataSerializer;
import org.apache.geode.Statistics;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem.StatisticsVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import org.apache.geode.distributed.internal.membership.*;

/**
 * Provides a response of remote statistic resources for a 
 * <code>FetchStatsRequest</code>
 *
 */
public final class FetchStatsResponse extends AdminResponse {

  //instance variables
  private RemoteStatResource[] stats;

  /**
   * Generate a complete response to request for stats. 
   *
   * @param dm         DistributionManager that is responding
   * @param recipient  the recipient who made the original request
   * @return           response containing all remote stat resources
   */
  public static FetchStatsResponse create(DistributionManager dm,
                                          InternalDistributedMember recipient, 
                                          final String statisticsTypeName) {
//    LogWriterI18n log = dm.getLogger();
    FetchStatsResponse m = new FetchStatsResponse();
    m.setRecipient(recipient);
    final List<RemoteStatResource> statList = new ArrayList<RemoteStatResource>();
    //get vm-local stats
    // call visitStatistics to fix for bug 40358
    if (statisticsTypeName == null) {
      dm.getSystem().visitStatistics(new StatisticsVisitor() {
          public void visit(Statistics s) {
            statList.add(new RemoteStatResource(s));
          }
        });
    } else {
      dm.getSystem().visitStatistics(new StatisticsVisitor() {
          public void visit(Statistics s) {
            if (s.getType().getName().equals(statisticsTypeName)) {
              statList.add(new RemoteStatResource(s));
            }
          }
        });
    }
    m.stats = new RemoteStatResource[statList.size()];
    m.stats = (RemoteStatResource[]) statList.toArray(m.stats);
    return m;
  }

  
  @Override
  public boolean sendViaUDP() {
    return true;
  }

  public int getDSFID() {
    return FETCH_STATS_RESPONSE;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(stats, out);
  }

  @Override  
  public void fromData(DataInput in) 
                throws IOException,
                       ClassNotFoundException {
    super.fromData(in);
    stats = (RemoteStatResource[]) DataSerializer.readObject(in);
  }

  /**
   * Retrieves all statistic resources from the specified VM.
   *
   * @param vm  local representation of remote vm that stats came from
   * @return    array of all statistic resources
   */
  public RemoteStatResource[] getAllStats(RemoteGemFireVM vm) {
    for (int i = 0; i < stats.length; i++) {
      stats[i].setGemFireVM(vm);
    }
    return stats;
  }

  /**
   * Retrieves all statistic resources from the specified VM except for those
   * involving SharedClass. This is used by the GUI Console.
   *
   * @param vm  local representation of remote vm that stats came from
   * @return    array of non-SharedClass statistic resources
   */
  public RemoteStatResource[] getStats(RemoteGemFireVM vm) {
    List statList = new ArrayList();
    for (int i = 0; i < stats.length; i++) {
      stats[i].setGemFireVM(vm);
      statList.add(stats[i]);
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
    return "FetchStatsResponse from " + this.getRecipient() + " stats.length=" + stats.length;
  }
  
}

