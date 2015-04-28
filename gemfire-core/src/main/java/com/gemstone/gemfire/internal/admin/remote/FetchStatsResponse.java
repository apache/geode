/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  =========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.StatisticsVisitor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Provides a response of remote statistic resources for a 
 * <code>FetchStatsRequest</code>
 *
 * @author Darrel Schneider
 * @author Kirk Lund
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
  public boolean sendViaJGroups() {
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

