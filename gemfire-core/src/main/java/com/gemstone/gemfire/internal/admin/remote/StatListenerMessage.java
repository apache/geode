/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
//import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular console distribution manager
 * when changes have been detected that will be of interest to
 * registered stat listeners.
 */
public final class StatListenerMessage extends PooledDistributionMessage
  implements AdminMessageType {

  //instance variables
  private long timestamp;
  private int changeCount;
  private transient int[] listenerIds;
  private transient double[] values;

  /**
   * Creates a new <code>StatListenerMessage</code>
   *
   * @param timestamp
   *        The time at which the statistics were sampled
   * @param maxChanges
   *        The number of statistics that are reported in the message
   */
  public static StatListenerMessage create(long timestamp, int maxChanges){
    StatListenerMessage m = new StatListenerMessage();
    m.timestamp = timestamp;
    m.changeCount = 0;
    m.listenerIds = new int[maxChanges];
    m.values = new double[maxChanges];
    return m;
  }

  /**
   * Notes that the value of a given statistics has changed
   *
   * @param listenerId
   *        
   */
  public void addChange(int listenerId, double value) {
    listenerIds[changeCount] = listenerId;
    values[changeCount] = value;
    changeCount++;
  }
  
  @Override
  public void process(DistributionManager dm) {
    RemoteGfManagerAgent agent = dm.getAgent();
    if (agent != null) {
      RemoteGemFireVM mgr =
        agent.getMemberById(this.getSender());
      if (mgr != null) {
        mgr.callStatListeners(timestamp, listenerIds, values);
      }
    }
  }

  public int getDSFID() {
    return STAT_LISTENER_MESSAGE;
  }

  @Override
  public boolean sendViaJGroups() {
    return true;
  }

@Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeLong(this.timestamp);
    out.writeInt(this.changeCount);
    for (int i = 0; i < this.changeCount; i++) {
      out.writeInt(this.listenerIds[i]);
      out.writeDouble(this.values[i]);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.timestamp = in.readLong();
    this.changeCount = in.readInt();
    this.listenerIds = new int[this.changeCount];
    this.values = new double[this.changeCount];
    for (int i = 0; i < this.changeCount; i++) {
      this.listenerIds[i] = in.readInt();
      this.values[i] = in.readDouble();
    }
  }

  
}
