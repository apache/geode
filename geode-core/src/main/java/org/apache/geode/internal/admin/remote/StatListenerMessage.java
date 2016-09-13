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
//import org.apache.geode.*;
//import org.apache.geode.internal.*;
//import org.apache.geode.internal.admin.*;
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
  public boolean sendViaUDP() {
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
