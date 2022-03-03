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

import org.apache.geode.distributed.internal.AdminMessageType;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A message that is sent to a particular console distribution manager when changes have been
 * detected that will be of interest to registered stat listeners.
 */
public class StatListenerMessage extends PooledDistributionMessage implements AdminMessageType {

  // instance variables
  private long timestamp;
  private int changeCount;
  private transient int[] listenerIds;
  private transient double[] values;

  /**
   * Creates a new <code>StatListenerMessage</code>
   *
   * @param timestamp The time at which the statistics were sampled
   * @param maxChanges The number of statistics that are reported in the message
   */
  public static StatListenerMessage create(long timestamp, int maxChanges) {
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
   *
   */
  public void addChange(int listenerId, double value) {
    listenerIds[changeCount] = listenerId;
    values[changeCount] = value;
    changeCount++;
  }

  @Override
  public void process(ClusterDistributionManager dm) {
    RemoteGfManagerAgent agent = dm.getAgent();
    if (agent != null) {
      RemoteGemFireVM mgr = agent.getMemberById(getSender());
      if (mgr != null) {
        mgr.callStatListeners(timestamp, listenerIds, values);
      }
    }
  }

  @Override
  public int getDSFID() {
    return STAT_LISTENER_MESSAGE;
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeLong(timestamp);
    out.writeInt(changeCount);
    for (int i = 0; i < changeCount; i++) {
      out.writeInt(listenerIds[i]);
      out.writeDouble(values[i]);
    }
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    timestamp = in.readLong();
    changeCount = in.readInt();
    listenerIds = new int[changeCount];
    values = new double[changeCount];
    for (int i = 0; i < changeCount; i++) {
      listenerIds[i] = in.readInt();
      values[i] = in.readDouble();
    }
  }


}
