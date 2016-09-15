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

import org.apache.geode.*;
import org.apache.geode.admin.AlertLevel;
import org.apache.geode.distributed.internal.*;
//import org.apache.geode.internal.*;
import org.apache.geode.internal.admin.*;
import org.apache.geode.management.internal.AlertDetails;

import java.io.*;
import java.util.*;
import org.apache.geode.distributed.internal.membership.*;

/**
 * A message that is sent to a particular console distribution manager
 * to notify it of an alert.
 */
public final class AlertListenerMessage extends PooledDistributionMessage implements AdminMessageType {
  //instance variables
  private int msgLevel;
  private Date msgDate;
  private String connectionName;
  private String threadName;
  private long tid;
  private String msg;
  private String exceptionText;
  
  public static AlertListenerMessage create(Object recipient,
                                            int msgLevel,
                                            Date msgDate,
                                            String connectionName,
                                            String threadName,
                                            long tid,
                                            String msg,
                                            String exceptionText)
  {
    AlertListenerMessage m = new AlertListenerMessage();
    m.setRecipient((InternalDistributedMember)recipient);
    m.msgLevel = msgLevel;
    m.msgDate = msgDate;
    m.connectionName = connectionName;
    if (m.connectionName == null) {
      m.connectionName = "";
    }
    m.threadName = threadName;
    if (m.threadName == null) {
      m.threadName = "";
    }
    m.tid = tid;
    m.msg = msg;
    if (m.msg == null) {
      m.msg = "";
    }
    m.exceptionText = exceptionText;
    if (m.exceptionText == null) {
      m.exceptionText = "";
    }
    return m;
  }

  @Override
  public void process(DistributionManager dm){
    RemoteGfManagerAgent agent = dm.getAgent();
    if (agent != null) {
      RemoteGemFireVM mgr = agent.getMemberById(this.getSender());
      if (mgr == null) return;
      Alert alert = new RemoteAlert(mgr,
                                    msgLevel,
                                    msgDate,
                                    connectionName,
                                    threadName,
                                    tid,
                                    msg,
                                    exceptionText,
                                    getSender());
      agent.callAlertListener(alert);
    }else{
      /**
       * Its assumed that its a managing node and it has to emit any alerts
       * emitted to it.
       */
      AlertDetails alertDetail = new AlertDetails(msgLevel, msgDate,
          connectionName, threadName, tid, msg, exceptionText, getSender());
      dm.getSystem().handleResourceEvent(ResourceEvent.SYSTEM_ALERT, alertDetail);
    }

  }
  
  @Override
  public boolean sendViaUDP() {
    return true;
  }

  public int getDSFID() {
    return ALERT_LISTENER_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(msgLevel);
    DataSerializer.writeObject(msgDate, out);
    DataSerializer.writeString(connectionName, out);
    DataSerializer.writeString(threadName, out);
    out.writeLong(tid);
    DataSerializer.writeString(msg, out);
    DataSerializer.writeString(exceptionText, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.msgLevel = in.readInt();
    this.msgDate = (Date)DataSerializer.readObject(in);
    this.connectionName = DataSerializer.readString(in);
    this.threadName = DataSerializer.readString(in);
    this.tid = in.readLong();
    this.msg = DataSerializer.readString(in);
    this.exceptionText = DataSerializer.readString(in);
  }

  @Override
  public String toString() {
    return "Alert \"" + this.msg + "\" level " +
      AlertLevel.forSeverity(this.msgLevel);
  }

}
