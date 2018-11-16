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
import java.util.Date;

import org.apache.geode.DataSerializer;
import org.apache.geode.admin.AlertLevel;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.AdminMessageType;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.management.internal.AlertDetails;

/**
 * A message that is sent to an admin member or manager to notify it of an alert.
 *
 * <p>
 * Alerts are sent from remote members to the manager via {@link AlertListenerMessage} which is
 * no-ack (asynchronous). This means you cannot log a warning in one VM and immediately verify that
 * it arrives in the manager VM. You have to use {@code Mockito#timeout(long)} or
 * {@code Awaitility}.
 */
public class AlertListenerMessage extends PooledDistributionMessage implements AdminMessageType {

  private int alertLevel;
  private Date date;
  private String connectionName;
  private String threadName;
  private long threadId;
  private String message;
  private String exceptionText;

  public static AlertListenerMessage create(DistributedMember recipient, int alertLevel, Date date,
      String connectionName, String threadName, long threadId, String message,
      String exceptionText) {
    AlertListenerMessage alertListenerMessage = new AlertListenerMessage();
    alertListenerMessage.setRecipient((InternalDistributedMember) recipient);
    alertListenerMessage.alertLevel = alertLevel;
    alertListenerMessage.date = date;
    alertListenerMessage.connectionName = connectionName;
    if (alertListenerMessage.connectionName == null) {
      alertListenerMessage.connectionName = "";
    }
    alertListenerMessage.threadName = threadName;
    if (alertListenerMessage.threadName == null) {
      alertListenerMessage.threadName = "";
    }
    alertListenerMessage.threadId = threadId;
    alertListenerMessage.message = message;
    if (alertListenerMessage.message == null) {
      alertListenerMessage.message = "";
    }
    alertListenerMessage.exceptionText = exceptionText;
    if (alertListenerMessage.exceptionText == null) {
      alertListenerMessage.exceptionText = "";
    }
    return alertListenerMessage;
  }

  @Override
  public void process(ClusterDistributionManager dm) {
    RemoteGfManagerAgent agent = dm.getAgent();
    if (agent != null) {
      RemoteGemFireVM manager = agent.getMemberById(getSender());
      if (manager == null) {
        return;
      }
      Alert alert = new RemoteAlert(manager, alertLevel, date, connectionName, threadName, threadId,
          message, exceptionText, getSender());
      agent.callAlertListener(alert);
    } else {
      /*
       * The other recipient type is a JMX Manager which needs AlertDetails so that it can send out
       * JMX notifications for the alert.
       */
      AlertDetails alertDetail = new AlertDetails(alertLevel, date, connectionName, threadName,
          threadId, message, exceptionText, getSender());
      dm.getSystem().handleResourceEvent(ResourceEvent.SYSTEM_ALERT, alertDetail);
    }
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  @Override
  public int getDSFID() {
    return ALERT_LISTENER_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(alertLevel);
    DataSerializer.writeObject(date, out);
    DataSerializer.writeString(connectionName, out);
    DataSerializer.writeString(threadName, out);
    out.writeLong(threadId);
    DataSerializer.writeString(message, out);
    DataSerializer.writeString(exceptionText, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    alertLevel = in.readInt();
    date = DataSerializer.readObject(in);
    connectionName = DataSerializer.readString(in);
    threadName = DataSerializer.readString(in);
    threadId = in.readLong();
    message = DataSerializer.readString(in);
    exceptionText = DataSerializer.readString(in);
  }

  @Override
  public String toString() {
    return "Alert \"" + message + "\" level " + AlertLevel.forSeverity(alertLevel);
  }
}
