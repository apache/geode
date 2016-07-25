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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.admin.jmx.internal.StatAlertsAggregator;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.internal.admin.StatAlert;

/**
 * Distribution message to be sent to alert aggregator
 * {@link StatAlertsAggregator} It wraps alert objects{@link StatAlert}
 * 
 * @since GemFire 5.7
 */
public class AlertsNotificationMessage extends PooledDistributionMessage {

  private StatAlert[] _alerts;

  public AlertsNotificationMessage() {
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObjectArray(this._alerts, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this._alerts = (StatAlert[])DataSerializer.readObjectArray(in);
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  public int getDSFID() {
    return ALERTS_NOTIF_MESSAGE;
  }

  @Override
  protected void process(DistributionManager dm) {
    // TODO add code to invoke process notification of agrregator
    // TODO: need to check whether it's a valid implimentation
    AdminDistributedSystemImpl ds = AdminDistributedSystemImpl
        .getConnectedInstance();

    if (ds instanceof StatAlertsAggregator) {
      StatAlertsAggregator aggregator = (StatAlertsAggregator)ds;

      RemoteGemFireVM remoteVM = dm.getAgent().getMemberById(getSender());

      aggregator.processNotifications(this._alerts, remoteVM);
    }
  }

  /**
   * @return list of alerts raised by member vm
   */
  public StatAlert[] getAlerts() {
    return _alerts;
  }

  /**
   * 
   * @param alerts
   *                List of alerts raised by member vm
   */
  public void setAlerts(StatAlert[] alerts) {
    _alerts = alerts;
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("AlertsNotification[");
    sb.append("count = " + _alerts.length);
    sb.append(" (");
    for (int i = 0; i < _alerts.length; i ++) {
      sb.append(_alerts[i].toString());
      if (i != _alerts.length - 1) {
        sb.append(", ");
      }
    } // for
    sb.append(")]");
    return sb.toString();
  }
}
