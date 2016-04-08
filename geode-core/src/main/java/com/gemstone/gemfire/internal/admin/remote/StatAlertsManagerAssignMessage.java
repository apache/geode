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
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.internal.admin.StatAlertDefinition;
import com.gemstone.gemfire.internal.admin.StatAlertsManager;

/**
 * This class represents a request object to set an alert manager for the newly
 * joined member.
 * 
 * @since 5.7
 */
public class StatAlertsManagerAssignMessage extends PooledDistributionMessage {
  // instance variables
  /* Stat Alert Definitions to be set */
  private StatAlertDefinition[] alertDefs;

  /* refresh interval to be set */
  private long refreshInterval;

  /**
   * Default constructor for reflection purposes
   */
  public StatAlertsManagerAssignMessage() {
    this.alertDefs = null;
    this.refreshInterval = -1L;
  }

  /**
   * Parameterized constructor for convenience
   * 
   * @param alertDefs
   *                Array of stat alert definitions to set
   * @param refreshInterval
   *                Refresh interval to set
   */
  public StatAlertsManagerAssignMessage(StatAlertDefinition[] alertDefs,
      long refreshInterval) {
    this.alertDefs = alertDefs;
    this.refreshInterval = refreshInterval;
  }

  /**
   * This method can be used to create a request used to assign a Stat Alerts
   * Manager for a newly joined member. Stat Alert Definitions & refresh
   * interval at that moment are set on the Stat Alerts Manager
   * 
   * @param alertDefs
   *                Array of stat alert definitions to be set
   * @param refreshInterval
   *                Refresh interval to be set
   * @return an instance of StatAlertsManagerAssignRequest
   */
  public static StatAlertsManagerAssignMessage create(
      StatAlertDefinition[] alertDefs, long refreshInterval) {
    return new StatAlertsManagerAssignMessage(alertDefs, refreshInterval);
  }

  /**
   * Executed at the receiver's end. Sets the AlertsManager to the receiver
   * member VM.
   * 
   * @param dm
   *                DistributionManager instance
   */
  @Override
  protected void process(DistributionManager dm) {
    setManager(dm);
  }

  /**
   * Sets the Alerts Manager on the receiver member VM. For the Alerts Manager,
   * alert defs & the refresh interval are set.
   * 
   * @param dm
   *                DistributionManager instance
   */
  private void setManager(DistributionManager dm) {
    StatAlertsManager manager = StatAlertsManager.getInstance(dm);
    manager.updateAlertDefinition(alertDefs,
        UpdateAlertDefinitionMessage.ADD_ALERT_DEFINITION);
    manager.setRefreshTimeInterval(refreshInterval);
  }

  /**
   * A callback used by GemFire Data Serialization mechanism to write to a
   * stream.
   * 
   * @param out
   *                DataOutput stream to write to
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeLong(refreshInterval);
    DataSerializer.writeObjectArray(alertDefs, out);
  }

  /**
   * A callback used by GemFire Data Serialization mechanism to read from a
   * stream.
   * 
   * @param in
   *                DataInput stream to read from
   */
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    refreshInterval = in.readLong();
    alertDefs = (StatAlertDefinition[])DataSerializer.readObjectArray(in);
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  public int getDSFID() {    
    return STAT_ALERTS_MGR_ASSIGN_MESSAGE;
  }

  /**
   * String representation of this object
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "StatAlertsManagerAssignRequest from " + this.getSender();
  }
}
