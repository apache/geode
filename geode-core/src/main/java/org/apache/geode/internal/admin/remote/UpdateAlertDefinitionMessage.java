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

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.internal.admin.StatAlertDefinition;
import org.apache.geode.internal.admin.StatAlertsManager;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * distribution message to register alert's definition {@link StatAlertDefinition} to member's alert
 * manager {@link StatAlertsManager}
 *
 */
public class UpdateAlertDefinitionMessage extends PooledDistributionMessage {

  public static final int ADD_ALERT_DEFINITION = 1;

  public static final int UPDATE_ALERT_DEFINITION = 2;

  public static final int REMOVE_ALERT_DEFINITION = 3;

  private StatAlertDefinition[] _alertDefinitions;

  private int _actionCode;

  /**
   * Returns a <code>FetchHostRequest</code> to be sent to the specified recipient.
   *
   * @param alertDefs an array of stat alert definitions to set
   * @param actionCode either of ADD_ALERT_DEFINITION, UPDATE_ALERT_DEFINITION,
   *        REMOVE_ALERT_DEFINITION
   */

  public static UpdateAlertDefinitionMessage create(StatAlertDefinition[] alertDefs,
      int actionCode) {

    UpdateAlertDefinitionMessage m = new UpdateAlertDefinitionMessage();
    m._alertDefinitions = alertDefs;
    m._actionCode = actionCode;

    return m;

  }

  public UpdateAlertDefinitionMessage() {

  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    out.writeByte(_actionCode);
    DataSerializer.writeObjectArray(_alertDefinitions, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    _actionCode = in.readByte();
    _alertDefinitions = (StatAlertDefinition[]) DataSerializer.readObjectArray(in);
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  @Override
  public int getDSFID() {
    return UPDATE_ALERTS_DEFN_MESSAGE;
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    StatAlertsManager.getInstance(dm).updateAlertDefinition(_alertDefinitions, _actionCode);
  }

  /**
   * @return list of stat alert definitions
   */
  public StatAlertDefinition[] getAlertDefinitions() {
    return _alertDefinitions;
  }

  /**
   *
   * @return action(ADD_ALERT_DEFINITION, UPDATE_ALERT_DEFINITION, REMOVE_ALERT_DEFINITION) to be
   *         taken on alert definitions
   *
   */
  public int getActionCode() {
    return _actionCode;
  }

  /**
   *
   * @param alertDefinitions List of stat alert definitions
   * @param actionCode Action(ADD_ALERT_DEFINITION, UPDATE_ALERT_DEFINITION,
   *        REMOVE_ALERT_DEFINITION) to be taken on alert definitions
   */
  public void updateAlertDefinition(StatAlertDefinition[] alertDefinitions, int actionCode) {
    _alertDefinitions = alertDefinitions;
    _actionCode = actionCode;
  }

  /**
   * Returns String representation of this message object.
   * <p>
   * 1. if the internalDS instance is not null & is connected, the returned string contains the
   * string as "... to [intended DistributedMember] from [sender DistributedMember]".
   * <p>
   * 2. if the internalDS instance is null or is dis-connected, the returned string is -
   * "InternalDistributedSystem instance not found, no connection with DistributedSystem."
   *
   * @return String representation of this message object.
   */
  @Override
  public String toString() {
    // instance specific for VM that executes this
    InternalDistributedSystem internalDS = InternalDistributedSystem.getAnyInstance();

    String stringInfo = "";

    if (internalDS != null && internalDS.isConnected()) {
      stringInfo = "Add/update the alert definitions" + " to " + internalDS.getDistributedMember()
          + " from " + getSender();
    } else { // when no DS instance found in current VM
      stringInfo = "InternalDistributedSystem instance not found, "
          + "no connection with DistributedSystem.";
    }
    return stringInfo;
  }
}
