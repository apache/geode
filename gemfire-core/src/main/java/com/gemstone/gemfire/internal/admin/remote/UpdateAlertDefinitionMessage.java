/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.PooledDistributionMessage;
import com.gemstone.gemfire.internal.admin.StatAlertDefinition;
import com.gemstone.gemfire.internal.admin.StatAlertsManager;

/**
 * distribution message to register alert's definition
 * {@link StatAlertDefinition} to member's alert manager
 * {@link StatAlertsManager}
 * 
 * @author mjha
 */
public class UpdateAlertDefinitionMessage extends PooledDistributionMessage {

  public static final int ADD_ALERT_DEFINITION = 1;

  public static final int UPDATE_ALERT_DEFINITION = 2;

  public static final int REMOVE_ALERT_DEFINITION = 3;

  private StatAlertDefinition[] _alertDefinitions;

  private int _actionCode;

  /**
   * Returns a <code>FetchHostRequest</code> to be sent to the specified
   * recipient.
   * 
   * @param alertDefs
   *                an array of stat alert definitions to set
   * @param actionCode
   *                either of ADD_ALERT_DEFINITION, UPDATE_ALERT_DEFINITION,
   *                REMOVE_ALERT_DEFINITION
   */

  public static UpdateAlertDefinitionMessage create(
      StatAlertDefinition[] alertDefs, int actionCode) {

    UpdateAlertDefinitionMessage m = new UpdateAlertDefinitionMessage();
    m._alertDefinitions = alertDefs;
    m._actionCode = actionCode;

    return m;

  }

  public UpdateAlertDefinitionMessage() {

  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeByte(_actionCode);
    DataSerializer.writeObjectArray(this._alertDefinitions, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this._actionCode = in.readByte();
    this._alertDefinitions = (StatAlertDefinition[])DataSerializer
        .readObjectArray(in);
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  public int getDSFID() {    
    return UPDATE_ALERTS_DEFN_MESSAGE;
  }

  @Override
  protected void process(DistributionManager dm) {
    StatAlertsManager.getInstance(dm).updateAlertDefinition(_alertDefinitions,
        _actionCode);
  }

  /**
   * @return list of stat alert definitions
   */
  public StatAlertDefinition[] getAlertDefinitions() {
    return _alertDefinitions;
  }

  /**
   * 
   * @return action(ADD_ALERT_DEFINITION, UPDATE_ALERT_DEFINITION,
   *         REMOVE_ALERT_DEFINITION) to be taken on alert definitions
   * 
   */
  public int getActionCode() {
    return _actionCode;
  }

  /**
   * 
   * @param alertDefinitions
   *                List of stat alert definitions
   * @param actionCode
   *                Action(ADD_ALERT_DEFINITION, UPDATE_ALERT_DEFINITION,
   *                REMOVE_ALERT_DEFINITION) to be taken on alert definitions
   */
  public void updateAlertDefinition(StatAlertDefinition[] alertDefinitions,
      int actionCode) {
    _alertDefinitions = alertDefinitions;
    _actionCode = actionCode;
  }

  /**
   * Returns String representation of this message object.<p>
   * 1. if the internalDS instance is not null & is connected, the returned 
   *    string contains the string as "... to [intended DistributedMember] from
   *    [sender DistributedMember]". <p>
   * 2. if the internalDS instance is null or is dis-connected, the returned 
   *    string is - "InternalDistributedSystem instance not found, no connection 
   *    with DistributedSystem."
   * 
   * @return String representation of this message object. 
   */
  @Override
  public String toString() {
    //instance specific for VM that executes this
    InternalDistributedSystem internalDS = 
                              InternalDistributedSystem.getAnyInstance();
    
    String stringInfo = "";
    
    if (internalDS != null && internalDS.isConnected()) {
      stringInfo = "Add/update the alert definitions" +
      		         " to " + internalDS.getDistributedMember() +  
                   " from "+this.getSender();
    } else { //when no DS instance found in current VM
      stringInfo = "InternalDistributedSystem instance not found, " +
      		         "no connection with DistributedSystem.";
    }
    return stringInfo;
  }  
}
