/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.admin.GemFireHealth;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular agent who was registered
 * a health listener on a GemFireVM. This message is only sent when
 * the health status changes.
 * @since 3.5
 */
public final class HealthListenerMessage extends PooledDistributionMessage implements AdminMessageType {
  //instance variables
  private int listenerId;
  private GemFireHealth.Health status;

  public static HealthListenerMessage create(int listenerId,
                                             GemFireHealth.Health status){
    HealthListenerMessage m = new HealthListenerMessage();
    m.listenerId = listenerId;
    m.status = status;
    return m;
  }

  @Override  
  public void process(DistributionManager dm){
    RemoteGfManagerAgent agent = dm.getAgent();
    if (agent != null) {
      RemoteGemFireVM mgr = agent.getMemberById(this.getSender());
      if (mgr != null) {
        mgr.callHealthListeners(this.listenerId, this.status);
      }
    }
  }

  public int getDSFID() {
    return HEALTH_LISTENER_MESSAGE;
  }
  
  @Override
  public boolean sendViaJGroups() {
    return true;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.listenerId);
    DataSerializer.writeObject(this.status, out);
  }

  @Override  
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.listenerId = in.readInt();
    this.status = (GemFireHealth.Health) DataSerializer.readObject(in);
  }

  @Override  
  public String toString() {
    return LocalizedStrings.HealthListenerMessage_THE_STATUS_OF_LISTENER_0_IS_1.toLocalizedString(new Object[] {Integer.valueOf(this.listenerId), this.status});
  }
  
}
