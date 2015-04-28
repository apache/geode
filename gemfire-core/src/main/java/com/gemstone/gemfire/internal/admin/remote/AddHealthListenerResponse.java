/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

//import com.gemstone.gemfire.*;
import com.gemstone.gemfire.admin.GemFireHealthConfig;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * The response to adding a health listener.
 * @since 3.5
 */
public final class AddHealthListenerResponse extends AdminResponse {
  // instance variables
  int listenerId;
  
  /**
   * Returns a <code>AddHealthListenerResponse</code> that will be returned to the
   * specified recipient.
   */
  public static AddHealthListenerResponse create(DistributionManager dm, InternalDistributedMember recipient, GemFireHealthConfig cfg) {
    AddHealthListenerResponse m = new AddHealthListenerResponse();
    m.setRecipient(recipient);
    dm.createHealthMonitor(recipient, cfg);
    m.listenerId = dm.getHealthMonitor(recipient).getId();
    return m;
  }

  // instance methods
  public int getHealthListenerId() {
    return this.listenerId;
  }
  
  public int getDSFID() {
    return ADD_HEALTH_LISTENER_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.listenerId);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.listenerId = in.readInt();
  }

  @Override
  public String toString() {
    return "AddHealthListenerResponse from " + this.getRecipient() + " listenerId=" + this.listenerId;
  }
}
