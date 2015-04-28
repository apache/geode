/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

//import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A message that is sent to a particular distribution manager to
 * get its current <code>RemoteAddStatListener</code>.
 */
public final class AddStatListenerResponse extends AdminResponse {
  // instance variables
  int listenerId;
  
  /**
   * Returns a <code>AddStatListenerResponse</code> that will be
   * returned to the specified recipient. The message will contains a
   * copy of the local manager's system config.
   */
  public static AddStatListenerResponse create(DistributionManager dm, InternalDistributedMember recipient, long resourceId, String statName) {
    AddStatListenerResponse m = new AddStatListenerResponse();
    m.setRecipient(recipient);
    GemFireStatSampler sampler = null;
    sampler = dm.getSystem().getStatSampler();
    if (sampler != null) {
      m.listenerId = sampler.addListener(recipient, resourceId, statName);
    }
    return m;
  }

  // instance methods
  public int getListenerId() {
    return this.listenerId;
  }
  
  public int getDSFID() {
    return ADD_STAT_LISTENER_RESPONSE;
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
    return "AddStatListenerResponse from " + this.getRecipient() + " listenerId=" + this.listenerId;
  }
}
