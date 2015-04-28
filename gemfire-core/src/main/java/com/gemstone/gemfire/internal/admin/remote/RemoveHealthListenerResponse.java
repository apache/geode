/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

//import com.gemstone.gemfire.*;
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
public final class RemoveHealthListenerResponse extends AdminResponse {
  // instance variables
  
  /**
   * Returns a <code>RemoveHealthListenerResponse</code> that will be returned to the
   * specified recipient.
   */
  public static RemoveHealthListenerResponse create(DistributionManager dm, InternalDistributedMember recipient, int id) {
    RemoveHealthListenerResponse m = new RemoveHealthListenerResponse();
    m.setRecipient(recipient);
    dm.removeHealthMonitor(recipient, id);
    return m;
  }

  // instance methods
  public int getDSFID() {
    return REMOVE_HEALTH_LISTENER_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public String toString() {
    return "RemoveHealthListenerResponse from " + this.getRecipient();
  }
}
