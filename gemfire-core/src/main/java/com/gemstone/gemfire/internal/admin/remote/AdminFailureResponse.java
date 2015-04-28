/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A response to a failed request.
 */
public final class AdminFailureResponse extends AdminResponse {
  // instance variables
  Exception cause;
  
  /**
   * Returns a <code>AdminFailureResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * system config.
   */
  public static AdminFailureResponse create(DistributionManager dm, InternalDistributedMember recipient, Exception cause) {
    AdminFailureResponse m = new AdminFailureResponse();
    m.setRecipient(recipient);
    m.cause = cause;
    return m;
  }

  // instance methods
  public Exception getCause() {
    return this.cause;
  }
  
  public int getDSFID() {
    return ADMIN_FAILURE_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.cause, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.cause = (Exception)DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "AdminFailureResponse from " + this.getRecipient() + " cause=" + this.cause;
  }
}
