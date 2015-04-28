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

import java.io.*;

/**
 * A message that is sent to a particular distribution manager to
 * get its current information
 */
public final class FetchHostRequest extends AdminRequest {
  /**
   * Returns a <code>FetchHostRequest</code> to be sent to the specified recipient.
   */
  public static FetchHostRequest create() {
    FetchHostRequest m = new FetchHostRequest();
    return m;
  }

  public FetchHostRequest() {
    friendlyName = LocalizedStrings.FetchHostRequest_FETCH_REMOTE_HOST.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override  
  protected AdminResponse createResponse(DistributionManager dm) {
    return FetchHostResponse.create(dm, this.getSender()); 
  }

  public int getDSFID() {
    return FETCH_HOST_REQUEST;
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
    return LocalizedStrings.FetchHostRequest_FETCHHOSTREQUEST_FOR_0.toLocalizedString(this.getRecipient());
  }
}
