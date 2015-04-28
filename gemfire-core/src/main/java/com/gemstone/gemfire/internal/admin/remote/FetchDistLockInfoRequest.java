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
//import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
import java.io.*;
//import java.util.*;

public final class FetchDistLockInfoRequest extends AdminRequest {
  /**
   * Returns a <code>FetchDistLockInfoRequest</code> to be sent to the specified recipient.
   */
  public static FetchDistLockInfoRequest create() {
    FetchDistLockInfoRequest m = new FetchDistLockInfoRequest();
    return m;
  }

  public FetchDistLockInfoRequest() {
    friendlyName = LocalizedStrings.FetchDistLockInfoRequest_LIST_DISTRIBUTED_LOCKS.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override  
  protected AdminResponse createResponse(DistributionManager dm) {
    return FetchDistLockInfoResponse.create(dm, this.getSender()); 
  }

  public int getDSFID() {
    return FETCH_DIST_LOCK_INFO_REQUEST;
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
    return LocalizedStrings.FetchDistLockInfoRequest_FETCHDISTLOCKINFOREQUEST_FROM_0.toLocalizedString(this.getSender());
  }
}
