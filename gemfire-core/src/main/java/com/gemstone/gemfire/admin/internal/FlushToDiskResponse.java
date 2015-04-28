/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.admin.remote.AdminResponse;

/**
 * The response to the {@link FlushToDiskRequest}
 * 
 * @author dsmith
 *
 */
public class FlushToDiskResponse extends AdminResponse {

  public FlushToDiskResponse() {
    super();
  }

  public FlushToDiskResponse(InternalDistributedMember sender) {
    this.setRecipient(sender);
  }
  
  public int getDSFID() {
    return FLUSH_TO_DISK_RESPONSE;
  }
  
  @Override
  public String toString() {
    return getClass().getName();
  }
}
