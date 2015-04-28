/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * @author dsmith
 *
 */
public class RevokePersistentIDResponse extends AdminResponse {
  public RevokePersistentIDResponse() {
  }
  
  public RevokePersistentIDResponse(InternalDistributedMember sender) {
    this.setRecipient(sender);
  }

  public int getDSFID() {
    return REVOKE_PERSISTENT_ID_RESPONSE;
  }
}
