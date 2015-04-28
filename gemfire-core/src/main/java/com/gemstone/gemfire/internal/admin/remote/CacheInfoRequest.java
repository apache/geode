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

/**
 * A message that is sent to a particular distribution manager to
 * get information on its current cache.
 * @since 3.5
 */
public final class CacheInfoRequest extends AdminRequest {
  /**
   * Returns a <code>CacheInfoRequest</code>.
   */
  public static CacheInfoRequest create() {
    CacheInfoRequest m = new CacheInfoRequest();
    return m;
  }

  public CacheInfoRequest() {
    friendlyName = LocalizedStrings.CacheInfoRequest_FETCH_CACHE_UP_TIME.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return CacheInfoResponse.create(dm, this.getSender()); 
  }

  public int getDSFID() {
    return CACHE_INFO_REQUEST;
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
    return "CacheInfoRequest from " + this.getSender();
  }
}
