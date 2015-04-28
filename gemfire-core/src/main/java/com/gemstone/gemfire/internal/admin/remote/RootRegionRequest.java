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
 * A message that is sent to a particular application to get its
 * root region(s). All it really needs to find out is if the app
 * has created a cache.
 */
public final class RootRegionRequest extends AdminRequest {
  // instance variables

  /**
   * Returns a <code>RootRegionRequest</code> to be sent to the specified recipient.
   */
  public static RootRegionRequest create() {
    RootRegionRequest m = new RootRegionRequest();
    return m;
  }

  public RootRegionRequest() {
    friendlyName = LocalizedStrings.RootRegionRequest_INSPECT_ROOT_CACHE_REGIONS.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override  
  protected AdminResponse createResponse(DistributionManager dm) {
    // nothing needs to be done. If we got this far then a cache must exist.
    return RootRegionResponse.create(dm, this.getSender());
  }

  public int getDSFID() {
    return ROOT_REGION_REQUEST;
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
    return "RootRegionRequest from " + getRecipient();
  }
}
