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
 * get its current version info.
 * @since 3.5
 */
public final class VersionInfoRequest extends AdminRequest {
  /**
   * Returns a <code>VersionInfoRequest</code>.
   */
  public static VersionInfoRequest create() {
    VersionInfoRequest m = new VersionInfoRequest();
    return m;
  }

  public VersionInfoRequest() {
    friendlyName = LocalizedStrings.VersionInfoRequest_FETCH_CURRENT_VERSION_INFORMATION.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override  
  protected AdminResponse createResponse(DistributionManager dm) {
    return VersionInfoResponse.create(dm, this.getSender()); 
  }

  public int getDSFID() {
    return VERSION_INFO_REQUEST;
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
    return "VersionInfoRequest from " + this.getSender();
  }
}
