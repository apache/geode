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
 * get its current {@link com.gemstone.gemfire.internal.Config}.
 */
public final class FetchSysCfgRequest extends AdminRequest {
  /**
   * Returns a <code>FetchSysCfgRequest</code> to be sent to the specified recipient.
   */
  public static FetchSysCfgRequest create() {
    FetchSysCfgRequest m = new FetchSysCfgRequest();
    return m;
  }

  public FetchSysCfgRequest() {
    friendlyName = LocalizedStrings.FetchSysCfgRequest_FETCH_CONFIGURATION_PARAMETERS.toLocalizedString();
  }
  
  @Override
  public boolean sendViaJGroups() {
    return true;
  }

  /**
   * Must return a proper response to this request.
   */
  @Override  
  protected AdminResponse createResponse(DistributionManager dm) {
    return FetchSysCfgResponse.create(dm, this.getSender()); 
  }

  public int getDSFID() {
    return FETCH_SYS_CFG_REQUEST;
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
    return "FetchSysCfgRequest sent to " + this.getRecipient() +
      " from " + this.getSender();
  }
}
