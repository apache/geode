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
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular distribution manager to
 * modify its current {@link com.gemstone.gemfire.internal.Config}.
 */
public final class StoreSysCfgRequest extends AdminRequest {
  // instance variables
  Config sc;

  /**
   * Returns a <code>StoreSysCfgRequest</code> that states that a
   * given set of distribution managers are known by <code>dm</code>
   * to be started.
   */
  public static StoreSysCfgRequest create(Config sc) {
    StoreSysCfgRequest m = new StoreSysCfgRequest();
    m.sc = sc;
    return m;
  }

  public StoreSysCfgRequest() {
    friendlyName = LocalizedStrings.StoreSysCfgRequest_APPLY_CONFIGURATION_PARAMETERS.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return StoreSysCfgResponse.create(dm, this.getSender(), this.sc); 
  }

  public int getDSFID() {
    return STORE_SYS_CFG_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.sc, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.sc = (Config)DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "StoreSysCfgRequest from " + this.getRecipient() + " syscfg=" + this.sc;
  }
}
