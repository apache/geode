/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.*;

import java.io.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A message that is sent to a particular distribution manager to
 * modify its current {@link com.gemstone.gemfire.internal.Config}.
 */
public final class StoreSysCfgResponse extends AdminResponse {
  // instance variables


  // static methods
  /**
   * Returns a <code>StoreSysCfgResponse</code> that states that a
   * given set of distribution managers are known by <code>dm</code>
   * to be started.
   */
  public static StoreSysCfgResponse create(DistributionManager dm, InternalDistributedMember recipient, Config sc) {
    StoreSysCfgResponse m = new StoreSysCfgResponse();
    m.setRecipient(recipient);
    InternalDistributedSystem sys = dm.getSystem();
    Config conf = sys.getConfig();
    String[] names = conf.getAttributeNames();
    for (int i=0; i<names.length; i++) {
      if (conf.isAttributeModifiable(names[i])) {
        conf.setAttributeObject(names[i], sc.getAttributeObject(names[i]), ConfigSource.runtime());
      }
    }
      
    return m;
  }

  // instance methods
  
  public int getDSFID() {
    return STORE_SYS_CFG_RESPONSE;
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
    return "StoreSysCfgResponse from " + this.getRecipient();
  }
}
