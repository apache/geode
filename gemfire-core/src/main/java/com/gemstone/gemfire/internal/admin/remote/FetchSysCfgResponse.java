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
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A message that is sent to a particular distribution manager to
 * get its current {@link Config}.
 */
public final class FetchSysCfgResponse extends AdminResponse {
  // instance variables
  Config sc;
  
  /**
   * Returns a <code>FetchSysCfgResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * config.
   */
  public static FetchSysCfgResponse create(DistributionManager dm, InternalDistributedMember recipient) {
    FetchSysCfgResponse m = new FetchSysCfgResponse();
    m.setRecipient(recipient);
    Config conf = dm.getSystem().getConfig();  
    if (conf instanceof RuntimeDistributionConfigImpl) {
      m.sc = ((RuntimeDistributionConfigImpl)conf).takeSnapshot();
    }
    return m;
  }

  // instance methods
  public Config getConfig() {
    return this.sc;
  }
  
  public int getDSFID() {
    return FETCH_SYS_CFG_RESPONSE;
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
    return "FetchSysCfgResponse from " + this.getRecipient() + " cfg=" + this.sc;
  }
}
