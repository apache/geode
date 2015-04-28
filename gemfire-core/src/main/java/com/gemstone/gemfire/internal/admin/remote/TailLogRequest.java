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

public final class TailLogRequest extends AdminRequest {
  public static TailLogRequest create(){
    TailLogRequest m = new TailLogRequest();
    return m;
  }

  @Override  
  public AdminResponse createResponse(DistributionManager dm){
    return TailLogResponse.create(dm, this.getSender());
  }

  public TailLogRequest() {
    friendlyName = LocalizedStrings.TailLogRequest_TAIL_SYSTEM_LOG.toLocalizedString();
  }
  
  public int getDSFID() {
    return TAIL_LOG_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public String toString(){
    return "TailLogRequest from " + this.getRecipient();
  }
}
