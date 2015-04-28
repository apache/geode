/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

//import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * A message that is sent to a particular distribution manager to
 * get its current <code>RemoteCancelStatListener</code>.
 */
public final class CancelStatListenerResponse extends AdminResponse {
  // instance variables
  
  /**
   * Returns a <code>CancelStatListenerResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * system config.
   */
  public static CancelStatListenerResponse create(DistributionManager dm,
                                                  InternalDistributedMember recipient, int listenerId) {
    CancelStatListenerResponse m = new CancelStatListenerResponse();
    m.setRecipient(recipient);
    GemFireStatSampler sampler = null;
    sampler = dm.getSystem().getStatSampler();
    if (sampler != null) {
      sampler.removeListener(listenerId);
    }
    return m;
  }

  // instance methods
  public int getDSFID() {
    return CANCEL_STAT_LISTENER_RESPONSE;
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
    return LocalizedStrings.CancelStatListenerResponse_CANCELSTATLISTENERRESPONSE_FROM_0.toLocalizedString(this.getRecipient());
  }
}
