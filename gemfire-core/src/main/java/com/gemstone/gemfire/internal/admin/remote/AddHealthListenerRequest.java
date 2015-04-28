/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.admin.GemFireHealthConfig;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular distribution manager to
 * add a health listener.
 * @since 3.5
 */
public final class AddHealthListenerRequest extends AdminRequest {
  // instance variables
  private GemFireHealthConfig cfg;

  /**
   * Returns a <code>AddHealthListenerRequest</code> to be sent to the
   * specified recipient.
   *
   * @throws NullPointerException
   *         If <code>cfg</code> is <code>null</code>
   */
  public static AddHealthListenerRequest create(GemFireHealthConfig cfg) {
    if (cfg == null) {
      throw new NullPointerException(LocalizedStrings.AddHealthListenerRequest_NULL_GEMFIREHEALTHCONFIG.toLocalizedString());
    }

    AddHealthListenerRequest m = new AddHealthListenerRequest();
    m.cfg = cfg;
    return m;
  }

  public AddHealthListenerRequest() {
    friendlyName = LocalizedStrings.AddHealthListenerRequest_ADD_HEALTH_LISTENER.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override  
  protected AdminResponse createResponse(DistributionManager dm) {
    return AddHealthListenerResponse.create(dm, this.getSender(), this.cfg);
  }

  public int getDSFID() {
    return ADD_HEALTH_LISTENER_REQUEST;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.cfg, out);
  }

  @Override  
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.cfg = (GemFireHealthConfig)DataSerializer.readObject(in);
  }

  @Override  
  public String toString() {
    return "AddHealthListenerRequest from " + this.getRecipient() + " cfg=" + this.cfg;
  }
}
