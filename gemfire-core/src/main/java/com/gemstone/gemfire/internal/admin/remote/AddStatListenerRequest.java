/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.distributed.internal.*;
//import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular distribution manager to
 * add a statistic listener.
 */
public final class AddStatListenerRequest extends AdminRequest {
  // instance variables
  private long resourceId;
  private String statName;

  /**
   * Returns a <code>AddStatListenerRequest</code> to be sent to the specified recipient.
   */
  public static AddStatListenerRequest create(StatResource observedResource,
                                              Stat observedStat) {
    AddStatListenerRequest m = new AddStatListenerRequest();
    m.resourceId = observedResource.getResourceUniqueID();
    m.statName = observedStat.getName();
    return m;
  }

  public AddStatListenerRequest() {
    friendlyName = LocalizedStrings.AddStatListenerRequest_ADD_STATISTIC_RESOURCE_LISTENER.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override  
  protected AdminResponse createResponse(DistributionManager dm) {
    return AddStatListenerResponse.create(dm, this.getSender(), this.resourceId, this.statName);
  }

  public int getDSFID() {
    return ADD_STAT_LISTENER_REQUEST;
  }

  @Override  
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeLong(this.resourceId);
    out.writeUTF(this.statName);
  }

  @Override  
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.resourceId = in.readLong();
    this.statName = in.readUTF();
  }

  @Override  
  public String toString() {
    return "AddStatListenerRequest from " + this.getRecipient() + " for " + this.resourceId + " " + this.statName;
  }
}
