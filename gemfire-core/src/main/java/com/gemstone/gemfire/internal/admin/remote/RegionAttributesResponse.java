/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Responds to {@link RegionAttributesResponse}.
 */
public final class RegionAttributesResponse extends AdminResponse {
  // instance variables 
  private RemoteRegionAttributes attributes;

  /**
   * Returns a <code>RegionAttributesResponse</code> that will be returned to the
   * specified recipient. The message will contains a copy of the local manager's
   * system config.
   */
  public static RegionAttributesResponse create(DistributionManager dm, InternalDistributedMember recipient, Region r) {
    RegionAttributesResponse m = new RegionAttributesResponse();
    m.setRecipient(recipient);
    m.attributes = new RemoteRegionAttributes(r.getAttributes());
    return m;
  }

  // instance methods
  public RegionAttributes getRegionAttributes() {
    return this.attributes;
  }
  
  public int getDSFID() {
    return REGION_ATTRIBUTES_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.attributes, out);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.attributes = (RemoteRegionAttributes)DataSerializer.readObject(in);
  }

  @Override
  public String toString() {
    return "RegionAttributesResponse from " + this.getRecipient();
  }
}
