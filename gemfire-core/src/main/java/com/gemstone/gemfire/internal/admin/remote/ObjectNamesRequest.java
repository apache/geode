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
 * A message that is sent to a particular app vm to request all the subregions
 * of a given parent region.
 */
public final class ObjectNamesRequest extends RegionAdminRequest implements Cancellable {
  // instance variables
  private transient boolean cancelled;
  private transient ObjectNamesResponse resp;

  /**
   * Returns a <code>ObjectNamesRequest</code> to be sent to the specified recipient.
   */
  public static ObjectNamesRequest create() {
    ObjectNamesRequest m = new ObjectNamesRequest();
    return m;
  }

  public ObjectNamesRequest() {
    friendlyName = LocalizedStrings.ObjectNamesRequest_LIST_CACHED_OBJECTS.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    CancellationRegistry.getInstance().registerMessage(this);
    resp = ObjectNamesResponse.create(dm, this.getSender());
    if (cancelled) { return null; }
    resp.buildNames(this.getRegion(dm.getSystem()));
    if (cancelled) { return null; }
    CancellationRegistry.getInstance().deregisterMessage(this);
    return resp;
  }

  public synchronized void cancel() {
    cancelled = true;
    if (resp != null) {
      resp.cancel();
    }
  }

  public int getDSFID() {
    return OBJECT_NAMES_REQUEST;
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
    return "ObjectNamesRequest from " + getRecipient() + " region=" + getRegionName();
  }
}
