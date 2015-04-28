/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
   
package com.gemstone.gemfire.internal.admin.remote;

//import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Responds to {@link RegionSizeRequest}.
 */
public final class RegionSizeResponse extends AdminResponse implements Cancellable {
  // instance variables
  private int entryCount;
  private int subregionCount;
  private transient boolean cancelled;

  /**
   * Returns a <code>RegionSizeResponse</code> that will be returned to the
   * specified recipient. 
   */
  public static RegionSizeResponse create(DistributionManager dm, InternalDistributedMember recipient) {
    RegionSizeResponse m = new RegionSizeResponse();
    m.setRecipient(recipient);
    return m;
  }

  public void calcSize(Region r) {
    if (cancelled) { return; }

    Set nameSet = r.keys();
    if (cancelled) { return; }
    this.entryCount = nameSet.size();
    Set subRegions = r.subregions(false);
    if (cancelled) { return; }
    this.subregionCount = subRegions.size();
  }

  public synchronized void cancel() {
    cancelled = true;
  }

  // instance methods
  public int getEntryCount() {
    return this.entryCount;
  }

  public int getSubregionCount() {
    return this.subregionCount;
  }
  
  public int getDSFID() {
    return REGION_SIZE_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(entryCount);
    out.writeInt(subregionCount);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.entryCount = in.readInt();
    this.subregionCount = in.readInt();
  }

  @Override
  public String toString() {
    return "RegionSizeResponse from " + this.getRecipient();
  }
}
