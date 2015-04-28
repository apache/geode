/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.cache.Operation;

/**
 * An event that describes an operation on a region.
 * Instances of this are delivered to a {@link SystemMemberCacheListener} when a
 * a region comes or goes.
 *
 * @author Darrel Schneider
 * @since 5.0
 */
public class SystemMemberRegionEventImpl
  extends SystemMemberCacheEventImpl
  implements SystemMemberRegionEvent
{

  /** 
   * The path of region created/destroyed 
   */
  private final String regionPath;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>SystemMemberRegionEvent</code> for the member
   * with the given id.
   */
  protected SystemMemberRegionEventImpl(DistributedMember id, Operation op, String regionPath) {
    super(id, op);
    this.regionPath = regionPath;
  }

  /////////////////////  Instance Methods  /////////////////////

  public String getRegionPath() {
    return this.regionPath;
  }

  @Override
  public String toString() {
    return super.toString() + " region=" + this.regionPath;
  }

}
