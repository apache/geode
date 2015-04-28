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
 * An event that describes an operation on a cache.
 * Instances of this are delivered to a {@link SystemMemberCacheListener} when a
 * a cache is created or closed.
 *
 * @author Darrel Schneider
 * @since 5.0
 */
public class SystemMemberCacheEventImpl
  extends SystemMembershipEventImpl
  implements SystemMemberCacheEvent
{

  /** The operation done by this event */
  private Operation op;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>SystemMemberCacheEvent</code> for the member
   * with the given id.
   */
  protected SystemMemberCacheEventImpl(DistributedMember id, Operation op) {
    super(id);
    this.op = op;
  }

  /////////////////////  Instance Methods  /////////////////////

  public Operation getOperation() {
    return this.op;
  }

  @Override
  public String toString() {
    return super.toString() + " op=" + this.op;
  }

}
