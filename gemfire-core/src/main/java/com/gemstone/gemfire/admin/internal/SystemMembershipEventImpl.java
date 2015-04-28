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

/**
 * An event delivered to a {@link SystemMembershipListener} when a
 * member has joined or left the distributed system.
 *
 * @author Darrel Schneider
 * @since 5.0
 */
public class SystemMembershipEventImpl implements SystemMembershipEvent {

  /** The id of the member that generated this event */
  private DistributedMember id;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>SystemMembershipEvent</code> for the member
   * with the given id.
   */
  protected SystemMembershipEventImpl(DistributedMember id) {
    this.id = id;
  }

  /////////////////////  Instance Methods  /////////////////////

  public String getMemberId() {
    return this.id.toString();
  }
  
  public DistributedMember getDistributedMember() {
    return this.id;
  }

//   /**
//    * Returns the user specified callback object associated with this
//    * membership event.  Note that the callback argument is always
//    * <code>null</code> for the event delivered to the {@link
//    * SystemMembershipListener#memberCrashed} method.
//    *
//    * @since 4.0
//    */
//   public Object getCallbackArgument() {
//     throw new UnsupportedOperationException("Not implemented yet");
//   }

  @Override
  public String toString() {
    return "Member " + this.getMemberId();
  }

}
