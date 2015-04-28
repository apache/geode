/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.membership;

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * An event that describes the member that originated this event.
 * Instances of this are delivered to a {@link MembershipListener} when a member
 * has joined or left the distributed system.
 *
 * @author rishim
 * @since 8.0
 */
public interface MembershipEvent {
  /**
   * Returns the distributed member as a String.
   */
  public String getMemberId();

  /**
   * Returns the {@link DistributedMember} that this event originated in.
   * 
   * @return the member that performed the operation that originated this event.
   * @since 8.0
   */
  public DistributedMember getDistributedMember();
}
