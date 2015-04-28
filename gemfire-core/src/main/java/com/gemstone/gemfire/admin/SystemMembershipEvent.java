/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.distributed.DistributedMember;
/**
 * An event that describes the distributed member originated this event.
 * Instances of this are delivered to a {@link SystemMembershipListener} when a
 * member has joined or left the distributed system.
 *
 * @author Darrel Schneider
 * @since 3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface SystemMembershipEvent {
  /**
   * Returns the distributed member as a String.
   */
  public String getMemberId();

  /**
   * Returns the {@link DistributedMember} that this event originated in.
   * @return the member that performed the operation that originated this event.
   * @since 5.0
   */
  public DistributedMember getDistributedMember();
}
