/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.membership;

/**
 * A listener whose callback methods are invoked when members join or leave the
 * GemFire distributed system.
 *
 * @see com.gemstone.gemfire.management.ManagementService#addMembershipListener
 *
 * @author rishim
 * @since 8.0
 */
public interface MembershipListener {

  /**
   * Invoked when a member has joined the distributed system
   */
  public void memberJoined(MembershipEvent event);

  /**
   * Invoked when a member has gracefully left the distributed system. This
   * occurs when the member took action to remove itself from the distributed
   * system.
   */
  public void memberLeft(MembershipEvent event);

  /**
   * Invoked when a member has unexpectedly left the distributed system. This
   * occurs when a member process terminates abnormally or is forcibly removed
   * from the distributed system by another process, such as from <a
   * href=../distributed/DistributedSystem.html#member-timeout> failure
   * detection</a>, or <a
   * href=../distributed/DistributedSystem.html#enable-network
   * -partition-detection> network partition detection</a> processing.
   */
  public void memberCrashed(MembershipEvent event);

}
