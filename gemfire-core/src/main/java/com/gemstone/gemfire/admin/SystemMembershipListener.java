/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin;

/**
 * A listener whose callback methods are invoked when members join or
 * leave the GemFire distributed system.
 *
 * @see AdminDistributedSystem#addMembershipListener
 *
 * @author David Whitlock
 * @since 3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public interface SystemMembershipListener {

  /**
   * Invoked when a member has joined the distributed system
   */
  public void memberJoined(SystemMembershipEvent event);

  /**
   * Invoked when a member has gracefully left the distributed system.  This
   * occurs when the member took action to remove itself from the distributed
   * system.
   */
  public void memberLeft(SystemMembershipEvent event);

  /**
   * Invoked when a member has unexpectedly left the distributed
   * system.  This occurs when a member is forcibly removed from the
   * distributed system by another process, such as from
   * <a href=../distributed/DistributedSystem.html#member-timeout> failure detection</a>, or
   * <a href=../distributed/DistributedSystem.html#enable-network-partition-detection>
   * network partition detection</a> processing.
   */
  public void memberCrashed(SystemMembershipEvent event);

//   /**
//    * Invoked when a member broadcasts an informational message.
//    *
//    * @see com.gemstone.gemfire.distributed.DistributedSystem#fireInfoEvent
//    *
//    * @since 4.0
//    */
//   public void memberInfo(SystemMembershipEvent event);

}
