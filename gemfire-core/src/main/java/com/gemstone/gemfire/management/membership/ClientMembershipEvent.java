/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.membership;

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * An event delivered to a {@link ClientMembershipListener} when this process
 * detects connection changes to ClientServers or clients.
 *
 * @author rishim
 * @since 8.0
 */
public interface ClientMembershipEvent {

  /**
   * Returns the the member that connected or disconnected.
   *
   * @see com.gemstone.gemfire.distributed.DistributedSystem#getDistributedMember
   */
  public DistributedMember getMember();

  /**
   * Returns the id of the member that connected or disconnected.
   *
   * @see com.gemstone.gemfire.distributed.DistributedMember#getId
   */
  public String getMemberId();

  /**
   * Returns true if the member is a client to a CacheServer hosted by
   * this process. Returns false if the member is a peer that this
   * process is connected to.
   */
  public boolean isClient();

}
