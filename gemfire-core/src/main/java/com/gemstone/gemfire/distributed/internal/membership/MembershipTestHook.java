/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.distributed.internal.membership;

/**
 * Test hook for hydra test development
 * 
 * @author bruce
 *
 */
public interface MembershipTestHook
{
  /**
   * test hook invoked prior to shutting down distributed system
   */
  public void beforeMembershipFailure(String reason, Throwable cause);
  
  /**
   * test hook invoked after shutting down distributed system
   */
  public void afterMembershipFailure(String reason, Throwable cause);

}
