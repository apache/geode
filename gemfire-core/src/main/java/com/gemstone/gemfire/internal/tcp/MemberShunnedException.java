/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.tcp;

import com.gemstone.gemfire.GemFireException;

/**
 * MemberShunnedException may be thrown to prevent ack-ing a message
 * received from a member that has been removed from membership.  It
 * is currently only thrown by JGroupMembershipManager.processMessage()
 * @author bruce
 */
public class MemberShunnedException extends GemFireException
{
  private static final long serialVersionUID = -8453126202477831557L;
  private Stub member;
  
  /**
   * constructor
   * @param member the member that was shunned
   */
  public MemberShunnedException(Stub member) {
    super("");
    this.member = member;
  }
  
  /**
   * @return the member that was shunned
   */
  public Stub getShunnedMember() {
    return this.member;
  }

}
