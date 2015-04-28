/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import java.util.Map;

import javax.management.NotificationBroadcasterSupport;

import com.gemstone.gemfire.management.LockServiceMXBean;

/**
 * Management API to manage a Lock Service MBean
 * 
 * @author rishim
 * 
 */
public class LockServiceMBean extends NotificationBroadcasterSupport implements
    LockServiceMXBean {

  private LockServiceMBeanBridge bridge;

  public LockServiceMBean(LockServiceMBeanBridge bridge) {
    this.bridge = bridge;
  }

  @Override
  public void becomeLockGrantor() {
    bridge.becomeLockGrantor();

  }

  @Override
  public String fetchGrantorMember() {
    return bridge.fetchGrantorMember();
  }

  
  @Override
  public int getMemberCount() {

    return bridge.getMemberCount();
  }

  @Override
  public String[] getMemberNames() {

    return bridge.getMemberNames();
  }

  @Override
  public String getName() {

    return bridge.getName();
  }

  @Override
  public Map<String, String> listThreadsHoldingLock() {

    return bridge.listThreadsHoldingLock();
  }

  @Override
  public boolean isDistributed() {

    return bridge.isDistributed();
  }

  @Override
  public boolean isLockGrantor() {

    return bridge.isLockGrantor();
  }

  @Override
  public String[] listHeldLocks() {

    return bridge.listHeldLocks();
  }

}
