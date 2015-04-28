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

import com.gemstone.gemfire.management.DistributedLockServiceMXBean;

/**
 * It represents a distributed view of a LockService
 * 
 * @author rishim
 * 
 */
public class DistributedLockServiceMBean implements
    DistributedLockServiceMXBean {

  private DistributedLockServiceBridge bridge;

  public DistributedLockServiceMBean(DistributedLockServiceBridge bridge) {
    this.bridge = bridge;
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
  public String[] listHeldLocks() {
    return bridge.listHeldLocks();
  }

  @Override
  public Map<String, String> listThreadsHoldingLock() {
    return bridge.listThreadsHoldingLock();
  }

}
