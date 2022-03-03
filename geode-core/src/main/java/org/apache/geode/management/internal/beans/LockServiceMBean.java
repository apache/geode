/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.beans;

import java.util.Map;

import javax.management.NotificationBroadcasterSupport;

import org.apache.geode.management.LockServiceMXBean;

/**
 * Management API to manage a Lock Service MBean
 *
 *
 */
public class LockServiceMBean extends NotificationBroadcasterSupport implements LockServiceMXBean {

  private final LockServiceMBeanBridge bridge;

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
