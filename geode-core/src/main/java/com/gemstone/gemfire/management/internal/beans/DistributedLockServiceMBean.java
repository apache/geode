/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.beans;

import java.util.Map;

import com.gemstone.gemfire.management.DistributedLockServiceMXBean;

/**
 * It represents a distributed view of a LockService
 * 
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
