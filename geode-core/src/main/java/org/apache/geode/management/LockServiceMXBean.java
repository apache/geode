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
package org.apache.geode.management;

import java.util.Map;

import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

/**
 * MBean that provides access to information and management functionality for a
 * {@link DLockService}. Since any number of DLockService objects can be created by a member there
 * may be 0 or more instances of this MBean available.
 *
 * @since GemFire 7.0
 * 
 */
@ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
public interface LockServiceMXBean {

  /**
   * Returns the name of the lock service.
   */
  public String getName();

  /**
   * Returns whether this is a distributed LockService.
   *
   * @return True is this is a distributed LockService, false otherwise.
   */
  public boolean isDistributed();

  /**
   * Returns the number of members using this LockService.
   */
  public int getMemberCount();

  /**
   * Returns of the name of the member which grants the lock.
   */
  public String fetchGrantorMember();

  /**
   * Returns a list of names of the members using this LockService.
   */
  public String[] getMemberNames();

  /**
   * Returns whether this member is the granter.
   * 
   * @return True if this member is the granter, false otherwise.
   */
  public boolean isLockGrantor();


  /**
   * Requests that this member become the granter.
   */
  @ResourceOperation(resource = Resource.DATA, operation = Operation.MANAGE)
  public void becomeLockGrantor();

  /**
   * Returns a map of the names of the objects being locked on and the names of the threads holding
   * the locks.
   */
  public Map<String, String> listThreadsHoldingLock();

  /**
   * Returns a list of names of the locks held by this member's threads.
   */
  public String[] listHeldLocks();

}
