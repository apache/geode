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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.locks.DLockToken;
import org.apache.geode.management.internal.ManagementConstants;

/**
 * This class acts as a Bridge between JMX layer and GemFire layer
 *
 *
 */
public class LockServiceMBeanBridge {

  private final DLockService lockService;

  public LockServiceMBeanBridge(DLockService lockService) {
    this.lockService = lockService;

  }

  /** Operations **/
  public void becomeLockGrantor() {
    lockService.becomeLockGrantor();

  }

  /**
   *
   * @return currently held lock by this lock Service
   */

  public String[] listHeldLocks() {
    Map<Object, DLockToken> tokenMap = lockService.snapshotService();
    Iterator<Object> it = tokenMap.keySet().iterator();
    List<String> listOfLocks = new ArrayList<>();
    int j = 0;
    while (it.hasNext()) {
      Object lockObject = it.next();
      DLockToken token = tokenMap.get(lockObject);
      if (token.getUsageCount() > 0) {
        // As lock objects are of Object type, this is the best
        // info we can give
        listOfLocks.add(lockObject.toString());
      }
    }
    if (listOfLocks.size() > 0) {
      String[] retStr = new String[listOfLocks.size()];
      listOfLocks.toArray(retStr);
      return retStr;
    }

    return ManagementConstants.NO_DATA_STRING;
  }

  /** Config Data **/

  public boolean isDistributed() {
    return lockService.isDistributed();
  }

  public boolean isLockGrantor() {
    return lockService.isLockGrantor();
  }

  public String fetchGrantorMember() {
    return lockService.peekLockGrantorId() != null
        ? lockService.peekLockGrantorId().getLockGrantorMember().getId() : null;
  }

  public int getMemberCount() {
    return 0;
  }

  public String[] getMemberNames() {
    return ManagementConstants.NO_DATA_STRING;
  }

  public String getName() {
    return lockService.getName();
  }

  /**
   * Returns a list of thread which are blocked on some Object
   *
   * @return map of object Name and thread holding lock on that object
   */
  public Map<String, String> listThreadsHoldingLock() {

    Map<Object, DLockToken> tokenMap = lockService.snapshotService();
    Iterator<Object> it = tokenMap.keySet().iterator();
    Map<String, String> listOfLocks = new HashMap<>();
    int j = 0;
    while (it.hasNext()) {
      Object lockObject = it.next();
      DLockToken token = tokenMap.get(lockObject);
      if (token.getUsageCount() > 0) {
        listOfLocks.put(lockObject.toString(), token.getThreadName());
      }
    }

    return listOfLocks;

  }


}
