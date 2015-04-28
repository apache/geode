/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal.beans;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.distributed.internal.locks.DLockToken;
import com.gemstone.gemfire.management.internal.ManagementConstants;

/**
 * This class acts as a Bridge between JMX layer and GemFire layer
 * 
 * @author rishim
 * 
 */
public class LockServiceMBeanBridge {

  private DLockService lockService;

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
    List<String> listOfLocks = new ArrayList<String>();
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
    return lockService.peekLockGrantorId() != null ? lockService
        .peekLockGrantorId().getLockGrantorMember().getId() : null;
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
    Map<String, String> listOfLocks = new HashMap<String, String>();
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