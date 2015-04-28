/*
 * ========================================================================= 
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved. 
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 * =========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.tier.MessageType;

/**
 * Matrix describes the state machine that needs to be applied to register
 * interest response processing and events comming from notification chanel
 * 
 * When register interest responses are deserialized and added to the cache, the
 * following rules are applied
 * 
 * 1) If the entry exists in the cache with a valid value, we consider it to be
 * more current (either because it made it via the update channel or because the
 * client updated it as part of put or invalidate)
 * 
 * 2) If the entry is invalid, and the RI response is valid, the valid value is
 * put into the cache
 * 
 * 3) If an entry is marked destroyed (either from the client operation or via
 * the update channel), it is not updated from the RI response. Destroyed
 * entries are gathered up after the RI response is completed and removed from
 * the system
 * 
 * @author Yogesh Mahajan
 * @since 5.1
 * 
 */
public class ServerResponseMatrix
{

  public static boolean checkForValidStateAfterRegisterInterest(
      LocalRegion region, Object key, Object serverValue)
  {
    int cacheEntryState = -1;
    RegionEntry re = region.entries.getEntry(key);
    if (re == null) {
      // nonexistent
      cacheEntryState = 0;
    }
    else {
      Token token = re.getValueAsToken();
      if (token == Token.DESTROYED) {
        // destroyed
        cacheEntryState = 3;
      }
      else if (token == Token.INVALID) {
        // invalid
        cacheEntryState = 2;
      }
      else {
        // valid
        cacheEntryState = 1;
      }
    }

    // A matrix During register interest response processing
    // 4 nonexistent, valid , invalid,destroyed
    // 2 invalid, valid
    boolean matrix[][] = { { true, true }, { false, false }, { true, true },
        { true, true } };

    int registerInterstResState = 0; // invalid
    if (serverValue != null) {
      registerInterstResState = 1; // valid
    }
    return matrix[cacheEntryState][registerInterstResState];
  }

  public static boolean checkForValidStateAfterNotification(LocalRegion region,
      Object key, int operation)
  {

    // 4 nonexistent/destroyed , valid , invalid,local-invalid ,
    // 4 create , update , invalidate, destroy
    boolean matrix[][] = { { true, true, true, true },
        { true, true, true, true }, { true, true, true, true },
        { true, true, true, true } };

    int cacheEntryState = -1;
    RegionEntry re = region.entries.getEntry(key);
    if (re == null) {
      // nonexistent or destroyed
      cacheEntryState = 0;
    }
    else {
      Token token = re.getValueAsToken();
      if (token == Token.DESTROYED || token == Token.REMOVED_PHASE1 || token == Token.REMOVED_PHASE2 || token == Token.TOMBSTONE) {
        // destroyed
        cacheEntryState = 0;
      }
      else if (token == Token.LOCAL_INVALID) {
        // local-invalid
        cacheEntryState = 3;
      }
      else if (token == Token.INVALID) {
        // invalid
        cacheEntryState = 2;
      }
      else {
        // valid
        cacheEntryState = 1;
      }
    }

    int notificationState = -1;
    switch (operation) {
    case MessageType.LOCAL_CREATE:
      notificationState = 0;
      break;
    case MessageType.LOCAL_UPDATE:
      notificationState = 1;
      break;
    case MessageType.LOCAL_INVALIDATE:
      notificationState = 2;
      break;
    case MessageType.LOCAL_DESTROY:
      notificationState = 3;
      break;
    }
    return matrix[cacheEntryState][notificationState];
  }
}
