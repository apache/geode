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
package org.apache.geode.internal.cache.tier.sockets;

import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.tier.MessageType;

/**
 * Matrix describes the state machine that needs to be applied to register interest response
 * processing and events comming from notification chanel
 *
 * When register interest responses are deserialized and added to the cache, the following rules are
 * applied
 *
 * 1) If the entry exists in the cache with a valid value, we consider it to be more current (either
 * because it made it via the update channel or because the client updated it as part of put or
 * invalidate)
 *
 * 2) If the entry is invalid, and the RI response is valid, the valid value is put into the cache
 *
 * 3) If an entry is marked destroyed (either from the client operation or via the update channel),
 * it is not updated from the RI response. Destroyed entries are gathered up after the RI response
 * is completed and removed from the system
 *
 * @since GemFire 5.1
 *
 */
public class ServerResponseMatrix {

  public static boolean checkForValidStateAfterRegisterInterest(LocalRegion region, Object key,
      Object serverValue) {
    int cacheEntryState = -1;
    RegionEntry re = region.entries.getEntry(key);
    if (re == null) {
      // nonexistent
      cacheEntryState = 0;
    } else {
      Token token = re.getValueAsToken();
      if (token == Token.DESTROYED) {
        // destroyed
        cacheEntryState = 3;
      } else if (token == Token.INVALID) {
        // invalid
        cacheEntryState = 2;
      } else {
        // valid
        cacheEntryState = 1;
      }
    }

    // A matrix During register interest response processing
    // 4 nonexistent, valid , invalid,destroyed
    // 2 invalid, valid
    boolean[][] matrix = {{true, true}, {false, false}, {true, true}, {true, true}};

    int registerInterstResState = 0; // invalid
    if (serverValue != null) {
      registerInterstResState = 1; // valid
    }
    return matrix[cacheEntryState][registerInterstResState];
  }

  public static boolean checkForValidStateAfterNotification(LocalRegion region, Object key,
      int operation) {

    // 4 nonexistent/destroyed , valid , invalid,local-invalid ,
    // 4 create , update , invalidate, destroy
    boolean[][] matrix = {{true, true, true, true}, {true, true, true, true},
        {true, true, true, true}, {true, true, true, true}};

    int cacheEntryState = -1;
    RegionEntry re = region.entries.getEntry(key);
    if (re == null) {
      // nonexistent or destroyed
      cacheEntryState = 0;
    } else {
      Token token = re.getValueAsToken();
      if (token == Token.DESTROYED || token == Token.REMOVED_PHASE1 || token == Token.REMOVED_PHASE2
          || token == Token.TOMBSTONE) {
        // destroyed
        cacheEntryState = 0;
      } else if (token == Token.LOCAL_INVALID) {
        // local-invalid
        cacheEntryState = 3;
      } else if (token == Token.INVALID) {
        // invalid
        cacheEntryState = 2;
      } else {
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
