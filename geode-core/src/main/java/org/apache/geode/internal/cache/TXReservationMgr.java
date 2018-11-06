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

package org.apache.geode.internal.cache;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.CommitConflictException;

/**
 * Used to reserve region entries, during a transaction commit, for modification by the transaction.
 *
 *
 * @since GemFire 4.0
 *
 */
public class TXReservationMgr {
  /**
   * keys are LocalRegion; values are ArrayList of Sets of held locks.
   */
  private final Map regionLocks;
  private final boolean local;

  public TXReservationMgr(boolean local) {
    Map m;
    if (local) {
      m = new IdentityHashMap();
    } else {
      m = new HashMap();
    }
    this.regionLocks = m;
    this.local = local;
  }

  public void makeReservation(IdentityArrayList localLocks) throws CommitConflictException {
    final int llSize = localLocks.size();
    final Object[] llArray = localLocks.getArrayRef();
    synchronized (this.regionLocks) {
      for (int i = 0; i < llSize; i++) {
        checkForConflict((TXRegionLockRequestImpl) llArray[i], localLocks);
      }
    }
  }

  public void releaseReservation(IdentityArrayList localLocks) {
    synchronized (this.regionLocks) {
      release(localLocks, false);
    }
  }

  private void checkForConflict(TXRegionLockRequestImpl rr, IdentityArrayList localLocks)
      throws CommitConflictException {
    Object r = getRegionObject(rr);
    Set keys = rr.getKeys();
    Object oldValue = this.regionLocks.put(r, keys);
    if (oldValue != null) {
      try {
        // we may have a conflict
        Object[] keysArray = keys.toArray();
        if (oldValue instanceof Set) {
          checkSetForConflict(rr, (Set) oldValue, keysArray, localLocks);
          IdentityArrayList newValue = new IdentityArrayList(2);
          newValue.add(oldValue);
          newValue.add(keys);
          this.regionLocks.put(r, newValue);
        } else {
          IdentityArrayList al = (IdentityArrayList) oldValue;
          int alSize = al.size();
          Object[] alArray = al.getArrayRef();
          for (int i = 0; i < alSize; i++) {
            checkSetForConflict(rr, (Set) alArray[i], keysArray, localLocks);
          }
          al.add(keys);
          this.regionLocks.put(r, al); // fix for bug 36689
        }
      } catch (CommitConflictException ex) {
        // fix for bug 36689
        this.regionLocks.put(r, oldValue);
        throw ex;
      }
    }
  }

  private void checkSetForConflict(TXRegionLockRequestImpl rr, Set s, Object[] keys,
      IdentityArrayList localLocks) throws CommitConflictException {
    for (int i = 0; i < keys.length; i++) {
      if (s.contains(keys[i])) {
        release(localLocks, true);
        throw new CommitConflictException(
            String.format(
                "The key %s in region %s was being modified by another transaction locally.",
                new Object[] {keys[i], rr.getRegionFullPath()}));
      }
    }
  }

  private Object getRegionObject(TXRegionLockRequestImpl lr) {
    if (local) {
      return lr.getLocalRegion();
    } else {
      return lr.getRegionFullPath();
    }
  }

  private void release(IdentityArrayList localLocks, boolean conflictDetected) {
    final int llSize = localLocks.size();
    final Object[] llArray = localLocks.getArrayRef();
    for (int i = 0; i < llSize; i++) {
      TXRegionLockRequestImpl rr = (TXRegionLockRequestImpl) llArray[i];
      Object r = getRegionObject(rr);
      Set keys = rr.getKeys();
      Object curValue = this.regionLocks.get(r);
      boolean foundIt = false;
      if (curValue != null) {
        if (curValue == keys) {
          foundIt = true;
          this.regionLocks.remove(r);
        } else if (curValue instanceof IdentityArrayList) {
          IdentityArrayList al = (IdentityArrayList) curValue;
          int idx = al.indexOf(keys);
          if (idx != -1) {
            foundIt = true;
            al.remove(idx);
            if (al.isEmpty()) {
              this.regionLocks.remove(r);
            }
          }
        }
      }
      if (!foundIt && conflictDetected) {
        // No need to process the rest since the will not be found either
        // All we need to do is release the ones obtained before the
        // conflict was detected.
        break;
      }
    }
  }
}
