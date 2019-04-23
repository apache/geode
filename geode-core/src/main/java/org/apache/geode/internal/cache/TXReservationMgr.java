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
  private final Map<Object, Map> regionLocks;
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
    Map keys = rr.getKeys();
    Map oldValue = this.regionLocks.put(r, keys);
    if (oldValue != null) {
      try {
        // we may have a conflict
        checkSetForConflict(rr, oldValue, keys, localLocks);
        HashMap<Object, Boolean> newValue =
            new HashMap<Object, Boolean>(oldValue.size() + keys.size());
        newValue.putAll(oldValue);
        newValue.putAll(keys);
        this.regionLocks.put(r, newValue);
      } catch (CommitConflictException ex) {
        // fix for bug 36689
        this.regionLocks.put(r, oldValue);
        throw ex;
      }
    }
  }

  private void checkSetForConflict(TXRegionLockRequestImpl rr, Map<Object, Boolean> oldValue,
      Map<Object, Boolean> keys, IdentityArrayList localLocks) throws CommitConflictException {
    for (Map.Entry<Object, Boolean> e : keys.entrySet()) {
      if (oldValue.containsKey(e.getKey())) {
        if (oldValue.get(e.getKey())) {
          release(localLocks, true);
          throw new CommitConflictException(
              String.format(
                  "The key %s in region %s was being modified by another transaction locally.",
                  new Object[] {e.getKey(), rr.getRegionFullPath()}));
        } else {
          oldValue.remove(e.getKey());
        }
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
      Map<Object, Boolean> keys = rr.getKeys();
      Map<Object, Boolean> curValue = this.regionLocks.get(r);
      boolean foundIt = false;
      if (curValue != null) {
        if (curValue == keys) {
          foundIt = true;
          this.regionLocks.remove(r);
        } else {
          for (Map.Entry<Object, Boolean> entry : keys.entrySet()) {
            if (curValue.remove(entry.getKey(), entry.getValue())) {
              foundIt = true;
            }
          }
          if (curValue.isEmpty()) {
            this.regionLocks.remove(r);
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
