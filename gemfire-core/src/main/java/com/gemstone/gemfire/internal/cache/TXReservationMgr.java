/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.internal.cache;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
//import com.gemstone.gemfire.internal.cache.locks.*;
//import java.io.*;
import java.util.*;

/** Used to reserve region entries, during a transaction commit,
 * for modification by the transaction.
 *
 * @author Darrel Schneider
 * 
 * @since 4.0
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
  
  public void makeReservation(IdentityArrayList localLocks)
    throws CommitConflictException
  {
    final int llSize = localLocks.size();
    final Object[] llArray = localLocks.getArrayRef();
    synchronized (this.regionLocks) {
      for (int i=0; i < llSize; i++) {
        checkForConflict((TXRegionLockRequestImpl)llArray[i], localLocks);
      }
    }
  }
  public void releaseReservation(IdentityArrayList localLocks) {
    synchronized (this.regionLocks) {
      release(localLocks, false);
    }
  }

  private void checkForConflict(TXRegionLockRequestImpl rr,
                                IdentityArrayList localLocks)
    throws CommitConflictException
  {
    Object r = getRegionObject(rr);
    Set keys = rr.getKeys();
    Object oldValue = this.regionLocks.put(r, keys);
    if (oldValue != null) {
      try {
      // we may have a conflict
      Object[] keysArray = keys.toArray();
      if (oldValue instanceof Set) {
        checkSetForConflict(rr, (Set)oldValue, keysArray, localLocks);
        IdentityArrayList newValue = new IdentityArrayList(2);
        newValue.add(oldValue);
        newValue.add(keys);
        this.regionLocks.put(r, newValue);
      } else {
        IdentityArrayList al = (IdentityArrayList)oldValue;
        int alSize = al.size();
        Object[] alArray = al.getArrayRef();
        for (int i=0; i < alSize; i++) {
          checkSetForConflict(rr, (Set)alArray[i], keysArray, localLocks);
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
                                   IdentityArrayList localLocks)
    throws CommitConflictException
  {
    for (int i=0; i < keys.length; i++) {
      if (s.contains(keys[i])) {
        release(localLocks, true);
        throw new CommitConflictException(LocalizedStrings.TXReservationMgr_THE_KEY_0_IN_REGION_1_WAS_BEING_MODIFIED_BY_ANOTHER_TRANSACTION_LOCALLY.toLocalizedString(new Object[] {keys[i], rr.getRegionFullPath()}));
      }
    }
  }
  private final Object getRegionObject(TXRegionLockRequestImpl lr) {
    if(local) {
      return lr.getLocalRegion();
    } else {
      return lr.getRegionFullPath();
    }
  }
  
  private void release(IdentityArrayList localLocks, boolean conflictDetected) {
    final int llSize = localLocks.size();
    final Object[] llArray = localLocks.getArrayRef();
    for (int i=0; i < llSize; i++) {
      TXRegionLockRequestImpl rr = (TXRegionLockRequestImpl)llArray[i];
      Object r = getRegionObject(rr);
      Set keys = rr.getKeys();
      Object curValue = this.regionLocks.get(r);
      boolean foundIt = false;
      if (curValue != null) {
        if (curValue == keys) {
          foundIt = true;
          this.regionLocks.remove(r);
        } else if (curValue instanceof IdentityArrayList) {
          IdentityArrayList al = (IdentityArrayList)curValue;
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
