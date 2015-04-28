/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: SyncSet.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
   1Aug1998  dl               Create public version
*/

package com.gemstone.org.jgroups.oswego.concurrent;
import java.util.*;

/**
 * SyncSets wrap Sync-based control around java.util.Sets.
 * They support two additional reader operations than do
 * SyncCollection: hashCode and equals.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 * @see SyncCollection
**/


public class SyncSet extends SyncCollection implements Set {

  /**
   * Create a new SyncSet protecting the given collection,
   * and using the given sync to control both reader and writer methods.
   * Common, reasonable choices for the sync argument include
   * Mutex, ReentrantLock, and Semaphores initialized to 1.
   **/
  public SyncSet(Set set, Sync sync) {
    super (set, sync);
  }

  /**
   * Create a new SyncSet protecting the given set,
   * and using the given ReadWriteLock to control reader and writer methods.
   **/
  public SyncSet(Set set, ReadWriteLock rwl) {
    super (set, rwl.readLock(), rwl.writeLock());
  }

  /**
   * Create a new SyncSet protecting the given set,
   * and using the given pair of locks to control reader and writer methods.
   **/
  public SyncSet(Set set, Sync readLock, Sync writeLock) {
    super(set, readLock, writeLock);
  }

  @Override // GemStoneAddition
  public int hashCode() {
    boolean wasInterrupted = beforeRead();
    try {
      return c_.hashCode();
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  @Override // GemStoneAddition
  public boolean equals(Object o) {
    boolean wasInterrupted = beforeRead();
    try {
      return c_.equals(o);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

}


