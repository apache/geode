/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: SyncSortedSet.java

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
 * SyncSortedSets wrap Sync-based control around java.util.SortedSets.
 * They support the following additional reader operations over
 * SyncCollection: comparator, subSet, headSet, tailSet, first, last.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 * @see SyncCollection
**/


public class SyncSortedSet extends SyncSet implements SortedSet {

  /**
   * Create a new SyncSortedSet protecting the given collection,
   * and using the given sync to control both reader and writer methods.
   * Common, reasonable choices for the sync argument include
   * Mutex, ReentrantLock, and Semaphores initialized to 1.
   **/
  public SyncSortedSet(SortedSet set, Sync sync) {
    super (set, sync);
  }

  /**
   * Create a new SyncSortedSet protecting the given set,
   * and using the given ReadWriteLock to control reader and writer methods.
   **/
  public SyncSortedSet(SortedSet set, ReadWriteLock rwl) {
    super (set, rwl.readLock(), rwl.writeLock());
  }

  /**
   * Create a new SyncSortedSet protecting the given set,
   * and using the given pair of locks to control reader and writer methods.
   **/
  public SyncSortedSet(SortedSet set, Sync readLock, Sync writeLock) {
    super(set, readLock, writeLock);
  }


  protected SortedSet baseSortedSet() {
    return (SortedSet)c_;
  }

  public Comparator comparator() {
    boolean wasInterrupted = beforeRead();
    try {
      return baseSortedSet().comparator();
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public Object first() {
    boolean wasInterrupted = beforeRead();
    try {
      return baseSortedSet().first();
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public Object last() {
    boolean wasInterrupted = beforeRead();
    try {
      return baseSortedSet().last();
    }
    finally {
      afterRead(wasInterrupted);
    }
  }


  public SortedSet subSet(Object fromElement, Object toElement) {
    boolean wasInterrupted = beforeRead();
    try {
      return new SyncSortedSet(baseSortedSet().subSet(fromElement, toElement),
                               rd_, wr_);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public SortedSet headSet(Object toElement) {
    boolean wasInterrupted = beforeRead();
    try {
      return new SyncSortedSet(baseSortedSet().headSet(toElement),
                               rd_, wr_);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public SortedSet tailSet(Object fromElement) {
    boolean wasInterrupted = beforeRead();
    try {
      return new SyncSortedSet(baseSortedSet().tailSet(fromElement),
                               rd_, wr_);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

}


