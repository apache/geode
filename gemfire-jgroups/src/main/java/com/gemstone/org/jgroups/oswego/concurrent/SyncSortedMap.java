/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: SyncSortedMap.java

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
 * SyncSortedMaps wrap Sync-based control around java.util.SortedMaps.
 * They support the following additional reader operations over
 * SyncMap: comparator, subMap, headMap, tailMap, firstKey, lastKey.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 * @see SyncCollection
**/


public class SyncSortedMap extends SyncMap implements SortedMap {

  /**
   * Create a new SyncSortedMap protecting the given map,
   * and using the given sync to control both reader and writer methods.
   * Common, reasonable choices for the sync argument include
   * Mutex, ReentrantLock, and Semaphores initialized to 1.
   **/
  public SyncSortedMap(SortedMap map, Sync sync) {
    this (map, sync, sync);
  }

  /**
   * Create a new SyncSortedMap protecting the given map,
   * and using the given ReadWriteLock to control reader and writer methods.
   **/
  public SyncSortedMap(SortedMap map, ReadWriteLock rwl) {
    super (map, rwl.readLock(), rwl.writeLock());
  }

  /**
   * Create a new SyncSortedMap protecting the given map,
   * and using the given pair of locks to control reader and writer methods.
   **/
  public SyncSortedMap(SortedMap map, Sync readLock, Sync writeLock) {
    super(map, readLock, writeLock);
  }


  protected SortedMap baseSortedMap() {
    return (SortedMap)c_;
  }

  public Comparator comparator() {
    boolean wasInterrupted = beforeRead();
    try {
      return baseSortedMap().comparator();
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public Object firstKey() {
    boolean wasInterrupted = beforeRead();
    try {
      return baseSortedMap().firstKey();
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public Object lastKey() {
    boolean wasInterrupted = beforeRead();
    try {
      return baseSortedMap().lastKey();
    }
    finally {
      afterRead(wasInterrupted);
    }
  }


  public SortedMap subMap(Object fromElement, Object toElement) {
    boolean wasInterrupted = beforeRead();
    try {
      return new SyncSortedMap(baseSortedMap().subMap(fromElement, toElement),
                               rd_, wr_);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public SortedMap headMap(Object toElement) {
    boolean wasInterrupted = beforeRead();
    try {
      return new SyncSortedMap(baseSortedMap().headMap(toElement),
                               rd_, wr_);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public SortedMap tailMap(Object fromElement) {
    boolean wasInterrupted = beforeRead();
    try {
      return new SyncSortedMap(baseSortedMap().tailMap(fromElement),
                               rd_, wr_);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

}


