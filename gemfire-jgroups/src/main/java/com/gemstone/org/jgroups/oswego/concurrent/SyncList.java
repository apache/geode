/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: SyncList.java

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
 * SyncLists wrap Sync-based control around java.util.Lists.
 * They support the following additional reader operations over
 * SyncCollection: hashCode, equals, get, indexOf, lastIndexOf,
 * subList. They support additional writer operations remove(int),
 * set(int), add(int), addAll(int). The corresponding listIterators
 * and are similarly extended.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 * @see SyncCollection
**/


public class SyncList extends SyncCollection implements List {

  /**
   * Create a new SyncList protecting the given collection,
   * and using the given sync to control both reader and writer methods.
   * Common, reasonable choices for the sync argument include
   * Mutex, ReentrantLock, and Semaphores initialized to 1.
   **/
  public SyncList(List list, Sync sync) {
    super (list, sync);
  }

  /**
   * Create a new SyncList protecting the given list,
   * and using the given ReadWriteLock to control reader and writer methods.
   **/
  public SyncList(List list, ReadWriteLock rwl) {
    super (list, rwl.readLock(), rwl.writeLock());
  }

  /**
   * Create a new SyncList protecting the given list,
   * and using the given pair of locks to control reader and writer methods.
   **/
  public SyncList(List list, Sync readLock, Sync writeLock) {
    super(list, readLock, writeLock);
  }


  protected List baseList() {
    return (List)c_;
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

  public Object get(int index) {
    boolean wasInterrupted = beforeRead();
    try {
      return baseList().get(index);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public int indexOf(Object o) {
    boolean wasInterrupted = beforeRead();
    try {
      return baseList().indexOf(o);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }


  public int lastIndexOf(Object o) {
    boolean wasInterrupted = beforeRead();
    try {
      return baseList().lastIndexOf(o);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }



  public List subList(int fromIndex, int toIndex) {
    boolean wasInterrupted = beforeRead();
    try {
      return new SyncList(baseList().subList(fromIndex, toIndex), rd_, wr_);
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public Object set(int index, Object o) {
    try {
      wr_.acquire();
      try {
        return baseList().set(index, o);
      }
      finally {
        wr_.release();
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new UnsupportedOperationException();
    }
  }


  public Object remove(int index) {
    try {
      wr_.acquire();
      try {
        return baseList().remove(index);
      }
      finally {
        wr_.release();
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new UnsupportedOperationException();
    }
  }

  public void add(int index, Object o) {
    try {
      wr_.acquire();
      try {
        baseList().add(index, o);
      }
      finally {
        wr_.release();
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new UnsupportedOperationException();
    }
  }

  public boolean addAll(int index, Collection coll) {
    try {
      wr_.acquire();
      try {
        return baseList().addAll(index, coll);
      }
      finally {
        wr_.release();
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new UnsupportedOperationException();
    }
  }

  public ListIterator unprotectedListIterator() {
    boolean wasInterrupted = beforeRead();
    try {
      return baseList().listIterator(); 
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public ListIterator listIterator() {
    boolean wasInterrupted = beforeRead();
    try {
      return new SyncCollectionListIterator(baseList().listIterator()); 
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public ListIterator unprotectedListIterator(int index) {
    boolean wasInterrupted = beforeRead();
    try {
      return baseList().listIterator(index); 
    }
    finally {
      afterRead(wasInterrupted);
    }
  }

  public ListIterator listIterator(int index) {
    boolean wasInterrupted = beforeRead();
    try {
      return new SyncCollectionListIterator(baseList().listIterator(index)); 
    }
    finally {
      afterRead(wasInterrupted);
    }
  }


  public class SyncCollectionListIterator extends SyncCollectionIterator implements ListIterator {

    SyncCollectionListIterator(Iterator baseIterator) {
      super(baseIterator);
    }

    protected ListIterator baseListIterator() {
      return (ListIterator)(baseIterator_);
    }

    public boolean hasPrevious() {
      boolean wasInterrupted = beforeRead();
      try {
        return baseListIterator().hasPrevious();
      }
      finally {
        afterRead(wasInterrupted);
      }
    }

    public Object previous() {
      boolean wasInterrupted = beforeRead();
      try {
        return baseListIterator().previous();
      }
      finally {
        afterRead(wasInterrupted);
      }
    }

    public int nextIndex() {
      boolean wasInterrupted = beforeRead();
      try {
        return baseListIterator().nextIndex();
      }
      finally {
        afterRead(wasInterrupted);
      }
    }


    public int previousIndex() {
      boolean wasInterrupted = beforeRead();
      try {
        return baseListIterator().previousIndex();
      }
      finally {
        afterRead(wasInterrupted);
      }
    }


    public void set(Object o) {
      try {
        wr_.acquire();
        try {
           baseListIterator().set(o);
        }
        finally {
          wr_.release();
        }
      }
      catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new UnsupportedOperationException();
      }
    }

    public void add(Object o) {
      try {
        wr_.acquire();
        try {
           baseListIterator().add(o);
        }
        finally {
          wr_.release();
        }
      }
      catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new UnsupportedOperationException();
      }
    }


  }

}


