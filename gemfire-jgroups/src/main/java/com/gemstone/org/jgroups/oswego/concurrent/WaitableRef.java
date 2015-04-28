/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: WaitableRef.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  19Jun1998  dl               Create public version
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * A class useful for offloading synch for Object reference instance variables.
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class WaitableRef extends SynchronizedRef {

  /** 
   * Create a WaitableRef initially holding the given reference 
   * and using its own internal lock.
   **/
  public WaitableRef(Object initialValue) { 
    super(initialValue);
  }

  /** 
   * Make a new WaitableRef with the given initial value,
   * and using the supplied lock.
   **/
  public WaitableRef(Object initialValue, Object lock) { 
    super(initialValue, lock); 
  }

  @Override // GemStoneAddition
  public Object set(Object newValue) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.set(newValue);
    }
  }

  @Override // GemStoneAddition
  public boolean commit(Object assumedValue, Object newValue) {
    synchronized (lock_) {
      boolean success = super.commit(assumedValue, newValue);
      if (success) lock_.notifyAll();
      return success;
    }
  }

  /**
   * Wait until value is null, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/

  public void whenNull(Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (value_ != null) lock_.wait();
      if (action != null) action.run();
    }
  }

  /**
   * wait until value is nonnull, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/
  public void whenNotNull(Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (value_ == null) lock_.wait();
      if (action != null) action.run();
    }
  }

  /**
   * Wait until value equals c, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/

  public void whenEqual(Object c, Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (!(value_ == c)) lock_.wait();
      if (action != null) action.run();
    }
  }

  /**
   * wait until value not equal to c, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/
  public void whenNotEqual(Object c, Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (!(value_ != c)) lock_.wait();
      if (action != null) action.run();
    }
  }

}

