/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: WaitableBoolean.java

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
 * A class useful for offloading synch for boolean instance variables.
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class WaitableBoolean extends SynchronizedBoolean {

  /** Make a new WaitableBoolean with the given initial value **/
  public WaitableBoolean(boolean initialValue) { super(initialValue); }


  /** 
   * Make a new WaitableBoolean with the given initial value,
   * and using the supplied lock.
   **/
  public WaitableBoolean(boolean initialValue, Object lock) { 
    super(initialValue, lock); 
  }


  @Override // GemStoneAddition
  public boolean set(boolean newValue) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.set(newValue);
    }
  }

  @Override // GemStoneAddition
  public boolean commit(boolean assumedValue, boolean newValue) {
    synchronized (lock_) {
      boolean success = super.commit(assumedValue, newValue);
      if (success) lock_.notifyAll();
      return success;
    }
  }

  @Override // GemStoneAddition
  public  boolean complement() { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.complement();
    }
  }

  @Override // GemStoneAddition
  public  boolean and(boolean b) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.and(b);
    }
  }

  @Override // GemStoneAddition
  public  boolean or(boolean b) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.or(b);
    }
  }

  @Override // GemStoneAddition
  public  boolean xor(boolean b) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.xor(b);
    }
  }

  /**
   * Wait until value is false, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/

  public  void whenFalse(Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (value_) lock_.wait();
      if (action != null) action.run();
    }
  }

  /**
   * wait until value is true, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/
  public  void whenTrue(Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (!value_) lock_.wait();
      if (action != null) action.run();
    }
  }

  /**
   * Wait until value equals c, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/

  public  void whenEqual(boolean c, Runnable action) throws InterruptedException {
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
  public  void whenNotEqual(boolean c, Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (!(value_ != c)) lock_.wait();
      if (action != null) action.run();
    }
  }

}

