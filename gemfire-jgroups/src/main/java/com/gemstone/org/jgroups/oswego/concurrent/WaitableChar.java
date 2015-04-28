/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: WaitableChar.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  23Jun1998  dl               Create public version
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * A class useful for offloading waiting and signalling operations
 * on single char variables. 
 * <p>
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class WaitableChar extends SynchronizedChar {
  /** 
   * Make a new WaitableChar with the given initial value,
   * and using its own internal lock.
   **/
  public WaitableChar(char initialValue) { 
    super(initialValue); 
  }

  /** 
   * Make a new WaitableChar with the given initial value,
   * and using the supplied lock.
   **/
  public WaitableChar(char initialValue, Object lock) { 
    super(initialValue, lock); 
  }


  @Override // GemStoneAddition
  public char set(char newValue) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.set(newValue);
    }
  }

  @Override // GemStoneAddition
  public boolean commit(char assumedValue, char newValue) {
    synchronized (lock_) {
      boolean success = super.commit(assumedValue, newValue);
      if (success) lock_.notifyAll();
      return success;
    }
  }


  @Override // GemStoneAddition
  public char add(char amount) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.add(amount);
    }
  }

  @Override // GemStoneAddition
  public char subtract(char amount) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.subtract(amount);
    }
  }

  @Override // GemStoneAddition
  public char multiply(char factor) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.multiply(factor);
    }
  }

  @Override // GemStoneAddition
  public char divide(char factor) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.divide(factor);
    }
  }


  /**
   * Wait until value equals c, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/

  public void whenEqual(char c, Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized(lock_) {
      while (!(value_ == c)) lock_.wait();
      if (action != null) action.run();
    }
  }

  /**
   * wait until value not equal to c, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/
  public void whenNotEqual(char c, Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (!(value_ != c)) lock_.wait();
      if (action != null) action.run();
    }
  }

  /**
   * wait until value less than or equal to c, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/
  public void whenLessEqual(char c, Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (!(value_ <= c)) lock_.wait();
      if (action != null) action.run();
    }
  }

  /**
   * wait until value less than c, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/
  public void whenLess(char c, Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (!(value_ < c)) lock_.wait();
      if (action != null) action.run();
    }
  }

  /**
   * wait until value greater than or equal to c, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/
  public void whenGreaterEqual(char c, Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (!(value_ >= c)) lock_.wait();
      if (action != null) action.run();
    }
  }

  /**
   * wait until value greater than c, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/
  public void whenGreater(char c, Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (!(value_ > c)) lock_.wait();
      if (action != null) action.run();
    }
  }

}

