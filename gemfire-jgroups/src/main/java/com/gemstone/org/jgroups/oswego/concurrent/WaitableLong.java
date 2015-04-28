/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: WaitableLong.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  23Jun1998  dl               Create public version
  13may2004  dl               Add notifying bit ops
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * A class useful for offloading waiting and signalling operations
 * on single long variables. 
 * <p>
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class WaitableLong extends SynchronizedLong {
  /** 
   * Make a new WaitableLong with the given initial value,
   * and using its own internal lock.
   **/
  public WaitableLong(long initialValue) { 
    super(initialValue); 
  }

  /** 
   * Make a new WaitableLong with the given initial value,
   * and using the supplied lock.
   **/
  public WaitableLong(long initialValue, Object lock) { 
    super(initialValue, lock); 
  }


  @Override // GemStoneAddition
  public long set(long newValue) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.set(newValue);
    }
  }

  @Override // GemStoneAddition
  public boolean commit(long assumedValue, long newValue) {
    synchronized (lock_) {
      boolean success = super.commit(assumedValue, newValue);
      if (success) lock_.notifyAll();
      return success;
    }
  }

  @Override // GemStoneAddition
  public long increment() { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.increment();
    }
  }

  @Override // GemStoneAddition
  public long decrement() { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.decrement();
    }
  }

  @Override // GemStoneAddition
  public long add(long amount) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.add(amount);
    }
  }

  @Override // GemStoneAddition
  public long subtract(long amount) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.subtract(amount);
    }
  }

  @Override // GemStoneAddition
  public long multiply(long factor) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.multiply(factor);
    }
  }

  @Override // GemStoneAddition
  public long divide(long factor) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.divide(factor);
    }
  }

  /** 
   * Set the value to its complement
   * @return the new value 
   **/
  @Override // GemStoneAddition
  public  long complement() { 
    synchronized (lock_) {
      value_ = ~value_;
      lock_.notifyAll();
      return value_;
    }
  }

  /** 
   * Set value to value &amp; b.
   * @return the new value 
   **/
  @Override // GemStoneAddition
  public  long and(long b) { 
    synchronized (lock_) {
      value_ = value_ & b;
      lock_.notifyAll();
      return value_;
    }
  }

  /** 
   * Set value to value | b.
   * @return the new value 
   **/
  @Override // GemStoneAddition
  public  long or(long b) { 
    synchronized (lock_) {
      value_ = value_ | b;
      lock_.notifyAll();
      return value_;
    }
  }


  /** 
   * Set value to value ^ b.
   * @return the new value 
   **/
  @Override // GemStoneAddition
  public  long xor(long b) { 
    synchronized (lock_) {
      value_ = value_ ^ b;
      lock_.notifyAll();
      return value_;
    }
  }


  /**
   * Wait until value equals c, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/

  public void whenEqual(long c, Runnable action) throws InterruptedException {
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
  public void whenNotEqual(long c, Runnable action) throws InterruptedException {
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
  public void whenLessEqual(long c, Runnable action) throws InterruptedException {
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
  public void whenLess(long c, Runnable action) throws InterruptedException {
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
  public void whenGreaterEqual(long c, Runnable action) throws InterruptedException {
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
  public void whenGreater(long c, Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (!(value_ > c)) lock_.wait();
      if (action != null) action.run();
    }
  }

}

