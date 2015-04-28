/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: WaitableByte.java

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
 * on single byte variables. 
 * <p>
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class WaitableByte extends SynchronizedByte {
  /** 
   * Make a new WaitableByte with the given initial value,
   * and using its own internal lock.
   **/
  public WaitableByte(byte initialValue) { 
    super(initialValue); 
  }

  /** 
   * Make a new WaitableByte with the given initial value,
   * and using the supplied lock.
   **/
  public WaitableByte(byte initialValue, Object lock) { 
    super(initialValue, lock); 
  }


  @Override // GemStoneAddition
  public byte set(byte newValue) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.set(newValue);
    }
  }

  @Override // GemStoneAddition
  public boolean commit(byte assumedValue, byte newValue) {
    synchronized (lock_) {
      boolean success = super.commit(assumedValue, newValue);
      if (success) lock_.notifyAll();
      return success;
    }
  }

  @Override // GemStoneAddition
  public byte increment() { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.increment();
    }
  }

  @Override // GemStoneAddition
  public byte decrement() { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.decrement();
    }
  }

  @Override // GemStoneAddition
  public byte add(byte amount) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.add(amount);
    }
  }

  @Override // GemStoneAddition
  public byte subtract(byte amount) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.subtract(amount);
    }
  }

  @Override // GemStoneAddition
  public byte multiply(byte factor) { 
    synchronized (lock_) {
      lock_.notifyAll();
      return super.multiply(factor);
    }
  }

  @Override // GemStoneAddition
  public byte divide(byte factor) { 
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
  public  byte complement() { 
    synchronized (lock_) {
      value_ = (byte)~value_;
      lock_.notifyAll();
      return value_;
    }
  }

  /** 
   * Set value to value &amp; b.
   * @return the new value 
   **/
  @Override // GemStoneAddition
  public  byte and(byte b) { 
    synchronized (lock_) {
      value_ = (byte)(value_ & b);
      lock_.notifyAll();
      return value_;
    }
  }

  /** 
   * Set value to value | b.
   * @return the new value 
   **/
  @Override // GemStoneAddition
  public  byte or(byte b) { 
    synchronized (lock_) {
      value_ = (byte)(value_ | b);
      lock_.notifyAll();
      return value_;
    }
  }


  /** 
   * Set value to value ^ b.
   * @return the new value 
   **/
  @Override // GemStoneAddition
  public  byte xor(byte b) { 
    synchronized (lock_) {
      value_ = (byte)(value_ ^ b);
      lock_.notifyAll();
      return value_;
    }
  }

  /**
   * Wait until value equals c, then run action if nonnull.
   * The action is run with the synchronization lock held.
   **/

  public void whenEqual(byte c, Runnable action) throws InterruptedException {
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
  public void whenNotEqual(byte c, Runnable action) throws InterruptedException {
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
  public void whenLessEqual(byte c, Runnable action) throws InterruptedException {
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
  public void whenLess(byte c, Runnable action) throws InterruptedException {
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
  public void whenGreaterEqual(byte c, Runnable action) throws InterruptedException {
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
  public void whenGreater(byte c, Runnable action) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
    synchronized (lock_) {
      while (!(value_ > c)) lock_.wait();
      if (action != null) action.run();
    }
  }

}

