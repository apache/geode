/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: SynchronizedLong.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  19Jun1998  dl               Create public version
  15Apr2003  dl               Removed redundant "synchronized" for multiply()
  23jan04    dl               synchronize self-swap case for swap
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * A class useful for offloading synch for long instance variables.
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class SynchronizedLong extends SynchronizedVariable implements Comparable, Cloneable {

  protected long value_;

  /** 
   * Make a new SynchronizedLong with the given initial value,
   * and using its own internal lock.
   **/
  public SynchronizedLong(long initialValue) { 
    super(); 
    value_ = initialValue; 
  }

  /** 
   * Make a new SynchronizedLong with the given initial value,
   * and using the supplied lock.
   **/
  public SynchronizedLong(long initialValue, Object lock) { 
    super(lock); 
    value_ = initialValue; 
  }

  /** 
   * Return the current value 
   **/
  public final long get() { synchronized(lock_) { return value_; } }

  /** 
   * Set to newValue.
   * @return the old value 
   **/

  public long set(long newValue) { 
    synchronized (lock_) {
      long old = value_;
      value_ = newValue; 
      return old;
    }
  }

  /**
   * Set value to newValue only if it is currently assumedValue.
   * @return true if successful
   **/
  public boolean commit(long assumedValue, long newValue) {
    synchronized(lock_) {
      boolean success = (assumedValue == value_);
      if (success) value_ = newValue;
      return success;
    }
  }

  /** 
   * Atomically swap values with another SynchronizedLong.
   * Uses identityHashCode to avoid deadlock when
   * two SynchronizedLongs attempt to simultaneously swap with each other.
   * @return the new value 
   **/

  public long swap(SynchronizedLong other) {
    if (other == this) return get();
    SynchronizedLong fst = this;
    SynchronizedLong snd = other;
    if (System.identityHashCode(fst) > System.identityHashCode(snd)) {
        fst = other;
        snd = this;
    }
    synchronized(fst.lock_) {
        synchronized(snd.lock_) {
            fst.set(snd.set(fst.get()));
            return get();
        }
    }
  }

  /** 
   * Increment the value.
   * @return the new value 
   **/
  public long increment() { 
    synchronized (lock_) {
      return ++value_; 
    }
  }

  /** 
   * Decrement the value.
   * @return the new value 
   **/
  public long decrement() { 
    synchronized (lock_) {
      return --value_; 
    }
  }

  /** 
   * Add amount to value (i.e., set value += amount)
   * @return the new value 
   **/
  public long add(long amount) { 
    synchronized (lock_) {
      return value_ += amount; 
    }
  }

  /** 
   * Subtract amount from value (i.e., set value -= amount)
   * @return the new value 
   **/
  public long subtract(long amount) { 
    synchronized (lock_) {
      return value_ -= amount; 
    }
  }

  /** 
   * Multiply value by factor (i.e., set value *= factor)
   * @return the new value 
   **/
  public long multiply(long factor) { 
    synchronized (lock_) {
      return value_ *= factor; 
    }
  }

  /** 
   * Divide value by factor (i.e., set value /= factor)
   * @return the new value 
   **/
  public long divide(long factor) { 
    synchronized (lock_) {
      return value_ /= factor; 
    }
  }

  /** 
   * Set the value to the negative of its old value
   * @return the new value 
   **/
  public  long negate() { 
    synchronized (lock_) {
      value_ = -value_;
      return value_;
    }
  }

  /** 
   * Set the value to its complement
   * @return the new value 
   **/
  public  long complement() { 
    synchronized (lock_) {
      value_ = ~value_;
      return value_;
    }
  }

  /** 
   * Set value to value &amp; b.
   * @return the new value 
   **/
  public  long and(long b) { 
    synchronized (lock_) {
      value_ = value_ & b;
      return value_;
    }
  }

  /** 
   * Set value to value | b.
   * @return the new value 
   **/
  public  long or(long b) { 
    synchronized (lock_) {
      value_ = value_ | b;
      return value_;
    }
  }


  /** 
   * Set value to value ^ b.
   * @return the new value 
   **/
  public  long xor(long b) { 
    synchronized (lock_) {
      value_ = value_ ^ b;
      return value_;
    }
  }


  public int compareTo(long other) {
    long val = get();
    return (val < other)? -1 : (val == other)? 0 : 1;
  }

  public int compareTo(SynchronizedLong other) {
    return compareTo(other.get());
  }

  public int compareTo(Object other) {
    return compareTo((SynchronizedLong)other);
  }

  @Override // GemStoneAddition
  public boolean equals(Object other) {
    if (other != null &&
        other instanceof SynchronizedLong)
      return get() == ((SynchronizedLong)other).get();
    else
      return false;
  }

  @Override // GemStoneAddition
  public int hashCode() { // same expression as java.lang.Long
    long v = get();
    return (int)(v ^ (v >> 32));
  }

  @Override // GemStoneAddition
  public String toString() { return String.valueOf(get()); }

}

