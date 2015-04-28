/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: SynchronizedInt.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  19Jun1998  dl               Create public version
  15Apr2003  dl               Removed redundant "synchronized" for multiply()
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * A class useful for offloading synch for int instance variables.
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class SynchronizedInt extends SynchronizedVariable implements Comparable, Cloneable {

  protected int value_;

  /** 
   * Make a new SynchronizedInt with the given initial value,
   * and using its own internal lock.
   **/
  public SynchronizedInt(int initialValue) { 
    super(); 
    value_ = initialValue; 
  }

  /** 
   * Make a new SynchronizedInt with the given initial value,
   * and using the supplied lock.
   **/
  public SynchronizedInt(int initialValue, Object lock) { 
    super(lock); 
    value_ = initialValue; 
  }

  /** 
   * Return the current value 
   **/
  public final int get() { synchronized(lock_) { return value_; } }

  /** 
   * Set to newValue.
   * @return the old value 
   **/

  public int set(int newValue) { 
    synchronized (lock_) {
      int old = value_;
      value_ = newValue; 
      return old;
    }
  }

  /**
   * Set value to newValue only if it is currently assumedValue.
   * @return true if successful
   **/
  public boolean commit(int assumedValue, int newValue) {
    synchronized(lock_) {
      boolean success = (assumedValue == value_);
      if (success) value_ = newValue;
      return success;
    }
  }

  /** 
   * Atomically swap values with another SynchronizedInt.
   * Uses identityHashCode to avoid deadlock when
   * two SynchronizedInts attempt to simultaneously swap with each other.
   * (Note: Ordering via identyHashCode is not strictly guaranteed
   * by the language specification to return unique, orderable
   * values, but in practice JVMs rely on them being unique.)
   * @return the new value 
   **/

  public int swap(SynchronizedInt other) {
    if (other == this) return get();
    SynchronizedInt fst = this;
    SynchronizedInt snd = other;
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
  public int increment() { 
    synchronized (lock_) {
      return ++value_; 
    }
  }

  /** 
   * Decrement the value.
   * @return the new value 
   **/
  public int decrement() { 
    synchronized (lock_) {
      return --value_; 
    }
  }

  /** 
   * Add amount to value (i.e., set value += amount)
   * @return the new value 
   **/
  public int add(int amount) { 
    synchronized (lock_) {
      return value_ += amount; 
    }
  }

  /** 
   * Subtract amount from value (i.e., set value -= amount)
   * @return the new value 
   **/
  public int subtract(int amount) { 
    synchronized (lock_) {
      return value_ -= amount; 
    }
  }

  /** 
   * Multiply value by factor (i.e., set value *= factor)
   * @return the new value 
   **/
  public int multiply(int factor) { 
    synchronized (lock_) {
      return value_ *= factor; 
    }
  }

  /** 
   * Divide value by factor (i.e., set value /= factor)
   * @return the new value 
   **/
  public int divide(int factor) { 
    synchronized (lock_) {
      return value_ /= factor; 
    }
  }

  /** 
   * Set the value to the negative of its old value
   * @return the new value 
   **/
  public  int negate() { 
    synchronized (lock_) {
      value_ = -value_;
      return value_;
    }
  }

  /** 
   * Set the value to its complement
   * @return the new value 
   **/
  public  int complement() { 
    synchronized (lock_) {
      value_ = ~value_;
      return value_;
    }
  }

  /** 
   * Set value to value &amp; b.
   * @return the new value 
   **/
  public  int and(int b) { 
    synchronized (lock_) {
      value_ = value_ & b;
      return value_;
    }
  }

  /** 
   * Set value to value | b.
   * @return the new value 
   **/
  public  int or(int b) { 
    synchronized (lock_) {
      value_ = value_ | b;
      return value_;
    }
  }


  /** 
   * Set value to value ^ b.
   * @return the new value 
   **/
  public  int xor(int b) { 
    synchronized (lock_) {
      value_ = value_ ^ b;
      return value_;
    }
  }

  public int compareTo(int other) {
    int val = get();
    return (val < other)? -1 : (val == other)? 0 : 1;
  }

  public int compareTo(SynchronizedInt other) {
    return compareTo(other.get());
  }

  public int compareTo(Object other) {
    return compareTo((SynchronizedInt)other);
  }

  @Override // GemStoneAddition
  public boolean equals(Object other) {
    if (other != null &&
        other instanceof SynchronizedInt)
      return get() == ((SynchronizedInt)other).get();
    else
      return false;
  }

  @Override // GemStoneAddition
  public int hashCode() { return get(); }

  @Override // GemStoneAddition
  public String toString() { return String.valueOf(get()); }

}

