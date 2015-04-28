/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: SynchronizedFloat.java

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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A class useful for offloading synch for float instance variables.
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class SynchronizedFloat extends SynchronizedVariable implements Comparable, Cloneable {

  protected float value_;

  /** 
   * Make a new SynchronizedFloat with the given initial value,
   * and using its own internal lock.
   **/
  public SynchronizedFloat(float initialValue) { 
    super(); 
    value_ = initialValue; 
  }

  /** 
   * Make a new SynchronizedFloat with the given initial value,
   * and using the supplied lock.
   **/
  public SynchronizedFloat(float initialValue, Object lock) { 
    super(lock); 
    value_ = initialValue; 
  }

  /** 
   * Return the current value 
   **/
  public final float get() { synchronized(lock_) { return value_; } }

  /** 
   * Set to newValue.
   * @return the old value 
   **/

  public float set(float newValue) { 
    synchronized (lock_) {
      float old = value_;
      value_ = newValue; 
      return old;
    }
  }

  /**
   * Set value to newValue only if it is currently assumedValue.
   * @return true if successful
   **/
  @SuppressFBWarnings(value="FE_FLOATING_POINT_EQUALITY", justification="GemFire does not use this class")
  public boolean commit(float assumedValue, float newValue) {
    synchronized(lock_) {
      boolean success = (assumedValue == value_);
      if (success) value_ = newValue;
      return success;
    }
  }


  /** 
   * Atomically swap values with another SynchronizedFloat.
   * Uses identityHashCode to avoid deadlock when
   * two SynchronizedFloats attempt to simultaneously swap with each other.
   * (Note: Ordering via identyHashCode is not strictly guaranteed
   * by the language specification to return unique, orderable
   * values, but in practice JVMs rely on them being unique.)
   * @return the new value 
   **/

  public float swap(SynchronizedFloat other) {
    if (other == this) return get();
    SynchronizedFloat fst = this;
    SynchronizedFloat snd = other;
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
   * Add amount to value (i.e., set value += amount)
   * @return the new value 
   **/
  public float add(float amount) { 
    synchronized (lock_) {
      return value_ += amount; 
    }
  }

  /** 
   * Subtract amount from value (i.e., set value -= amount)
   * @return the new value 
   **/
  public float subtract(float amount) { 
    synchronized (lock_) {
      return value_ -= amount; 
    }
  }

  /** 
   * Multiply value by factor (i.e., set value *= factor)
   * @return the new value 
   **/
  public float multiply(float factor) { 
    synchronized (lock_) {
      return value_ *= factor; 
    }
  }

  /** 
   * Divide value by factor (i.e., set value /= factor)
   * @return the new value 
   **/
  public float divide(float factor) { 
    synchronized (lock_) {
      return value_ /= factor; 
    }
  }

  public int compareTo(float other) {
    float val = get();
    return (val < other)? -1 : (val == other)? 0 : 1;
  }

  public int compareTo(SynchronizedFloat other) {
    return compareTo(other.get());
  }

  public int compareTo(Object other) {
    return compareTo((SynchronizedFloat)other);
  }

  @Override // GemStoneAddition
  public boolean equals(Object other) {
    if (other != null &&
        other instanceof SynchronizedFloat)
      return get() == ((SynchronizedFloat)other).get();
    else
      return false;
  }

  @Override // GemStoneAddition
  public int hashCode() {
    return Float.floatToIntBits(get());
  }

  @Override // GemStoneAddition
  public String toString() { return String.valueOf(get()); }

}

