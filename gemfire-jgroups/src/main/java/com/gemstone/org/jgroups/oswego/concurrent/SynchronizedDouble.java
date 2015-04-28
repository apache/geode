/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: SynchronizedDouble.java

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
 * A class useful for offloading synch for double instance variables.
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/
@SuppressFBWarnings(value="FE_FLOATING_POINT_EQUALITY",justification="GemFire doesn't use this class")
public class SynchronizedDouble extends SynchronizedVariable implements Comparable, Cloneable {

  protected double value_;

  /** 
   * Make a new SynchronizedDouble with the given initial value,
   * and using its own internal lock.
   **/
  public SynchronizedDouble(double initialValue) { 
    super(); 
    value_ = initialValue; 
  }

  /** 
   * Make a new SynchronizedDouble with the given initial value,
   * and using the supplied lock.
   **/
  public SynchronizedDouble(double initialValue, Object lock) { 
    super(lock); 
    value_ = initialValue; 
  }

  /** 
   * Return the current value 
   **/
  public final double get() { synchronized(lock_) { return value_; } }

  /** 
   * Set to newValue.
   * @return the old value 
   **/

  public double set(double newValue) { 
    synchronized (lock_) {
      double old = value_;
      value_ = newValue; 
      return old;
    }
  }

  /**
   * Set value to newValue only if it is currently assumedValue.
   * @return true if successful
   **/
  public boolean commit(double assumedValue, double newValue) {
    synchronized(lock_) {
      boolean success = (assumedValue == value_);
      if (success) value_ = newValue;
      return success;
    }
  }


  /** 
   * Atomically swap values with another SynchronizedDouble.
   * Uses identityHashCode to avoid deadlock when
   * two SynchronizedDoubles attempt to simultaneously swap with each other.
   * (Note: Ordering via identyHashCode is not strictly guaranteed
   * by the language specification to return unique, orderable
   * values, but in practice JVMs rely on them being unique.)
   * @return the new value 
   **/

  public double swap(SynchronizedDouble other) {
    if (other == this) return get();
    SynchronizedDouble fst = this;
    SynchronizedDouble snd = other;
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
  public double add(double amount) { 
    synchronized (lock_) {
      return value_ += amount; 
    }
  }

  /** 
   * Subtract amount from value (i.e., set value -= amount)
   * @return the new value 
   **/
  public double subtract(double amount) { 
    synchronized (lock_) {
      return value_ -= amount; 
    }
  }

  /** 
   * Multiply value by factor (i.e., set value *= factor)
   * @return the new value 
   **/
  public double multiply(double factor) { 
    synchronized (lock_) {
      return value_ *= factor; 
    }
  }

  /** 
   * Divide value by factor (i.e., set value /= factor)
   * @return the new value 
   **/
  public double divide(double factor) { 
    synchronized (lock_) {
      return value_ /= factor; 
    }
  }

  public int compareTo(double other) {
    double val = get();
    return (val < other)? -1 : (val == other)? 0 : 1;
  }

  public int compareTo(SynchronizedDouble other) {
    return compareTo(other.get());
  }

  public int compareTo(Object other) {
    return compareTo((SynchronizedDouble)other);
  }

  @Override // GemStoneAddition
  public boolean equals(Object other) {
    if (other != null &&
        other instanceof SynchronizedDouble)
      return get() == ((SynchronizedDouble)other).get();
    else
      return false;
  }

  @Override // GemStoneAddition
  public int hashCode() { // same hash as Double
    long bits = Double.doubleToLongBits(get());
    return (int)(bits ^ (bits >> 32));
  }

  @Override // GemStoneAddition
  public String toString() { return String.valueOf(get()); }

}

