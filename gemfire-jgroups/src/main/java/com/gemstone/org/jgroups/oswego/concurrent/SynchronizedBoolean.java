/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: SynchronizedBoolean.java

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

public class SynchronizedBoolean  extends SynchronizedVariable implements Comparable, Cloneable {
  protected boolean value_;

  /** 
   * Make a new SynchronizedBoolean with the given initial value,
   * and using its own internal lock.
   **/
  public SynchronizedBoolean(boolean initialValue) { 
    super(); 
    value_ = initialValue; 
  }

  /** 
   * Make a new SynchronizedBoolean with the given initial value,
   * and using the supplied lock.
   **/
  public SynchronizedBoolean(boolean initialValue, Object lock) { 
    super(lock); 
    value_ = initialValue; 
  }

  /** 
   * Return the current value 
   **/
  public final boolean get() { synchronized(lock_) { return value_; } }

  /** 
   * Set to newValue.
   * @return the old value 
   **/

  public boolean set(boolean newValue) { 
    synchronized (lock_) {
      boolean old = value_;
      value_ = newValue; 
      return old;
    }
  }

  /**
   * Set value to newValue only if it is currently assumedValue.
   * @return true if successful
   **/
  public boolean commit(boolean assumedValue, boolean newValue) {
    synchronized(lock_) {
      boolean success = (assumedValue == value_);
      if (success) value_ = newValue;
      return success;
    }
  }

  /** 
   * Atomically swap values with another SynchronizedBoolean.
   * Uses identityHashCode to avoid deadlock when
   * two SynchronizedBooleans attempt to simultaneously swap with each other.
   * (Note: Ordering via identyHashCode is not strictly guaranteed
   * by the language specification to return unique, orderable
   * values, but in practice JVMs rely on them being unique.)
   * @return the new value 
   **/

  public boolean swap(SynchronizedBoolean other) {
    if (other == this) return get();
    SynchronizedBoolean fst = this;
    SynchronizedBoolean snd = other;
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
   * Set the value to its complement
   * @return the new value 
   **/
  public  boolean complement() { 
    synchronized (lock_) {
      value_ = !value_;
      return value_;
    }
  }

  /** 
   * Set value to value &amp; b.
   * @return the new value 
   **/
  public  boolean and(boolean b) { 
    synchronized (lock_) {
      value_ = value_ & b;
      return value_;
    }
  }

  /** 
   * Set value to value | b.
   * @return the new value 
   **/
  public  boolean or(boolean b) { 
    synchronized (lock_) {
      value_ = value_ | b;
      return value_;
    }
  }


  /** 
   * Set value to value ^ b.
   * @return the new value 
   **/
  public  boolean xor(boolean b) { 
    synchronized (lock_) {
      value_ = value_ ^ b;
      return value_;
    }
  }


  public int compareTo(boolean other) {
    boolean val = get();
    return  (val == other)? 0 : (val)? 1 : -1;
  }

  public int compareTo(SynchronizedBoolean other) {
    return compareTo(other.get());
  }

  public int compareTo(Object other) {
    return compareTo((SynchronizedBoolean)other);
  }
  

  @Override // GemStoneAddition
  public boolean equals(Object other) {
    if (other != null &&
        other instanceof SynchronizedBoolean)
      return get() == ((SynchronizedBoolean)other).get();
    else
      return false;
  }

  @Override // GemStoneAddition
  public int hashCode() { 
    boolean b = get(); 
    return (b)? 3412688 :  8319343; // entirely arbitrary
  }

  @Override // GemStoneAddition
  public String toString() { return String.valueOf(get()); }

}

