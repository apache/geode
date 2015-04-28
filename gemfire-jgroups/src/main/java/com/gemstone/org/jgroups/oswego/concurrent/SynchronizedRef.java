/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: SynchronizedRef.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  11Jun1998  dl               Create public version
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * A simple class maintaining a single reference variable that
 * is always accessed and updated under synchronization.
 * <p>
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class SynchronizedRef extends SynchronizedVariable {
  /** The maintained reference **/
  protected Object value_;

  /** 
   * Create a SynchronizedRef initially holding the given reference 
   * and using its own internal lock.
   **/
  public SynchronizedRef(Object initialValue) { 
    super();
    value_ = initialValue; 
  }

  /** 
   * Make a new SynchronizedRef with the given initial value,
   * and using the supplied lock.
   **/
  public SynchronizedRef(Object initialValue, Object lock) { 
    super(lock); 
    value_ = initialValue; 
  }

  /** 
   * Return the current value 
   **/
  public final Object get() { synchronized(lock_) { return value_; } }

  /** 
   * Set to newValue.
   * @return the old value 
   **/

  public Object set(Object newValue) { 
    synchronized (lock_) {
      Object old = value_;
      value_ = newValue; 
      return old;
    }
  }

  /**
   * Set value to newValue only if it is currently assumedValue.
   * @return true if successful
   **/
  public boolean commit(Object assumedValue, Object newValue) {
    synchronized(lock_) {
      boolean success = (assumedValue == value_);
      if (success) value_ = newValue;
      return success;
    }
  }


  /** 
   * Atomically swap values with another SynchronizedRef.
   * Uses identityHashCode to avoid deadlock when
   * two SynchronizedRefs attempt to simultaneously swap with each other.
   * (Note: Ordering via identyHashCode is not strictly guaranteed
   * by the language specification to return unique, orderable
   * values, but in practice JVMs rely on them being unique.)
   * @return the new value 
   **/

  public Object swap(SynchronizedRef other) {
    if (other == this) return get();
    SynchronizedRef fst = this;
    SynchronizedRef snd = other;
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


}
