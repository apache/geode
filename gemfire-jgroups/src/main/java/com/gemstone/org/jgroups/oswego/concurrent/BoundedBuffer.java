/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: BoundedBuffer.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  11Jun1998  dl               Create public version
  17Jul1998  dl               Simplified by eliminating wait counts
  25aug1998  dl               added peek
   5May1999  dl               replace % with conditional (slightly faster)
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * Efficient array-based bounded buffer class.
 * Adapted from CPJ, chapter 8, which describes design.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>] <p>
 **/

public class BoundedBuffer implements BoundedChannel {

  protected final Object[]  array_;      // the elements

  protected int takePtr_ = 0;            // circular indices
  protected int putPtr_ = 0;       

  protected int usedSlots_ = 0;          // length
  protected int emptySlots_;             // capacity - length

  /**
   * Helper monitor to handle puts. 
   **/
  protected final Object putMonitor_ = new Object();

  /**
   * Create a BoundedBuffer with the given capacity.
   * @exception IllegalArgumentException if capacity less or equal to zero
   **/
  public BoundedBuffer(int capacity) throws IllegalArgumentException {
    if (capacity <= 0) throw new IllegalArgumentException();
    array_ = new Object[capacity];
    emptySlots_ = capacity;
  }

  /**
   * Create a buffer with the current default capacity
   **/

  public BoundedBuffer() { 
    this(DefaultChannelCapacity.get()); 
  }

  /** 
   * Return the number of elements in the buffer.
   * This is only a snapshot value, that may change
   * immediately after returning.
   **/
  public synchronized int size() { return usedSlots_; }

  public int capacity() { return array_.length; }

  protected void incEmptySlots() {
    synchronized(putMonitor_) {
      ++emptySlots_;
      putMonitor_.notify();
    }
  }

  protected synchronized void incUsedSlots() {
    ++usedSlots_;
    notify();
  }

  protected final void insert(Object x) { // mechanics of put
    --emptySlots_;
    array_[putPtr_] = x;
    if (++putPtr_ >= array_.length) putPtr_ = 0;
  }

  protected final Object extract() { // mechanics of take
    --usedSlots_;
    Object old = array_[takePtr_];
    array_[takePtr_] = null;
    if (++takePtr_ >= array_.length) takePtr_ = 0;
    return old;
  }

  public Object peek() {
    synchronized(this) {
      if (usedSlots_ > 0)
        return array_[takePtr_];
      else
        return null;
    }
  }


  public void put(Object x) throws InterruptedException {
    if (x == null) throw new IllegalArgumentException();
    if (Thread.interrupted()) throw new InterruptedException();

    synchronized(putMonitor_) {
      while (emptySlots_ <= 0) {
	try { putMonitor_.wait(); }
        catch (InterruptedException ex) {
          putMonitor_.notify();
          throw ex;
        }
      }
      insert(x);
    }
    incUsedSlots();
  }

  public boolean offer(Object x, long msecs) throws InterruptedException {
    if (x == null) throw new IllegalArgumentException();
    if (Thread.interrupted()) throw new InterruptedException();

    synchronized(putMonitor_) {
      long start = (msecs <= 0)? 0 : System.currentTimeMillis();
      long waitTime = msecs;
      while (emptySlots_ <= 0) {
        if (waitTime <= 0) return false;
	try { putMonitor_.wait(waitTime); }
        catch (InterruptedException ex) {
          putMonitor_.notify();
          throw ex;
        }
        waitTime = msecs - (System.currentTimeMillis() - start);
      }
      insert(x);
    }
    incUsedSlots();
    return true;
  }



  public  Object take() throws InterruptedException { 
    if (Thread.interrupted()) throw new InterruptedException();
    Object old = null; 
    synchronized(this) { 
      while (usedSlots_ <= 0) {
        try { wait(); }
        catch (InterruptedException ex) {
          notify();
          throw ex; 
        }
      }
      old = extract();
    }
    incEmptySlots();
    return old;
  }

  public  Object poll(long msecs) throws InterruptedException { 
    if (Thread.interrupted()) throw new InterruptedException();
    Object old = null; 
    synchronized(this) { 
      long start = (msecs <= 0)? 0 : System.currentTimeMillis();
      long waitTime = msecs;
      
      while (usedSlots_ <= 0) {
        if (waitTime <= 0) return null;
        try { wait(waitTime); }
        catch (InterruptedException ex) {
          notify();
          throw ex; 
        }
        waitTime = msecs - (System.currentTimeMillis() - start);

      }
      old = extract();
    }
    incEmptySlots();
    return old;
  }

}


