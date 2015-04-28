/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: Slot.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  11Jun1998  dl               Create public version
  25aug1998  dl               added peek
*/

package com.gemstone.org.jgroups.oswego.concurrent;
import java.lang.reflect.*;

/**
 * A one-slot buffer, using semaphores to control access.
 * Slots are usually more efficient and controllable than using other
 * bounded buffers implementations with capacity of 1.
 * <p>
 * Among other applications, Slots can be convenient in token-passing
 * designs: Here. the Slot holds a some object serving as a token,
 * that can be obtained
 * and returned by various threads.
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
**/

public class Slot extends SemaphoreControlledChannel {

  /**
   * Create a buffer with the given capacity, using
   * the supplied Semaphore class for semaphores.
   * @exception NoSuchMethodException If class does not have constructor 
   * that intializes permits
   * @exception SecurityException if constructor information 
   * not accessible
   * @exception InstantiationException if semaphore class is abstract
   * @exception IllegalAccessException if constructor cannot be called
   * @exception InvocationTargetException if semaphore constructor throws an
   * exception
   **/

  public Slot(Class semaphoreClass) 
   throws NoSuchMethodException, 
          SecurityException, 
          InstantiationException, 
          IllegalAccessException, 
          InvocationTargetException {
    super(1, semaphoreClass);
  }

  /** 
   * Create a new Slot using default Semaphore implementations 
   **/
  public Slot() {
    super(1);
  }

  /** The slot **/
  protected Object item_ = null;


  /** Set the item in preparation for a take **/
  @Override // GemStoneAddition
  protected synchronized void insert(Object x) { 
    item_ = x; 
  }

  /** Take item known to exist **/
  @Override // GemStoneAddition
  protected synchronized Object extract() { 
    Object x = item_;
    item_ = null;
    return x;
  }

  public synchronized Object peek() {
    return item_;
  }

}
