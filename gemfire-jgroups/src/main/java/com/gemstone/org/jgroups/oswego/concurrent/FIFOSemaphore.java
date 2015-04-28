/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: FIFOSemaphore.java

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
 * A First-in/First-out implementation of a Semaphore.
 * Waiting requests will be satisified in
 * the order that the processing of those requests got to a certain point.
 * If this sounds vague it is meant to be. FIFO implies a
 * logical timestamping at some point in the processing of the
 * request. To simplify things we don't actually timestamp but
 * simply store things in a FIFO queue. Thus the order in which
 * requests enter the queue will be the order in which they come
 * out.  This order need not have any relationship to the order in
 * which requests were made, nor the order in which requests
 * actually return to the caller. These depend on Java thread
 * scheduling which is not guaranteed to be predictable (although
 * JVMs tend not to go out of their way to be unfair). 
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
**/

public class FIFOSemaphore extends QueuedSemaphore {
  
  /** 
   * Create a Semaphore with the given initial number of permits.
   * Using a seed of one makes the semaphore act as a mutual exclusion lock.
   * Negative seeds are also allowed, in which case no acquires will proceed
   * until the number of releases has pushed the number of permits past 0.
  **/

  public FIFOSemaphore(long initialPermits) { 
    super(new FIFOWaitQueue(), initialPermits);
  }

  /** 
   * Simple linked list queue used in FIFOSemaphore.
   * Methods are not synchronized; they depend on synch of callers
  **/

  protected static class FIFOWaitQueue extends WaitQueue {
    protected WaitNode head_ = null;
    protected WaitNode tail_ = null;

    @Override // GemStoneAddition
    protected void insert(WaitNode w) {
      if (tail_ == null) 
        head_ = tail_ = w;
      else {
        tail_.next = w;
        tail_ = w;
      }
    }

    @Override // GemStoneAddition
    protected WaitNode extract() { 
      if (head_ == null) 
        return null;
      else {
        WaitNode w = head_;
        head_ = w.next;
        if (head_ == null) tail_ = null;
        w.next = null;  
        return w;
      }
    }
  }

}
