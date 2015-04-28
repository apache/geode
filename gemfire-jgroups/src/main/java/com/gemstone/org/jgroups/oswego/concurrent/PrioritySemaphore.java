/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: PrioritySemaphore.java

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
 * A Semaphore that grants requests to threads with higher
 * Thread priority rather than lower priority when there is
 * contention. Ordering of requests with the same priority
 * is approximately FIFO.
 * Priorities are based on Thread.getPriority.
 * Changing the priority of an already-waiting thread does NOT 
 * change its ordering. This class also does not specially deal with priority
 * inversion --  when a new high-priority thread enters
 * while a low-priority thread is currently running, their
 * priorities are <em>not</em> artificially manipulated.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]

**/

public class PrioritySemaphore extends QueuedSemaphore {

  /** 
   * Create a Semaphore with the given initial number of permits.
   * Using a seed of one makes the semaphore act as a mutual exclusion lock.
   * Negative seeds are also allowed, in which case no acquires will proceed
   * until the number of releases has pushed the number of permits past 0.
  **/


  public PrioritySemaphore(long initialPermits) { 
    super(new PriorityWaitQueue(), initialPermits);
  }

  protected static class PriorityWaitQueue extends WaitQueue {


    /** An array of wait queues, one per priority **/
    protected final FIFOSemaphore.FIFOWaitQueue[] cells_ = 
      new FIFOSemaphore.FIFOWaitQueue[Thread.MAX_PRIORITY -
                                     Thread.MIN_PRIORITY + 1];

    /**
     * The index of the highest priority cell that may need to be signalled,
     * or -1 if none. Used to minimize array traversal.
    **/

    protected int maxIndex_ = -1;

    protected PriorityWaitQueue() { 
      for (int i = 0; i < cells_.length; ++i) 
        cells_[i] = new FIFOSemaphore.FIFOWaitQueue();
    }

    @Override // GemStoneAddition
    protected void insert(WaitNode w) {
      int idx = Thread.currentThread().getPriority() - Thread.MIN_PRIORITY;
      cells_[idx].insert(w); 
      if (idx > maxIndex_) maxIndex_ = idx;
    }

    @Override // GemStoneAddition
    protected WaitNode extract() {
      for (;;) {
        int idx = maxIndex_;
        if (idx < 0) 
          return null;
        WaitNode w = cells_[idx].extract();
        if (w != null) 
          return w;
        else
          --maxIndex_;
      }
    }
  }





}
