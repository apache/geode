/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: WaiterPreferenceSemaphore.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  11Jun1998  dl               Create public version
   5Aug1998  dl               replaced int counters with longs
*/


package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * An implementation of counting Semaphores that
 *  enforces enough fairness for applications that
 *  need to avoid indefinite overtaking without
 *  necessarily requiring FIFO ordered access.
 *  Empirically, very little is paid for this property
 *  unless there is a lot of contention among threads
 *  or very unfair JVM scheduling.
 *  The acquire method waits even if there are permits
 *  available but have not yet been claimed by threads that have
 *  been notified but not yet resumed. This makes the semaphore
 *  almost as fair as the underlying Java primitives allow. 
 *  So, if synch lock entry and notify are both fair
 *  so is the semaphore -- almost:  Rewaits stemming
 *  from timeouts in attempt, along with potentials for
 *  interrupted threads to be notified can compromise fairness,
 *  possibly allowing later-arriving threads to pass before
 *  later arriving ones. However, in no case can a newly
 *  entering thread obtain a permit if there are still others waiting.
 *  Also, signalling order need not coincide with
 *  resumption order. Later-arriving threads might get permits
 *  and continue before other resumable threads are actually resumed.
 *  However, all of these potential fairness breaches are
 *  very rare in practice unless the underlying JVM
 *  performs strictly LIFO notifications (which has, sadly enough, 
 *  been known to occur) in which case you need to use
 *  a FIFOSemaphore to maintain a reasonable approximation
 *  of fairness.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
**/


public final class WaiterPreferenceSemaphore extends Semaphore  {

  /** 
   * Create a Semaphore with the given initial number of permits.
  **/

  public WaiterPreferenceSemaphore(long initial) {  super(initial); }

  /** Number of waiting threads **/
  protected long waits_ = 0;   

  @Override // GemStoneAddition
  public void acquire() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    synchronized(this) {
      /*
        Only take if there are more permits than threads waiting
        for permits. This prevents infinite overtaking.
      */ 
      if (permits_ > waits_) { 
        --permits_;
        return;
      }
      else { 
        ++waits_;
        try { 
          for (;;) {
            wait(); 
            if (permits_ > 0) {
              --waits_;
              --permits_;
              return;
            }
          }
        }
        catch(InterruptedException ex) { 
          --waits_;
          notify();
          throw ex;
        }
      }
    }
  }

  @Override // GemStoneAddition
  public boolean attempt(long msecs) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();

    synchronized(this) {
      if (permits_ > waits_) { 
        --permits_;
        return true;
      }
      else if (msecs <= 0)   
        return false;
      else {
        ++waits_;
        
        long startTime = System.currentTimeMillis();
        long waitTime = msecs;
        
        try {
          for (;;) {
            wait(waitTime);
            if (permits_ > 0) {
              --waits_;
              --permits_;
              return true;
            }
            else { // got a time-out or false-alarm notify
              waitTime = msecs - (System.currentTimeMillis() - startTime);
              if (waitTime <= 0) {
                --waits_;
                return false;
              }
            }
          }
        }
        catch(InterruptedException ex) { 
          --waits_;
          notify();
          throw ex;
        }
      }
    }
  }

  @Override // GemStoneAddition
  public synchronized void release() {
    ++permits_;
    notify();
  }

  /** Release N permits **/
  @Override // GemStoneAddition
  public synchronized void release(long n) {
    permits_ += n;
    for (long i = 0; i < n; ++i) notify();
  }

}

