/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: QueuedSemaphore.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  11Jun1998  dl               Create public version
   5Aug1998  dl               replaced int counters with longs
  24Aug1999  dl               release(n): screen arguments
*/


package com.gemstone.org.jgroups.oswego.concurrent;

/** 
 * Abstract base class for semaphores relying on queued wait nodes.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
**/


public abstract class QueuedSemaphore extends Semaphore {
  
  protected final WaitQueue wq_;

  QueuedSemaphore(WaitQueue q, long initialPermits) { 
    super(initialPermits);  
    wq_ = q;
  }

  @Override // GemStoneAddition
  public void acquire() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    if (precheck()) return;
    WaitQueue.WaitNode w = new WaitQueue.WaitNode();
    w.doWait(this);
  }

  @Override // GemStoneAddition
  public boolean attempt(long msecs) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    if (precheck()) return true;
    if (msecs <= 0) return false;

    WaitQueue.WaitNode w = new WaitQueue.WaitNode();
    return w.doTimedWait(this, msecs);
  }

  protected synchronized boolean precheck() {
    boolean pass = (permits_ > 0);
    if (pass) --permits_;
    return pass;
  }

  protected synchronized boolean recheck(WaitQueue.WaitNode w) {
    boolean pass = (permits_ > 0);
    if (pass) --permits_;
    else       wq_.insert(w);
    return pass;
  }


  protected synchronized WaitQueue.WaitNode getSignallee() {
    WaitQueue.WaitNode w = wq_.extract();
    if (w == null) ++permits_; // if none, inc permits for new arrivals
    return w;
  }

  @Override // GemStoneAddition
  public void release() {
    for (;;) {
      WaitQueue.WaitNode w = getSignallee();
      if (w == null) return;  // no one to signal
      if (w.signal()) return; // notify if still waiting, else skip
    }
  }

  /** Release N permits **/
  @Override // GemStoneAddition
  public void release(long n) {
    if (n < 0) throw new IllegalArgumentException("Negative argument");

    for (long i = 0; i < n; ++i) release();
  }

  /** 
   * Base class for internal queue classes for semaphores, etc.
   * Relies on subclasses to actually implement queue mechanics
   **/

  protected static abstract class WaitQueue {

    protected abstract void insert(WaitNode w);// assumed not to block
    protected abstract WaitNode extract();     // should return null if empty

    protected static class WaitNode {
      boolean waiting = true;
      WaitNode next = null;

      protected synchronized boolean signal() {
        boolean signalled = waiting;
        if (signalled) {
          waiting = false;
          notify();
        }
        return signalled;
      }

      protected synchronized boolean doTimedWait(QueuedSemaphore sem, 
                                                 long msecs) 
        throws InterruptedException {
        if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
        if (sem.recheck(this) || !waiting) 
          return true;
        else if (msecs <= 0) {
          waiting = false;
          return false;
        }
        else { 
          long waitTime = msecs;
          long start = System.currentTimeMillis();

          try {
            for (;;) {
              wait(waitTime);  
              if (!waiting)   // definitely signalled
                return true;
              else { 
                waitTime = msecs - (System.currentTimeMillis() - start);
                if (waitTime <= 0) { //  timed out
                  waiting = false;
                  return false;
                }
              }
            }
          }
          catch(InterruptedException ex) {
            if (waiting) { // no notification
              waiting = false; // invalidate for the signaller
              throw ex;
            }
            else { // thread was interrupted after it was notified
              Thread.currentThread().interrupt();
              return true;
            }
          }
        }
      }

      protected synchronized void doWait(QueuedSemaphore sem) 
        throws InterruptedException {
        if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
        if (!sem.recheck(this)) {
          try {
            while (waiting) wait();  
          }
          catch(InterruptedException ex) {
            if (waiting) { // no notification
              waiting = false; // invalidate for the signaller
              throw ex;
            }
            else { // thread was interrupted after it was notified
              Thread.currentThread().interrupt();
              return;
            }
          }
        }
      }
    }

  }


}
