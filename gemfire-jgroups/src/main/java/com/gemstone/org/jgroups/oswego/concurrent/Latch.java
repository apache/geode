/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: Latch.java

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
 * A latch is a boolean condition that is set at most once, ever.
 * Once a single release is issued, all acquires will pass.
 * <p>
 * <b>Sample usage.</b> Here are a set of classes that use
 * a latch as a start signal for a group of worker threads that
 * are created and started beforehand, and then later enabled.
 * <pre>
 * class Worker implements Runnable {
 *   private final Latch startSignal;
 *   Worker(Latch l) { startSignal = l; }
 *    public void run() {
 *      startSignal.acquire();
 *      doWork();
 *   }
 *   void doWork() { ... }
 * }
 *
 * class Driver { // ...
 *   void main() {
 *     Latch go = new Latch();
 *     for (int i = 0; i < N; ++i) // make threads
 *       new Thread(new Worker(go)).start();
 *     doSomethingElse();         // don't let run yet 
 *     go.release();              // let all threads proceed
 *   } 
 * }
 *</pre>
 * [<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>] <p>
**/  

public class Latch implements Sync {
  protected boolean latched_ = false;

  /*
    This could use double-check, but doesn't.
    If the latch is being used as an indicator of
    the presence or state of an object, the user would
    not necessarily get the memory barrier that comes with synch
    that would be needed to correctly use that object. This
    would lead to errors that users would be very hard to track down. So, to
    be conservative, we always use synch.
  */

  public void acquire() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    synchronized(this) {
      while (!latched_) 
        wait(); 
    }
  }

  public boolean attempt(long msecs) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    synchronized(this) {
      if (latched_) 
        return true;
      else if (msecs <= 0) 
        return false;
      else {
        long waitTime = msecs;
        long start = System.currentTimeMillis();
        for (;;) {
          wait(waitTime);
          if (latched_) 
            return true;
          else {
            waitTime = msecs - (System.currentTimeMillis() - start);
            if (waitTime <= 0) 
              return false;
          }
        }
      }
    }
  }

  /** Enable all current and future acquires to pass **/
  public synchronized void release() {
    latched_ = true;
    notifyAll();
  }

}

