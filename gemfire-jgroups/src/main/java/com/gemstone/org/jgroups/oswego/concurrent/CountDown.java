/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: CountDown.java

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
 * A CountDown can serve as a simple one-shot barrier. 
 * A Countdown is initialized
 * with a given count value. Each release decrements the count.
 * All acquires block until the count reaches zero. Upon reaching
 * zero all current acquires are unblocked and all 
 * subsequent acquires pass without blocking. This is a one-shot
 * phenomenon -- the count cannot be reset. 
 * If you need a version that resets the count, consider
 * using a Barrier.
 * <p>
 * <b>Sample usage.</b> Here are a set of classes in which
 * a group of worker threads use a countdown to
 * notify a driver when all threads are complete.
 * <pre>
 * class Worker implements Runnable { 
 *   private final CountDown done;
 *   Worker(CountDown d) { done = d; }
 *   public void run() {
 *     doWork();
 *    done.release();
 *   }
 * }
 * 
 * class Driver { // ...
 *   void main() {
 *     CountDown done = new CountDown(N);
 *     for (int i = 0; i < N; ++i) 
 *       new Thread(new Worker(done)).start();
 *     doSomethingElse(); 
 *     done.acquire(); // wait for all to finish
 *   } 
 * }
 * </pre>
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 *
**/

public class CountDown implements Sync {
  protected final int initialCount_;
  protected int count_;

  /** Create a new CountDown with given count value **/
  public CountDown(int count) { count_ = initialCount_ = count; }

  
  /*
    This could use double-check, but doesn't out of concern
    for surprising effects on user programs stemming
    from lack of memory barriers with lack of synch.
  */
  public void acquire() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    synchronized(this) {
      while (count_ > 0) 
        wait();
    }
  }


  public boolean attempt(long msecs) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException();
    synchronized(this) {
      if (count_ <= 0) 
        return true;
      else if (msecs <= 0) 
        return false;
      else {
        long waitTime = msecs;
        long start = System.currentTimeMillis();
        for (;;) {
          wait(waitTime);
          if (count_ <= 0) 
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

  /**
   * Decrement the count.
   * After the initialCount'th release, all current and future
   * acquires will pass
   **/
  public synchronized void release() {
    if (--count_ == 0) 
      notifyAll();
  }

  /** Return the initial count value **/
  public int initialCount() { return initialCount_; }


  /** 
   * Return the current count value.
   * This is just a snapshot value, that may change immediately
   * after returning.
   **/
  public synchronized int currentCount() { return count_; }
}

