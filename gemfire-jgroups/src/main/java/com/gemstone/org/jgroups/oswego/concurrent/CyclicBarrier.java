/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: CyclicBarrier.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  11Jul1998  dl               Create public version
  28Aug1998  dl               minor code simplification
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * A cyclic barrier is a reasonable choice for a barrier in contexts
 * involving a fixed sized group of threads that
 * must occasionally wait for each other. 
 * (A Rendezvous better handles applications in which
 * any number of threads meet, n-at-a-time.)
 * <p>
 * CyclicBarriers use an all-or-none breakage model
 * for failed synchronization attempts: If threads
 * leave a barrier point prematurely because of timeout
 * or interruption, others will also leave abnormally
 * (via BrokenBarrierException), until
 * the barrier is <code>restart</code>ed. This is usually
 * the simplest and best strategy for sharing knowledge
 * about failures among cooperating threads in the most
 * common usages contexts of Barriers.
 * This implementation  has the property that interruptions
 * among newly arriving threads can cause as-yet-unresumed
 * threads from a previous barrier cycle to return out
 * as broken. This transmits breakage
 * as early as possible, but with the possible byproduct that
 * only some threads returning out of a barrier will realize
 * that it is newly broken. (Others will not realize this until a
 * future cycle.) (The Rendezvous class has a more uniform, but
 * sometimes less desirable policy.)
 * <p>
 * Barriers support an optional Runnable command
 * that is run once per barrier point.
 * <p>
 * <b>Sample usage</b> Here is a code sketch of 
 *  a  barrier in a parallel decomposition design.
 * <pre>
 * class Solver {
 *   final int N;
 *   final float[][] data;
 *   final CyclicBarrier barrier;
 *   
 *   class Worker implements Runnable {
 *      int myRow;
 *      Worker(int row) { myRow = row; }
 *      public void run() {
 *         while (!done()) {
 *            processRow(myRow);
 *
 *            try {
 *              barrier.barrier(); 
 *            }
 *            catch (InterruptedException ex) { return; }
 *            catch (BrokenBarrierException ex) { return; }
 *         }
 *      }
 *   }
 *
 *   public Solver(float[][] matrix) {
 *     data = matrix;
 *     N = matrix.length;
 *     barrier = new CyclicBarrier(N);
 *     barrier.setBarrierCommand(new Runnable() {
 *       public void run() { mergeRows(...); }
 *     });
 *     for (int i = 0; i < N; ++i) {
 *       new Thread(new Worker(i)).start();
 *     waitUntilDone();
 *    }
 * }
 * </pre>
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]

 **/
public class CyclicBarrier implements Barrier {

  protected final int parties_;
  protected boolean broken_ = false;
  protected Runnable barrierCommand_ = null;
  protected int count_; // number of parties still waiting
  protected int resets_ = 0; // incremented on each release

  /** 
   * Create a CyclicBarrier for the indicated number of parties,
   * and no command to run at each barrier.
   * @exception IllegalArgumentException if parties less than or equal to zero.
   **/

  public CyclicBarrier(int parties) { this(parties, null); }

  /** 
   * Create a CyclicBarrier for the indicated number of parties.
   * and the given command to run at each barrier point.
   * @exception IllegalArgumentException if parties less than or equal to zero.
   **/

  public CyclicBarrier(int parties, Runnable command) { 
    if (parties <= 0) throw new IllegalArgumentException();
    parties_ = parties; 
    count_ = parties;
    barrierCommand_ = command;
  }

  /**
   * Set the command to run at the point at which all threads reach the
   * barrier. This command is run exactly once, by the thread
   * that trips the barrier. The command is not run if the barrier is
   * broken.
   * @param command the command to run. If null, no command is run.
   * @return the previous command
   **/

  public synchronized Runnable setBarrierCommand(Runnable command) {
    Runnable old = barrierCommand_;
    barrierCommand_ = command;
    return old;
  }

  public synchronized boolean broken() { return broken_; }

  /**
   * Reset to initial state. Clears both the broken status
   * and any record of waiting threads, and releases all
   * currently waiting threads with indeterminate return status.
   * This method is intended only for use in recovery actions
   * in which it is somehow known
   * that no thread could possibly be relying on the
   * the synchronization properties of this barrier.
   **/

  public synchronized void restart() { 
    broken_ = false; 
    ++resets_;
    count_ = parties_;
    notifyAll();
  }
  
 
  public int parties() { return parties_; }

  /**
   * Enter barrier and wait for the other parties()-1 threads.
   * @return the arrival index: the number of other parties 
   * that were still waiting
   * upon entry. This is a unique value from zero to parties()-1.
   * If it is zero, then the current
   * thread was the last party to hit barrier point
   * and so was responsible for releasing the others. 
   * @exception BrokenBarrierException if any other thread
   * in any previous or current barrier 
   * since either creation or the last <code>restart</code>
   * operation left the barrier
   * prematurely due to interruption or time-out. (If so,
   * the <code>broken</code> status is also set.)
   * Threads that are notified to have been
   * interrupted <em>after</em> being released are not considered
   * to have broken the barrier.
   * In all cases, the interruption
   * status of the current thread is preserved, so can be tested
   * by checking <code>Thread.interrupted</code>. 
   * @exception InterruptedException if this thread was interrupted
   * during the barrier, and was the one causing breakage. 
   * If so, <code>broken</code> status is also set.
   **/

  public int barrier() throws InterruptedException, BrokenBarrierException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
   return doBarrier(false, 0);
  }

  /**
   * Enter barrier and wait at most msecs for the other parties()-1 threads.
   * @return if not timed out, the arrival index: the number of other parties 
   * that were still waiting
   * upon entry. This is a unique value from zero to parties()-1.
   * If it is zero, then the current
   * thread was the last party to hit barrier point
   * and so was responsible for releasing the others. 
   * @exception BrokenBarrierException 
   * if any other thread
   * in any previous or current barrier 
   * since either creation or the last <code>restart</code>
   * operation left the barrier
   * prematurely due to interruption or time-out. (If so,
   * the <code>broken</code> status is also set.) 
   * Threads that are noticed to have been
   * interrupted <em>after</em> being released are not considered
   * to have broken the barrier.
   * In all cases, the interruption
   * status of the current thread is preserved, so can be tested
   * by checking <code>Thread.interrupted</code>. 
   * @exception InterruptedException if this thread was interrupted
   * during the barrier. If so, <code>broken</code> status is also set.
   * @exception TimeoutException if this thread timed out waiting for
   *  the barrier. If the timeout occured while already in the
   * barrier, <code>broken</code> status is also set.
   **/

  public int attemptBarrier(long msecs) 
    throws InterruptedException, TimeoutException, BrokenBarrierException {
//    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in doBarrier
    return doBarrier(true, msecs);
  }

  protected synchronized int doBarrier(boolean timed, long msecs) 
    throws InterruptedException, TimeoutException, BrokenBarrierException  { 
    
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition

    int index = --count_;

    if (broken_) {
      throw new BrokenBarrierException(index);
    }
    else if (Thread.interrupted()) {
      broken_ = true;
      notifyAll();
      throw new InterruptedException();
    }
    else if (index == 0) {  // tripped
      count_ = parties_;
      ++resets_;
      notifyAll();
      try {
        if (barrierCommand_ != null)
          barrierCommand_.run();
        return 0;
      }
      catch (RuntimeException ex) {
        broken_ = true;
        return 0;
      }
    }
    else if (timed && msecs <= 0) {
      broken_ = true;
      notifyAll();
      throw new TimeoutException(msecs);
    }
    else {                   // wait until next reset
      int r = resets_;      
      long startTime = (timed)? System.currentTimeMillis() : 0;
      long waitTime = msecs;
      for (;;) {
        boolean interrupted = Thread.interrupted(); // GemStoneAddition
        try {
          wait(waitTime);
        }
        catch (InterruptedException ex) {
          // Only claim that broken if interrupted before reset
          if (resets_ == r) { 
            broken_ = true;
            notifyAll();
            throw ex;
          }
          else {
//            Thread.currentThread().interrupt(); // propagate
            interrupted = true; // GemStoneAddition
          }
        }
        finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }

        if (broken_) 
          throw new BrokenBarrierException(index);

        else if (r != resets_)
          return index;

        else if (timed) {
          waitTime = msecs - (System.currentTimeMillis() - startTime);
          if  (waitTime <= 0) {
            broken_ = true;
            notifyAll();
            throw new TimeoutException(msecs);
          }
        }
      }
    }
  }

}
