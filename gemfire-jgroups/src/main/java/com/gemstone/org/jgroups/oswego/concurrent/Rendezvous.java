/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: Rendezvous.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  11Jun1998  dl               Create public version
  30Jul1998  dl               Minor code simplifications
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * A rendezvous is a barrier that:
 * <ul>
 *   <li> Unlike a CyclicBarrier, is not restricted to use 
 *     with fixed-sized groups of threads.
 *     Any number of threads can attempt to enter a rendezvous,
 *     but only the predetermined number of parties enter
 *     and later become released from the rendezvous at any give time.
 *   <li> Enables each participating thread to exchange information
 *     with others at the rendezvous point. Each entering thread
 *     presents some object on entry to the rendezvous, and
 *     returns some object on release. The object returned is
 *     the result of a RendezvousFunction that is run once per
 *     rendezvous, (it is run by the last-entering thread). By
 *     default, the function applied is a rotation, so each
 *     thread returns the object given by the next (modulo parties)
 *     entering thread. This default function faciliates simple
 *     application of a common use of rendezvous, as exchangers.
 * </ul>
 * <p>
 * Rendezvous use an all-or-none breakage model
 * for failed synchronization attempts: If threads
 * leave a rendezvous point prematurely because of timeout
 * or interruption, others will also leave abnormally
 * (via BrokenBarrierException), until
 * the rendezvous is <code>restart</code>ed. This is usually
 * the simplest and best strategy for sharing knowledge
 * about failures among cooperating threads in the most
 * common usages contexts of Rendezvous.
 * <p>
 * While any positive number (including 1) of parties can
 * be handled, the most common case is to have two parties.
 * <p>
 * <b>Sample Usage</b><p>
 * Here are the highlights of a class that uses a Rendezvous to
 * swap buffers between threads so that the thread filling the
 * buffer  gets a freshly
 * emptied one when it needs it, handing off the filled one to
 * the thread emptying the buffer.
 * <pre>
 * class FillAndEmpty {
 *   Rendezvous exchanger = new Rendezvous(2);
 *   Buffer initialEmptyBuffer = ... a made-up type
 *   Buffer initialFullBuffer = ...
 *
 *   class FillingLoop implements Runnable {
 *     public void run() {
 *       Buffer currentBuffer = initialEmptyBuffer;
 *       try {
 *         while (currentBuffer != null) {
 *           addToBuffer(currentBuffer);
 *           if (currentBuffer.full()) 
 *             currentBuffer = (Buffer)(exchanger.rendezvous(currentBuffer));
 *         }
 *       }
 *       catch (BrokenBarrierException ex) {
 *         return;
 *       }
 *       catch (InterruptedException ex) {
 *         Thread.currentThread().interrupt();
 *       }
 *     }
 *   }
 *
 *   class EmptyingLoop implements Runnable {
 *     public void run() {
 *       Buffer currentBuffer = initialFullBuffer;
 *       try {
 *         while (currentBuffer != null) {
 *           takeFromBuffer(currentBuffer);
 *           if (currentBuffer.empty()) 
 *             currentBuffer = (Buffer)(exchanger.rendezvous(currentBuffer));
 *         }
 *       }
 *       catch (BrokenBarrierException ex) {
 *         return;
 *       }
 *       catch (InterruptedException ex) {
 *         Thread.currentThread().interrupt();
 *       }
 *     }
 *   }
 *
 *   void start() {
 *     new Thread(new FillingLoop()).start();
 *     new Thread(new EmptyingLoop()).start();
 *   }
 * }
 * </pre>
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]

 **/

public class Rendezvous implements Barrier {

  /**
   * Interface for functions run at rendezvous points
   **/
  public interface RendezvousFunction {
    /**
     * Perform some function on the objects presented at
     * a rendezvous. The objects array holds all presented
     * items; one per thread. Its length is the number of parties. 
     * The array is ordered by arrival into the rendezvous.
     * So, the last element (at objects[objects.length-1])
     * is guaranteed to have been presented by the thread performing
     * this function. No identifying information is
     * otherwise kept about which thread presented which item.
     * If you need to 
     * trace origins, you will need to use an item type for rendezvous
     * that includes identifying information. After return of this
     * function, other threads are released, and each returns with
     * the item with the same index as the one it presented.
     **/
    public void rendezvousFunction(Object[] objects);
  }

  /**
   * The default rendezvous function. Rotates the array
   * so that each thread returns an item presented by some
   * other thread (or itself, if parties is 1).
   **/
  public static class Rotator implements RendezvousFunction {
    /** Rotate the array **/
    public void rendezvousFunction(Object[] objects) {
      int lastIdx = objects.length - 1;
      Object first = objects[0];
      for (int i = 0; i < lastIdx; ++i) objects[i] = objects[i+1];
      objects[lastIdx] = first;
    }
  }


  protected final int parties_;


  protected boolean broken_ = false;

  /** 
   * Number of threads that have entered rendezvous
   **/
  protected int entries_ = 0;

  /** 
   * Number of threads that are permitted to depart rendezvous 
   **/
  protected long departures_ = 0;

  /** 
   * Incoming threads pile up on entry until last set done.
   **/
  protected final Semaphore entryGate_;

  /**
   * Temporary holder for items in exchange
   **/
  protected final Object[] slots_;

  /**
   * The function to run at rendezvous point
   **/

  protected RendezvousFunction rendezvousFunction_;

  /** 
   * Create a Barrier for the indicated number of parties,
   * and the default Rotator function to run at each barrier point.
   * @exception IllegalArgumentException if parties less than or equal to zero.
   **/

  public Rendezvous(int parties) { 
    this(parties, new Rotator()); 
  }

  /** 
   * Create a Barrier for the indicated number of parties.
   * and the given function to run at each barrier point.
   * @exception IllegalArgumentException if parties less than or equal to zero.
   **/

  public Rendezvous(int parties, RendezvousFunction function) { 
    if (parties <= 0) throw new IllegalArgumentException();
    parties_ = parties; 
    rendezvousFunction_ = function;
    entryGate_ = new WaiterPreferenceSemaphore(parties);
    slots_ = new Object[parties];
  }

  /**
   * Set the function to call at the point at which all threads reach the
   * rendezvous. This function is run exactly once, by the thread
   * that trips the barrier. The function is not run if the barrier is
   * broken. 
   * @param function the function to run. If null, no function is run.
   * @return the previous function
   **/


  public synchronized RendezvousFunction setRendezvousFunction(RendezvousFunction function) {
    RendezvousFunction old = rendezvousFunction_;
    rendezvousFunction_ = function;
    return old;
  }

  public int parties() { return parties_; }

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

  public void restart() { 
    // This is not very good, but probably the best that can be done
    for (;;) {
      synchronized(this) {
        if (entries_ != 0) {
          notifyAll();
        }
        else {
          broken_ = false; 
          return;
        }
      }
      Thread.yield();
    }
  }


  /**
   * Enter a rendezvous; returning after all other parties arrive.
   * @param x the item to present at rendezvous point. 
   * By default, this item is exchanged with another.
   * @return an item x given by some thread, and/or processed
   * by the rendezvousFunction.
   * @exception BrokenBarrierException 
   * if any other thread
   * in any previous or current barrier 
   * since either creation or the last <code>restart</code>
   * operation left the barrier
   * prematurely due to interruption or time-out. (If so,
   * the <code>broken</code> status is also set.) 
   * Also returns as
   * broken if the RendezvousFunction encountered a run-time exception.
   * Threads that are noticed to have been
   * interrupted <em>after</em> being released are not considered
   * to have broken the barrier.
   * In all cases, the interruption
   * status of the current thread is preserved, so can be tested
   * by checking <code>Thread.interrupted</code>. 
   * @exception InterruptedException if this thread was interrupted
   * during the exchange. If so, <code>broken</code> status is also set.
   **/


  public Object rendezvous(Object x) throws InterruptedException, BrokenBarrierException {
//    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in doRendezvous
    return doRendezvous(x, false, 0);
  }

  /**
   * Wait msecs to complete a rendezvous.
   * @param x the item to present at rendezvous point. 
   * By default, this item is exchanged with another.
   * @param msecs The maximum time to wait.
   * @return an item x given by some thread, and/or processed
   * by the rendezvousFunction.
   * @exception BrokenBarrierException 
   * if any other thread
   * in any previous or current barrier 
   * since either creation or the last <code>restart</code>
   * operation left the barrier
   * prematurely due to interruption or time-out. (If so,
   * the <code>broken</code> status is also set.) 
   * Also returns as
   * broken if the RendezvousFunction encountered a run-time exception.
   * Threads that are noticed to have been
   * interrupted <em>after</em> being released are not considered
   * to have broken the barrier.
   * In all cases, the interruption
   * status of the current thread is preserved, so can be tested
   * by checking <code>Thread.interrupted</code>. 
   * @exception InterruptedException if this thread was interrupted
   * during the exchange. If so, <code>broken</code> status is also set.
   * @exception TimeoutException if this thread timed out waiting for
   * the exchange. If the timeout occured while already in the
   * exchange, <code>broken</code> status is also set.
   **/


  public Object attemptRendezvous(Object x, long msecs) 
    throws InterruptedException, TimeoutException, BrokenBarrierException {
//    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in doRendezvous
    return doRendezvous(x, true, msecs);
  }

  protected Object doRendezvous(Object x, boolean timed, long msecs) 
    throws InterruptedException, TimeoutException, BrokenBarrierException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition

    // rely on semaphore to throw interrupt on entry

    long startTime;

    if (timed) {
      startTime = System.currentTimeMillis();
      if (!entryGate_.attempt(msecs)) {
        throw new TimeoutException(msecs);
      }
    }
    else {
      startTime = 0;
      entryGate_.acquire();
    }

    synchronized(this) {

      Object y = null;

      int index =  entries_++;
      slots_[index] = x;

      try { 
        // last one in runs function and releases
        if (entries_ == parties_) {

          departures_ = entries_;
          notifyAll();

          try {
            if (!broken_ && rendezvousFunction_ != null)
            rendezvousFunction_.rendezvousFunction(slots_);
          }
          catch (RuntimeException ex) {
            broken_ = true;
          }

        }

        else {

          while (!broken_ && departures_ < 1) {
            long timeLeft = 0;
            if (timed) {
              timeLeft = msecs - (System.currentTimeMillis() - startTime);
              if (timeLeft <= 0) {
                broken_ = true;
                departures_ = entries_;
                notifyAll();
                throw new TimeoutException(msecs);
              }
            }
            
            try {
              wait(timeLeft); 
            }
            catch (InterruptedException ex) { 
              if (broken_ || departures_ > 0) { // interrupted after release
                Thread.currentThread().interrupt();
                break;
              }
              else {
                broken_ = true;
                departures_ = entries_;
                notifyAll();
                throw ex;
              }
            }
          }
        }

      }

      finally {

        y = slots_[index];
        
        // Last one out cleans up and allows next set of threads in
        if (--departures_ <= 0) {
          for (int i = 0; i < slots_.length; ++i) slots_[i] = null;
          entryGate_.release(entries_);
          entries_ = 0;
        }
      }

      // continue if no IE/TO throw
      if (broken_)
        throw new BrokenBarrierException(index);
      else
        return y;
    }
  }

}


