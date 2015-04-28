/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: TimeoutSync.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
   1Aug1998  dl               Create public version
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * A TimeoutSync is an adaptor class that transforms all
 * calls to acquire to instead invoke attempt with a predetermined
 * timeout value.
 *<p>
 * <b>Sample Usage</b>. A TimeoutSync can be used to obtain
 * Timeouts for locks used in SyncCollections. For example:
 * <pre>
 * Mutex lock = new Mutex();
 * TimeoutSync timedLock = new TimeoutSync(lock, 1000); // 1 sec timeouts
 * Set set = new SyncSet(new HashSet(), timedlock);
 * try {
 *   set. add("hi");
 * }
 * // SyncSets translate timeouts and other lock failures 
 * //   to unsupported operation exceptions, 
 * catch (UnsupportedOperationException ex) {
 *    System.out.println("Lock failure");
 * }
 * </pre>  
 *
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 * @see Sync
**/


public class TimeoutSync implements Sync {

  protected final Sync sync_;     // the adapted sync
  protected final long timeout_;  // timeout value

  /** 
   * Create a TimeoutSync using the given Sync object, and
   * using the given timeout value for all calls to acquire.
   **/

  public TimeoutSync(Sync sync, long timeout) {
    sync_ = sync;
    timeout_ = timeout;
  }

  public void acquire() throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition for safety
    if (!sync_.attempt(timeout_)) throw new TimeoutException(timeout_);
  }

  public boolean attempt(long msecs) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition for safety
    return sync_.attempt(msecs);
  }

  public void release() {
    sync_.release();
  }

}
