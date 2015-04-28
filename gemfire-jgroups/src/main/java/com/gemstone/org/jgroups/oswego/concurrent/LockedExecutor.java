/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: LockedExecutor.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  21Jun1998  dl               Create public version
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * An implementation of Executor that 
 * invokes the run method of the supplied command within
 * a synchronization lock and then returns.
 * 
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/
public class LockedExecutor implements Executor {
  
  /** The mutex **/
  protected final Sync mutex_;

  /** 
   * Create a new LockedExecutor that relies on the given mutual
   * exclusion lock. 
   * @param mutex Any mutual exclusion lock.
   * Standard usage is to supply an instance of <code>Mutex</code>,
   * but, for example, a Semaphore initialized to 1 also works.
   * On the other hand, many other Sync implementations would not
   * work here, so some care is required to supply a sensible 
   * synchronization object.
   **/
 
  public LockedExecutor(Sync mutex) {
    mutex_ = mutex;
  }

  /** 
   * Execute the given command directly in the current thread,
   * within the supplied lock.
   **/
  public void execute(Runnable command) throws InterruptedException {
    if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition for safety
    mutex_.acquire();
    try {
      command.run();
    }
    finally {
      mutex_.release();
    }
  }

}
