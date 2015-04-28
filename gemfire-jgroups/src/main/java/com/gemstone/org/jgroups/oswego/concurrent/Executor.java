/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: Executor.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  19Jun1998  dl               Create public version
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * Interface for objects that execute Runnables,
 * as well as various objects that can be wrapped
 * as Runnables.
 * The main reason to use Executor throughout a program or
 * subsystem is to provide flexibility: You can easily
 * change from using thread-per-task to using pools or
 * queuing, without needing to change most of your code that
 * generates tasks.
 * <p>
 * The general intent is that execution be asynchronous,
 * or at least independent of the caller. For example,
 * one of the simplest implementations of <code>execute</code>
 * (as performed in ThreadedExecutor)
 * is <code>new Thread(command).start();</code>.
 * However, this interface allows implementations that instead
 * employ queueing or pooling, or perform additional
 * bookkeeping.
 * <p>
 * 
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/
public interface Executor {
  /** 
   * Execute the given command. This method is guaranteed
   * only to arrange for execution, that may actually
   * occur sometime later; for example in a new
   * thread. However, in fully generic use, callers
   * should be prepared for execution to occur in
   * any fashion at all, including immediate direct
   * execution.
   * <p>
   * The method is defined not to throw 
   * any checked exceptions during execution of the command. Generally,
   * any problems encountered will be asynchronous and
   * so must be dealt with via callbacks or error handler
   * objects. If necessary, any context-dependent 
   * catastrophic errors encountered during
   * actions that arrange for execution could be accompanied
   * by throwing context-dependent unchecked exceptions.
   * <p>
   * However, the method does throw InterruptedException:
   * It will fail to arrange for execution
   * if the current thread is currently interrupted.
   * Further, the general contract of the method is to avoid,
   * suppress, or abort execution if interruption is detected
   * in any controllable context surrounding execution.
   **/
  public void execute(Runnable command) throws InterruptedException;

}
