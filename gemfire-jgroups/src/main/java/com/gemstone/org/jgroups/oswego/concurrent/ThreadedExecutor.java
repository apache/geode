/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: ThreadedExecutor.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  21Jun1998  dl               Create public version
  28aug1998  dl               factored out ThreadFactoryUser
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * 
 * An implementation of Executor that creates a new
 * Thread that invokes the run method of the supplied command.
 * 
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/
public class ThreadedExecutor extends ThreadFactoryUser implements Executor {

  /** 
   * Execute the given command in a new thread.
   **/
  public synchronized void execute(Runnable command) throws InterruptedException {
    if (Thread.interrupted()) 
      throw new InterruptedException();

    Thread thread = getThreadFactory().newThread(command);
    thread.start();
  }
}
