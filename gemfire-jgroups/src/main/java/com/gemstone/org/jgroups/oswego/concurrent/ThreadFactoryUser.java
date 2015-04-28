/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: ThreadFactoryUser.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  28aug1998  dl               refactored from Executor classes
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * 
 * Base class for Executors and related classes that rely on thread factories.
 * Generally intended to be used as a mixin-style abstract class, but
 * can also be used stand-alone.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 **/

public class ThreadFactoryUser {

  protected ThreadFactory threadFactory_ = new DefaultThreadFactory();

  protected static class DefaultThreadFactory implements ThreadFactory {
    public Thread newThread(Runnable command) {
      return new Thread(command);
    }
  }

  /** 
   * Set the factory for creating new threads.
   * By default, new threads are created without any special priority,
   * threadgroup, or status parameters.
   * You can use a different factory
   * to change the kind of Thread class used or its construction
   * parameters.
   * @param factory the factory to use
   * @return the previous factory
   **/

  public synchronized ThreadFactory setThreadFactory(ThreadFactory factory) {
    ThreadFactory old = threadFactory_;
    threadFactory_ = factory;
    return old;
  }

  /** 
   * Get the factory for creating new threads.
   **/  
  public synchronized ThreadFactory getThreadFactory() {
    return threadFactory_;
  }

}
