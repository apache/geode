/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/

/*
  File: Callable.java

  Originally written by Doug Lea and released into the public domain.
  This may be used for any purposes whatsoever without acknowledgment.
  Thanks for the assistance and support of Sun Microsystems Labs,
  and everyone contributing, testing, and using this code.

  History:
  Date       Who                What
  30Jun1998  dl               Create public version
   5Jan1999  dl               Change Exception to Throwable in call signature
  27Jan1999  dl               Undo last change
*/

package com.gemstone.org.jgroups.oswego.concurrent;

/**
 * Interface for runnable actions that bear results and/or throw Exceptions.
 * This interface is designed to provide a common protocol for
 * result-bearing actions that can be run independently in threads, 
 * in which case
 * they are ordinarily used as the bases of Runnables that set
 * FutureResults
 * <p>
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 * @see FutureResult
 **/

public interface Callable {
  /** Perform some action that returns a result or throws an exception **/
  Object call() throws Exception;
}

