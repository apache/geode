/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/

/*
  File: Barrier.java

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
 * Barriers serve
 * as synchronization points for groups of threads that
 * must occasionally wait for each other. 
 * Barriers may support any of several methods that
 * accomplish this synchronization. This interface
 * merely expresses their minimal commonalities:
 * <ul>
 *   <li> Every barrier is defined for a given number
 *     of <code>parties</code> -- the number of threads
 *     that must meet at the barrier point. (In all current
 *     implementations, this
 *     value is fixed upon construction of the Barrier.)
 *   <li> A barrier can become <code>broken</code> if
 *     one or more threads leave a barrier point prematurely,
 *     generally due to interruption or timeout. Corresponding
 *     synchronization methods in barriers fail, throwing
 *     BrokenBarrierException for other threads
 *     when barriers are in broken states.
 * </ul>
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]

 **/
public interface Barrier {


  /** 
   * Return the number of parties that must meet per barrier
   * point. The number of parties is always at least 1.
   **/

  public int parties();

  /**
   * Returns true if the barrier has been compromised
   * by threads leaving the barrier before a synchronization
   * point (normally due to interruption or timeout). 
   * Barrier methods in implementation classes throw
   * throw BrokenBarrierException upon detection of breakage.
   * Implementations may also support some means
   * to clear this status.
   **/

  public boolean broken();
}
