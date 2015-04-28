/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: Puttable.java

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
 * This interface exists to enable stricter type checking
 * for channels. A method argument or instance variable
 * in a producer object can be declared as only a Puttable
 * rather than a Channel, in which case a Java compiler
 * will disallow take operations.
 * <p>
 * Full method descriptions appear in the Channel interface.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 * @see Channel
 * @see Takable
**/

public interface Puttable {


  /** 
   * Place item in the channel, possibly waiting indefinitely until
   * it can be accepted. Channels implementing the BoundedChannel
   * subinterface are generally guaranteed to block on puts upon
   * reaching capacity, but other implementations may or may not block.
   * @param item the element to be inserted. Should be non-null.
   * @exception InterruptedException if the current thread has
   * been interrupted at a point at which interruption
   * is detected, in which case the element is guaranteed not
   * to be inserted. Otherwise, on normal return, the element is guaranteed
   * to have been inserted.
  **/
  public void put(Object item) throws InterruptedException;


  /** 
   * Place item in channel only if it can be accepted within
   * msecs milliseconds. The time bound is interpreted in
   * a coarse-grained, best-effort fashion. 
   * @param item the element to be inserted. Should be non-null.
   * @param msecs the number of milliseconds to wait. If less than
   * or equal to zero, the method does not perform any timed waits,
   * but might still require
   * access to a synchronization lock, which can impose unbounded
   * delay if there is a lot of contention for the channel.
   * @return true if accepted, else false
   * @exception InterruptedException if the current thread has
   * been interrupted at a point at which interruption
   * is detected, in which case the element is guaranteed not
   * to be inserted (i.e., is equivalent to a false return).
  **/
  public boolean offer(Object item, long msecs) throws InterruptedException;
}
