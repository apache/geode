/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: Takable.java

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
 * in a consumer object can be declared as only a Takable
 * rather than a Channel, in which case a Java compiler
 * will disallow put operations.
 * <p>
 * Full method descriptions appear in the Channel interface.
 * <p>[<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>]
 * @see Channel
 * @see Puttable
**/

public interface Takable {

  /** 
   * Return and remove an item from channel, 
   * possibly waiting indefinitely until
   * such an item exists.
   * @return  some item from the channel. Different implementations
   *  may guarantee various properties (such as FIFO) about that item
   * @exception InterruptedException if the current thread has
   * been interrupted at a point at which interruption
   * is detected, in which case state of the channel is unchanged.
   *
  **/
  public Object take() throws InterruptedException;


  /** 
   * Return and remove an item from channel only if one is available within
   * msecs milliseconds. The time bound is interpreted in a coarse
   * grained, best-effort fashion.
   * @param msecs the number of milliseconds to wait. If less than
   *  or equal to zero, the operation does not perform any timed waits,
   * but might still require
   * access to a synchronization lock, which can impose unbounded
   * delay if there is a lot of contention for the channel.
   * @return some item, or null if the channel is empty.
   * @exception InterruptedException if the current thread has
   * been interrupted at a point at which interruption
   * is detected, in which case state of the channel is unchanged
   * (i.e., equivalent to a false return).
  **/

  public Object poll(long msecs) throws InterruptedException;

}
