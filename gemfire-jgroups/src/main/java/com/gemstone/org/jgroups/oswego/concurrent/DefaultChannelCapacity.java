/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
/*
  File: DefaultChannelCapacity.java

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
 * A utility class to set the default capacity of
 * BoundedChannel
 * implementations that otherwise require a capacity argument
 * @see BoundedChannel
 * [<a href="http://gee.cs.oswego.edu/dl/classes/EDU/oswego/cs/dl/util/concurrent/intro.html"> Introduction to this package. </a>] <p>
 **/

public class DefaultChannelCapacity {

  /** The initial value of the default capacity is 1024 **/
  public static final int INITIAL_DEFAULT_CAPACITY = 1024;

  /**  the current default capacity **/
  private static final SynchronizedInt defaultCapacity_ = 
    new SynchronizedInt(INITIAL_DEFAULT_CAPACITY);

  /**
   * Set the default capacity used in 
   * default (no-argument) constructor for BoundedChannels
   * that otherwise require a capacity argument.
   * @exception IllegalArgumentException if capacity less or equal to zero
   */
  public static void set(int capacity) {
    if (capacity <= 0) throw new IllegalArgumentException();
    defaultCapacity_.set(capacity);
  }

  /**
   * Get the default capacity used in 
   * default (no-argument) constructor for BoundedChannels
   * that otherwise require a capacity argument.
   * Initial value is <code>INITIAL_DEFAULT_CAPACITY</code>
   * @see #INITIAL_DEFAULT_CAPACITY
   */
  public static int get() {
    return defaultCapacity_.get();
  }
}
