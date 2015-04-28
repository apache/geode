package com.gemstone.gemfire.distributed.internal;

/** Sizeable objects have a getSize() method that returns the approximate size of the object.

    @author Bruce Schuchardt
    @since 5.0
 */

public interface Sizeable {
  /** returns the approximate size of this object */
  public int getSize();
}
