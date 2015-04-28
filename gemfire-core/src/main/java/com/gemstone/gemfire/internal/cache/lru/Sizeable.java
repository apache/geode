/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.lru;

/**
 * An interface that allows an object to define its own size.<br>
 *
 *<b>Sample Implementation</b><br>
 *<code>public int getSizeInBytes(){</code><br>
    // The sizes of the primitive as well as object instance variables are calculated:<br>
    
    <code>int size = 0;</code><br>
    
    // Add overhead for this instance.<br>
    <code>size += Sizeable.PER_OBJECT_OVERHEAD;</code><br>

    // Add object references (implements Sizeable)<br>
    // value reference = 4 bytes <br>
    
    <code>size += 4;</code><br>
    
    // Add primitive instance variable size<br>
    // byte bytePr = 1 byte<br>
    // boolean flag = 1 byte<br>
    
    <code>size += 2;</code><br>
    
    // Add individual object size<br> 
    <code>size += (value.getSizeInBytes());</code><br> 
     
    <code>return size;</code><br>
  }<br>
 *
 * @author David Whitlock
 *
 * @since 3.2
 */
public interface Sizeable {

  /** The overhead of an object in the VM in bytes */
  public static final int PER_OBJECT_OVERHEAD = 8; // TODO for a 64bit jvm with small oops this is 12; for other 64bit jvms it is 16

  /**
   * Returns the size (in bytes) of this object including the {@link
   * #PER_OBJECT_OVERHEAD}.
   */
  public int getSizeInBytes();

}
