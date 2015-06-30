/*=========================================================================
 * Copyright (c) 2010-2011 VMware, Inc. All rights reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. VMware products are covered by
 * one or more patents listed at http://www.vmware.com/go/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

/**
 * Indicates that attempts to allocate more objects in off-heap memory has
 * failed and the Cache will be closed to prevent it from losing distributed
 * consistency.
 * 
 * @author Kirk Lund
 */
public class OutOfOffHeapMemoryException extends CancelException {
  private static final long serialVersionUID = 4111959438738739010L;

  /**
   * Constructs an <code>OutOfOffHeapMemoryError</code> with no detail message.
   */
  public OutOfOffHeapMemoryException() {
  }

  /**
   * Constructs an <code>OutOfOffHeapMemoryError</code> with the specified
   * detail message.
   *
   * @param   message   the detail message.
   */
  public OutOfOffHeapMemoryException(String message) {
    super(message);
  }

}
