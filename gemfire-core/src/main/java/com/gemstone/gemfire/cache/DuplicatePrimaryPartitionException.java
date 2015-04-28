/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.GemFireException;

/**
 * This exception is thrown when two nodes are defined with same primary
 * partitions
 * 
 * @since 6.6
 * @author kbachhhav
 */
public class DuplicatePrimaryPartitionException extends GemFireException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new <code>DuplicatePrimaryPartitionException</code> with no
   * detailed message.
   */

  public DuplicatePrimaryPartitionException() {
    super();
  }

  /**
   * Creates a new <code>DuplicatePrimaryPartitionException</code> with the
   * given detail message.
   */
  public DuplicatePrimaryPartitionException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>DuplicatePrimaryPartitionException</code> with the
   * given cause and no detail message
   */
  public DuplicatePrimaryPartitionException(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a new <code>DuplicatePrimaryPartitionException</code> with the
   * given detail message and cause.
   */
  public DuplicatePrimaryPartitionException(String message, Throwable cause) {
    super(message, cause);
  }

}
