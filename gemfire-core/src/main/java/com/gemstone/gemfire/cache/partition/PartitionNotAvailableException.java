/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.partition;

import com.gemstone.gemfire.GemFireException;

/**
 * This exception is thrown when for the given fixed partition, datastore
 * (local-max-memory > 0) is not available.
 * 
 * @author kbachhhav
 * @since 6.6
 */

public class PartitionNotAvailableException extends GemFireException {

  private static final long serialVersionUID = 1L;

  /**
   * Creates a new <code>PartitionNotAvailableException</code> with no detailed
   * message.
   */

  public PartitionNotAvailableException() {
    super();
  }

  /**
   * Creates a new <code>PartitionNotAvailableException</code> with the given
   * detail message.
   */
  public PartitionNotAvailableException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>PartitionNotAvailableException</code> with the given
   * cause and no detail message
   */
  public PartitionNotAvailableException(Throwable cause) {
    super(cause);
  }

  /**
   * Creates a new <code>PartitionNotAvailableException</code> with the given
   * detail message and cause.
   */
  public PartitionNotAvailableException(String message, Throwable cause) {
    super(message, cause);
  }

}
