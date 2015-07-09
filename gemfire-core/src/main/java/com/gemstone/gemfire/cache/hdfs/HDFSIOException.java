/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.cache.hdfs;

import com.gemstone.gemfire.GemFireIOException;

/**
 * Thrown when an error has occurred while attempted to use
 * the HDFS file system. This error may indicate a failure of the HDFS
 * system.
 * 
 * @author dsmith
 * 
 * @since 7.5
 * 
 */
public class HDFSIOException extends GemFireIOException {

  /**
   * @param message
   * @param cause
   */
  public HDFSIOException(String message, Throwable cause) {
    super(message, cause);
    // TODO Auto-generated constructor stub
  }

  /**
   * @param message
   */
  public HDFSIOException(String message) {
    super(message);
    // TODO Auto-generated constructor stub
  }

}
