/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.persistence;

import com.gemstone.gemfire.GemFireException;

/**
 * Thrown when a replicated region is configured for persistence
 * on some members but none of those members are currently online.
 * 
 * If you see this exception you should restart members that are
 * configured for hosting persistent replicates.
 * 
 * @author sbawaska
 * @since 7.0
 */
public class PersistentReplicatesOfflineException extends GemFireException {
  private static final long serialVersionUID = 6209644027346609970L;

  public PersistentReplicatesOfflineException() {
  }
  
  public PersistentReplicatesOfflineException(String message) {
    super(message);
  }
  
  public PersistentReplicatesOfflineException(String message, Throwable cause) {
    super(message, cause);
  }
}
