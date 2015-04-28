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
package com.gemstone.gemfire.internal.cache.versions;

import java.util.ConcurrentModificationException;

/**
 * ConcurrentCacheModification is thrown by the internal concurrency-checking
 * mechanism when a conflict is detected.  It is not currently meant to be
 * exposed to customers.
 * 
 * @author Bruce Schuchardt
 * @since 7.0
 */
public class ConcurrentCacheModificationException extends
    ConcurrentModificationException {
  public ConcurrentCacheModificationException() {
  }

  public ConcurrentCacheModificationException(String message) {
      super(message);
  }

}
