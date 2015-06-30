/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs;

import com.gemstone.gemfire.cache.CacheException;

/**
 * Thrown when attempting to create a {@link HDFSStore} if one already exists.
 * 
 * @author Ashvin Agrawal
 */
public class StoreExistsException extends CacheException {
  private static final long serialVersionUID = 1L;

  public StoreExistsException(String storeName) {
    super(storeName);
  }
}
