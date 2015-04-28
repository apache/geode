/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.execute;

import com.gemstone.gemfire.GemFireException;

public class BucketMovedException extends GemFireException {
    private static final long serialVersionUID = 4893171227542647452L;

    /**
     * Creates new function exception with given error message.
     * 
     * @param msg
     * @since 6.0
     */
    public BucketMovedException(String msg) {
      super(msg);
    }

    /**
     * Creates new function exception with given error message and optional nested
     * exception.
     * 
     * @param msg
     * @param cause
     * @since 6.0 
     */
    public BucketMovedException(String msg, Throwable cause) {
      super(msg, cause);
    }

    /**
     * Creates new function exception given throwable as a cause and source of
     * error message.
     * 
     * @param cause
     * @since 6.0 
     */
    public BucketMovedException(Throwable cause) {
      super(cause);
    }
  }

