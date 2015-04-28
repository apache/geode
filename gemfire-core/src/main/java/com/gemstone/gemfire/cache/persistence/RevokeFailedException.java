/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.persistence;

import com.gemstone.gemfire.GemFireException;

/**
 * Thrown when a member tries to revoke a persistent ID, but the member
 * with that persistent ID is currently running. You can only revoke
 * members which is not running.
 * 
 * @author dsmith
 * @since 6.6.2
 */
public class RevokeFailedException extends GemFireException {

  private static final long serialVersionUID = -2629287782021455875L;

  public RevokeFailedException() {
    super();
  }

  public RevokeFailedException(String message, Throwable cause) {
    super(message, cause);
  }

  public RevokeFailedException(String message) {
    super(message);
  }

  public RevokeFailedException(Throwable cause) {
    super(cause);
  }

  

}
