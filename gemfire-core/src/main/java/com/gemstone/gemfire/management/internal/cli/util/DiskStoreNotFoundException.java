/*
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 */

package com.gemstone.gemfire.management.internal.cli.util;

import com.gemstone.gemfire.GemFireException;

/**
 * The DiskStoreNotFoundException is a GemFireException class indicating that a disk store by name could not be found
 * on a member specified by name!
 * </p>
 * @author jblum
 * @see com.gemstone.gemfire.GemFireException
 * @since 7.0
 */
// TODO this GemFireException should be moved to a more appropriate package!
  @SuppressWarnings("unused")
public class DiskStoreNotFoundException extends GemFireException {

  public DiskStoreNotFoundException() {
  }

  public DiskStoreNotFoundException(final String message) {
    super(message);
  }

  public DiskStoreNotFoundException(final Throwable cause) {
    super(cause);
  }

  public DiskStoreNotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
