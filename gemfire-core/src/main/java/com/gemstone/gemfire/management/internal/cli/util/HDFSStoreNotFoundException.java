/*
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 */

package com.gemstone.gemfire.management.internal.cli.util;

import com.gemstone.gemfire.GemFireException;

/**
 * The HDFSStoreNotFoundException is a GemFireException class indicating that a hdfs store by name could not be found
 * on a member specified by name!
 * </p>
 * @see com.gemstone.gemfire.GemFireException
 */
// TODO this GemFireException should be moved to a more appropriate package!
  @SuppressWarnings("unused")
public class HDFSStoreNotFoundException extends GemFireException {

  public HDFSStoreNotFoundException() {
  }

  public HDFSStoreNotFoundException(final String message) {
    super(message);
  }

  public HDFSStoreNotFoundException(final Throwable cause) {
    super(cause);
  }

  public HDFSStoreNotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
