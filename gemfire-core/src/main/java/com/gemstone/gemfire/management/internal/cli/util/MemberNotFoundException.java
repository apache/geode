/*
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 */

package com.gemstone.gemfire.management.internal.cli.util;

import com.gemstone.gemfire.GemFireException;

/**
 * The MemberNotFoundException is a GemFirException indicating that a member by name could not be found in the GemFire
 * distributed system.
 * </p>
 * @author jblum
 * @see com.gemstone.gemfire.GemFireException
 * @since 7.0
 */
// TODO this GemFireException should be moved to a more appropriate package!
@SuppressWarnings("unused")
public class MemberNotFoundException extends GemFireException {

  public MemberNotFoundException() {
  }

  public MemberNotFoundException(final String message) {
    super(message);
  }

  public MemberNotFoundException(final Throwable cause) {
    super(cause);
  }

  public MemberNotFoundException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
