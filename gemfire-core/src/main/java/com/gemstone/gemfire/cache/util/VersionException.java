/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * An <code>VersionException</code> is an exception that indicates
 * a client / server version mismatch exception has occurred.
 *
 * @author Barry Oglesby
 * @deprecated Use {@link com.gemstone.gemfire.cache.VersionException} instead.
 *
 * @since 5.6
 */
@Deprecated
public abstract class VersionException extends GemFireCheckedException {

  /** Constructs a new <code>VersionException</code>. */
  public VersionException() {
    super();
  }

  /** Constructs a new <code>VersionException</code> with a message string. */
  public VersionException(String s) {
    super(s);
  }

  /** Constructs a <code>VersionException</code> with a message string and
   * a base exception
   */
  public VersionException(String s, Throwable cause) {
    super(s, cause);
  }

  /** Constructs a <code>VersionException</code> with a cause */
  public VersionException(Throwable cause) {
    super(cause);
  }
}
