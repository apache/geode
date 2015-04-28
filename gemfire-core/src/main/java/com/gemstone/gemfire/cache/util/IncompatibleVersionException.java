/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.internal.Version;

/**
 * An <code>Incompatible</code> indicates an unknown version.
 *
 * @author Barry Oglesby
 * @deprecated
 *
 * @since 5.6
 */
public class IncompatibleVersionException extends VersionException {

  private static final long serialVersionUID = 7008667865037538081L;

  /**
   * Constructs a new <code>IncompatibleVersionException</code>.
   *
   * @param clientVersion The client version
   * @param serverVersion The server version
   */
  public IncompatibleVersionException(Object clientVersion,
      Object serverVersion) {
    // the arguments should be of class Version, but that's an
    // internal class and this is an external class that shouldn't
    // ref internals in method signatures
    this("Client version " + clientVersion
        + " is incompatible with server version " + serverVersion);
  }

  /**
   * Constructs a new <code>IncompatibleVersionException</code>.
   *
   * @param message The exception message
   */
  public IncompatibleVersionException(String message) {
    super(message);
  }
}
