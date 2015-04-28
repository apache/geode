/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * An <code>IncompatibleVersionException</code> that the client version
 * was not compatible with the server version.
 *
 * @since 5.7
 */
public class IncompatibleVersionException extends VersionException {
private static final long serialVersionUID = 668812986092856749L;

  /**
   * Constructs a new <code>IncompatibleVersionException</code>.
   *
   * @param clientVersion The client <code>Version</code>
   * @param serverVersion The server <code>Version</code>
   */
  public IncompatibleVersionException(Object/*Version*/ clientVersion,
      Object/*Version*/ serverVersion) {
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
