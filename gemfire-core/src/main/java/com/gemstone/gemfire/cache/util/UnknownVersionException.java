/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache.util;

/**
 * An <code>UnknownVersionException</code> indicates an unknown version.
 *
 * @author Barry Oglesby
 * @deprecated
 *
 * @since 5.6
 */
public class UnknownVersionException extends VersionException {

  private static final long serialVersionUID = 7379530185697556990L;

  /**
   * Constructs a new <code>UnknownVersionException</code>.
   * 
   * @param versionOrdinal The ordinal of the requested <code>Version</code>
   */
  public UnknownVersionException(byte versionOrdinal) {
    super(String.valueOf(versionOrdinal));
  }
  
  /**
   * Constructs a new <code>UnknownVersionException</code>.
   * 
   * @param message The exception message
   */
  public UnknownVersionException(String message) {
    super(message);
  }
}
