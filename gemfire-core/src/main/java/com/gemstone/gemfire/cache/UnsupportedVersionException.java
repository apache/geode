/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.cache;

/**
 * An <code>UnsupportedVersionException</code> indicates an unsupported version.
 *
 * @since 5.7
 */
public class UnsupportedVersionException extends VersionException {
private static final long serialVersionUID = 1152280300663399399L;

  /**
   * Constructs a new <code>UnsupportedVersionException</code>.
   * 
   * @param versionOrdinal The ordinal of the requested <code>Version</code>
   */
  public UnsupportedVersionException(short versionOrdinal) {
    super(String.valueOf(versionOrdinal));
  }
  
  /**
   * Constructs a new <code>UnsupportedVersionException</code>.
   * 
   * @param message The exception message
   */
  public UnsupportedVersionException(String message) {
    super(message);
  }
}
