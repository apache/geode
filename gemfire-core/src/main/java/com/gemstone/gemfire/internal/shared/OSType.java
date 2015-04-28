/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.shared;

/**
 * Enumeration for various types of OSes supported by {@link NativeCalls} using
 * JNA ({@link NativeCallsJNAImpl}).
 */
public enum OSType {

  /** Indicates a Linux family OS. */
  LINUX {
    @Override
    public boolean isPOSIX() {
      return true;
    }
  },

  /** Indicates a Solaris family OS. */
  SOLARIS {
    @Override
    public boolean isPOSIX() {
      return true;
    }
  },

  /** Indicates a MacOSX family OS. */
  MACOSX {
    @Override
    public boolean isPOSIX() {
      return true;
    }
  },

  /** Indicates a FreeBSD family OS. */
  FREEBSD {
    @Override
    public boolean isPOSIX() {
      return true;
    }
  },

  /**
   * Indicates a generic POSIX complaint OS (at least to a reasonable degree).
   */
  GENERIC_POSIX {
    @Override
    public boolean isPOSIX() {
      return true;
    }
  },

  /**
   * Indicates a Microsoft Windows family OS.
   */
  WIN,

  /**
   * Indicates an OS whose kind cannot be determined or that is not supported by
   * JNA.
   */
  GENERIC;

  /**
   * Indicates a Microsoft Windows family OS.
   */
  public final boolean isWindows() {
    return this == WIN;
  }

  /**
   * Indicates an OS that conforms to POSIX specifications (at least to a
   * reasonable degree).
   */
  public boolean isPOSIX() {
    return false;
  }
}
