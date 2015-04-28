/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
   
package com.gemstone.gemfire.internal;

/**
 * Used to determine if product should use pure java mode.
 */
public final class PureJavaMode {
  /**
   * System property to set to true to force pure java mode
   */
  public final static String PURE_MODE_PROPERTY = "gemfire.pureJavaMode";
  /**
   * System property to set to true enable debug information regarding native library loading.
   * @since 5.1
   */
  public final static String LOADLIBRARY_DEBUG_PROPERTY = "gemfire.loadLibrary.debug";

  private static final boolean debug = Boolean.getBoolean(LOADLIBRARY_DEBUG_PROPERTY);

  private static final boolean isPure;
  private static final boolean is64Bit;
  private static final boolean osStatsAreAvailable;
  static {
    boolean tmpIsPure = false;
    if (Boolean.getBoolean(PURE_MODE_PROPERTY)) {
      if (debug) {
        System.out.println("property " + PURE_MODE_PROPERTY + " is true");
      }
      tmpIsPure = true;
    } else {
      tmpIsPure = false;
      try {
        //Attempting to load the library
        SharedLibrary.loadLibrary(debug);
      } catch (UnsatisfiedLinkError ignore) {
        if (debug) {
          System.out.println("java.library.path is set to:\n" + System.getProperty("java.library.path"));
          System.out.println("Error: Failed to load library " + SharedLibrary.getName());
          ignore.printStackTrace();
        }
        tmpIsPure = true;
      }
    }
    isPure = tmpIsPure;
    is64Bit = SharedLibrary.is64Bit();
    String osName = System.getProperty("os.name", "unknown");
    osStatsAreAvailable = osName.startsWith("Linux") || ! isPure; 
  }

  public final static boolean isPure() {
    return isPure;
  }
  public final static boolean is64Bit() {
    return is64Bit;
  }
  /**
   * Linux has OsStats even in PureJava mode but other platforms
   * require the native code to provide OS Statistics.
   * return true if OSStatistics are available
   */
  public final static boolean osStatsAreAvailable() {
    return osStatsAreAvailable;
  }
}
