/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.util;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *
 * @author Eric Zoerner
 *
 */
public abstract class DebuggerSupport  {
  
  /** Creates a new instance of DebuggerSupport */
  private DebuggerSupport() {
  }
  
  /** Debugger support */
  public static void waitForJavaDebugger(LogWriterI18n logger) {
    waitForJavaDebugger(logger, null);
  }
  
  @SuppressFBWarnings(value="IL_INFINITE_LOOP", justification="Endless loop is for debugging purposes.") 
  public static void waitForJavaDebugger(LogWriterI18n logger, String extraLogMsg) {
    boolean cont = false;
    String msg = ":";
    if (extraLogMsg != null)
      msg += extraLogMsg;
    logger.severe(LocalizedStrings.DebuggerSupport_WAITING_FOR_DEBUGGER_TO_ATTACH_0, msg);
    boolean interrupted = false;
    while (!cont) { // set cont to true in debugger when ready to continue
      try {
        // SET BREAKPOINT HERE
        Thread.sleep(1000);
      }
      catch (InterruptedException e) {
        interrupted = true;
        // ...but keep going, waiting for debugger
      }
    } // while
    if (interrupted) {
      Thread.currentThread().interrupt();
    }
    logger.info(LocalizedStrings.DebuggerSupport_DEBUGGER_CONTINUING);
  }
}
