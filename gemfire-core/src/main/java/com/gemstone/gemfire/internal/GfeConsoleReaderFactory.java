/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.internal;

import java.io.Console;

import com.gemstone.gemfire.management.internal.cli.CliUtil;
import com.gemstone.gemfire.management.internal.cli.LogWrapper;
import com.gemstone.gemfire.management.internal.cli.util.GfshConsoleReader;

/**
 * Factory for Console Reader Utility.
 * 
 * Default uses <code>java.io.Console</code> returned by
 * <code>System.console()</code>
 * 
 * 
 * @author Abhishek Chaudhari
 * @since 7.0.1
 */
public class GfeConsoleReaderFactory {
  private static GfeConsoleReader defaultConsoleReader = createConsoleReader();
  
  public static final GfeConsoleReader getDefaultConsoleReader() {
    return defaultConsoleReader;
  }
  
  public static final GfeConsoleReader createConsoleReader() {
    GfeConsoleReader consoleReader = null;

    if (CliUtil.isGfshVM()) {
      LogWrapper.getInstance().info("GfeConsoleReaderFactory.createConsoleReader(): isGfshVM");
      consoleReader = new GfshConsoleReader();
      LogWrapper.getInstance().info("GfeConsoleReaderFactory.createConsoleReader(): consoleReader: "+consoleReader+"="+consoleReader.isSupported());
    }
    if (consoleReader == null) {
      consoleReader = new GfeConsoleReader();
    }
    return consoleReader;
  }

  public static class GfeConsoleReader {
    private Console console;
    
    protected GfeConsoleReader() {
      console = System.console();
    }
    
    public boolean isSupported() {
      return console != null;
    }
    
    public String readLine(String textToPrompt) {
      if (isSupported()) {
        return console.readLine(textToPrompt);
      }
      return null;
    }
    
    public char [] readPassword(String textToPrompt) {
      if (isSupported()) {
        return console.readPassword(textToPrompt);
      }
      return null;
    }

    @Override
    public String toString() {
      return getClass().getSimpleName() + ": isSupported=" + isSupported();
    }
  }
}
