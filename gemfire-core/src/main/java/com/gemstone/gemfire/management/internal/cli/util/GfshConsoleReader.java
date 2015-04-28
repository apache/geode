/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.gemstone.gemfire.management.internal.cli.util;

import java.io.IOException;

import com.gemstone.gemfire.internal.GfeConsoleReaderFactory.GfeConsoleReader;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

/**
 * {@link GfeConsoleReader} implementation which uses JLine's Console Reader.
 * 
 * Using the default {@link GfeConsoleReader} which uses {@link java.io.Console}
 * makes the shell repeat the characters twice.
 * 
 * TODO - Abhishek: Investigate if stty settings can avoid this?
 * 
 * @author Abhishek Chaudhari
 * @since 7.0.1
 */
public class GfshConsoleReader extends GfeConsoleReader {
  private Gfsh gfsh;
  
  public GfshConsoleReader() {
    gfsh = Gfsh.getCurrentInstance();
  }
  
  public boolean isSupported() {
    return gfsh != null && !gfsh.isHeadlessMode();
  }
  
  public String readLine(String textToPrompt) {
    String lineRead = null;
    if (isSupported()) {
      try {
        lineRead = gfsh.interact(textToPrompt);
      } catch (IOException e) {
        lineRead = null;
      }
    }
    return lineRead;
  }
  
  public char[] readPassword(String textToPrompt) {
    char[] password = null;
    if (isSupported()) {
      try {
        String passwordString = gfsh.readWithMask(textToPrompt, '*');
        password = passwordString.toCharArray();
      } catch (IOException e) {
        password = null;
      }
    }
    return password;
  }
}

//package com.gemstone.gemfire.management.internal.cli.util;
//
//import com.gemstone.gemfire.internal.GfeConsoleReaderFactory.GfeConsoleReader;
//
//public class GfshConsoleReader extends GfeConsoleReader {
//
//  public boolean isSupported() {
//    return false;
//  }
//
//  public String readLine(String textToPrompt) {
//    throw new UnsupportedOperationException("readLine() not supported for GfshConsoleReader");
//  }
//
//  public char[] readPassword(String textToPrompt) {
//    throw new UnsupportedOperationException("readPassword() not supported for GfshConsoleReader");
//  }
//}