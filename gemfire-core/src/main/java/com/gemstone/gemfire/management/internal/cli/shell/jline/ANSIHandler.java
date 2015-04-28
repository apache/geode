/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.shell.jline;

import org.springframework.shell.core.JLineLogHandler;

import jline.ANSIBuffer;

/**
 * Overrides jline.History to add History without newline characters.
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class ANSIHandler {
  private static ANSIHandler instance;

  private boolean isAnsiEnabled;
  
  public ANSIHandler(boolean isAnsiEnabled) {
    this.isAnsiEnabled = isAnsiEnabled;
  }
  
  public static ANSIHandler getInstance(boolean isAnsiSupported) {
    if (instance == null) {
      instance = new ANSIHandler(isAnsiSupported);
    }
    return instance;
  }
  
  public boolean isAnsiEnabled() {
    return isAnsiEnabled;
  }

  public String decorateString(String input, ANSIStyle...styles) {
    String decoratedInput = input;
    
    if (isAnsiEnabled()) {
      ANSIBuffer ansiBuffer = JLineLogHandler.getANSIBuffer();

      for (ANSIStyle ansiStyle : styles) {
        switch (ansiStyle) {
        case RED:
          ansiBuffer.red(input);
          break;
        case BLUE:
          ansiBuffer.blue(input);
          break;
        case GREEN:
          ansiBuffer.green(input);
          break;
        case BLACK:
          ansiBuffer.black(input);
          break;
        case YELLOW:
          ansiBuffer.yellow(input);
          break;
        case MAGENTA:
          ansiBuffer.magenta(input);
          break;
        case CYAN:
          ansiBuffer.cyan(input);
          break;
        case BOLD:
          ansiBuffer.bold(input);
          break;
        case UNDERSCORE:
          ansiBuffer.underscore(input);
          break;
        case BLINK:
          ansiBuffer.blink(input);
          break;
        case REVERSE:
          ansiBuffer.reverse(input);
          break;
        default:
          break;
        }
      }
      
      decoratedInput = ansiBuffer.toString();
    }
    
    return decoratedInput;
  }
  
  public static enum ANSIStyle {
    RED, 
    BLUE, 
    GREEN, 
    BLACK, 
    YELLOW, 
    MAGENTA, 
    CYAN, 
    BOLD, 
    UNDERSCORE, 
    BLINK, 
    REVERSE;   
  }
}
