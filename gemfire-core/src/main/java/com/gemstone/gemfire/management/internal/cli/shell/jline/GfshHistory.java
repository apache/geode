/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.shell.jline;

import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.PreprocessorUtils;

import jline.History;

/**
 * Overrides jline.History to add History without newline characters.
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class GfshHistory extends History {
  // let the history from history file get added initially
  private boolean autoFlush = true;
  
  @Override
  public void addToHistory(String buffer) {
    if (isAutoFlush()) {
      super.addToHistory(toHistoryLoggable(buffer));
    }
  }

  public boolean isAutoFlush() {
    return autoFlush;
  }

  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
  }
  
  public static String toHistoryLoggable(String buffer) {
    return PreprocessorUtils.trim(buffer, false).getString();
  }
}
