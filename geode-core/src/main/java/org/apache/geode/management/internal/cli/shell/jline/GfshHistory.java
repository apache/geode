/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.cli.shell.jline;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.PreprocessorUtils;

import jline.console.history.MemoryHistory;

/**
 * Overrides jline.History to add History without newline characters.
 * 
 * @since GemFire 7.0
 */
public class GfshHistory extends MemoryHistory {

  // Pattern which is intended to pick up any params containing the word 'password'.
  private static final Pattern passwordRe = Pattern.compile("(--[^=\\s]*password[^=\\s]*\\s*=\\s*)([^\\s]*)");

  // let the history from history file get added initially
  private boolean autoFlush = true;

  public void addToHistory(String buffer) {
    if (isAutoFlush()) {
      super.add(redact(buffer));
    }
  }

  public boolean isAutoFlush() {
    return autoFlush;
  }

  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
  }
  
  public static String redact(String buffer) {
    String trimmed = PreprocessorUtils.trim(buffer, false).getString();

    Matcher matcher = passwordRe.matcher(trimmed);
    String sanitized = matcher.replaceAll("$1*****");
    return sanitized;
  }
}
