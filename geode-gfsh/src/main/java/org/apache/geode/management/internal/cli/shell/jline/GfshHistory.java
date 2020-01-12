/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.cli.shell.jline;

import jline.console.history.MemoryHistory;

import org.apache.geode.internal.util.ArgumentRedactor;

/**
 * Overrides jline.History to add History without newline characters.
 *
 * @since GemFire 7.0
 */
public class GfshHistory extends MemoryHistory {

  // let the history from history file get added initially
  private boolean autoFlush = true;

  public void addToHistory(String buffer) {
    if (isAutoFlush()) {
      super.add(ArgumentRedactor.redact(buffer.trim()));
    }
  }

  public boolean isAutoFlush() {
    return autoFlush;
  }

  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
  }
}
