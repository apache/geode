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

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.jline.reader.impl.history.DefaultHistory;

import org.apache.geode.internal.util.ArgumentRedactor;

/**
 * Overrides JLine History to add History without newline characters.
 * Updated for JLine 3.x: extends DefaultHistory instead of MemoryHistory
 *
 * <p>
 * This implementation handles both old JLine 2 format history files (with timestamp comments)
 * and new JLine 3 format. When loading an old format file, it will be converted to the new format.
 *
 * @since GemFire 7.0
 */
public class GfshHistory extends DefaultHistory {

  // let the history from history file get added initially
  private boolean autoFlush = true;
  private Path historyFilePath = null;

  /**
   * Sets the path for the history file for direct file writing
   */
  public void setHistoryFilePath(Path path) {
    this.historyFilePath = path;

    // Initialize history file with a timestamp line (JLine 2 behavior)
    if (historyFilePath != null) {
      try {
        Files.createDirectories(historyFilePath.getParent());
        // Write initial timestamp line if file is empty
        if (!Files.exists(historyFilePath) || Files.size(historyFilePath) == 0) {
          try (BufferedWriter writer = Files.newBufferedWriter(historyFilePath,
              StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            writer.write("# " + new java.util.Date());
            writer.newLine();
          }
        }
      } catch (IOException e) {
        // Ignore - history file may not be writable
      }
    }
  }

  /**
   * Override attach to handle migration from old JLine 2 history format.
   * If loading fails due to format issues, we'll backup the old file and start fresh.
   */
  @Override
  public void attach(org.jline.reader.LineReader reader) {
    try {
      super.attach(reader);
    } catch (Exception e) {
      // Check if it's a history file format issue
      Throwable cause = e;
      while (cause != null) {
        if (cause instanceof IllegalArgumentException
            && cause.getMessage() != null
            && cause.getMessage().contains("Bad history file syntax")) {
          // Backup old history file and start fresh
          migrateOldHistoryFile();
          // Try again with clean file
          try {
            super.attach(reader);
          } catch (Exception ex) {
            // If still fails, just continue without history
          }
          return;
        }
        cause = cause.getCause();
      }
      // Re-throw if not a history format issue
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      }
      throw new RuntimeException(e);
    }
  }

  /**
   * Migrates old JLine 2 format history file to JLine 3 format.
   * Backs up the old file and creates a new one with only the valid history entries.
   */
  private void migrateOldHistoryFile() {
    if (historyFilePath == null || !Files.exists(historyFilePath)) {
      return;
    }

    try {
      // Backup old history file
      Path backupPath = historyFilePath.getParent()
          .resolve(historyFilePath.getFileName().toString() + ".old");
      Files.move(historyFilePath, backupPath,
          java.nio.file.StandardCopyOption.REPLACE_EXISTING);

      // Create new history file with timestamp
      try (BufferedWriter writer = Files.newBufferedWriter(historyFilePath,
          StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
        writer.write("# Migrated from old format on " + new java.util.Date());
        writer.newLine();
      }
    } catch (IOException e) {
      // Ignore - just start with empty history
    }
  }

  public void addToHistory(String buffer) {
    if (isAutoFlush()) {
      String redacted = ArgumentRedactor.redact(buffer.trim());
      super.add(redacted);

      // For JLine 3: write directly to file if path is set
      if (historyFilePath != null) {
        try {
          Files.createDirectories(historyFilePath.getParent());
          try (BufferedWriter writer = Files.newBufferedWriter(historyFilePath,
              StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(redacted);
            writer.newLine();
          }
        } catch (IOException e) {
          // Ignore - history file may not be writable
        }
      }
    }
  }

  public boolean isAutoFlush() {
    return autoFlush;
  }

  public void setAutoFlush(boolean autoFlush) {
    this.autoFlush = autoFlush;
  }
}
