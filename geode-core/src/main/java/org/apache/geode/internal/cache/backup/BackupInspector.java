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
package org.apache.geode.internal.cache.backup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.internal.lang.SystemUtils;

/**
 * Inspects a completed backup and parses the operation log file data from the restore script
 * produced by a previous backup.
 */
abstract class BackupInspector {

  /**
   * Maps operation log file names to script lines that copy previously backed up operation log
   * files. These lines will be added to future restore scripts if the operation logs are still
   * relevant to the member.
   */
  private final Map<String, String> oplogLineMap = new HashMap<>();

  /**
   * Contains the unique set of operation log file names contained in the restore script.
   */
  private final Set<String> oplogFileNames = new HashSet<>();

  /**
   * Returns a BackupInspector for a member's backup directory.
   *
   * @param backupDir a member's backup directory.
   * @return a new BackupInspector.
   * @throws IOException the backup directory was malformed.
   */
  static BackupInspector createInspector(final File backupDir) throws IOException {
    if (SystemUtils.isWindows()) {
      return new WindowsBackupInspector(backupDir);
    }

    return new UnixBackupInspector(backupDir);
  }

  /**
   * Creates a new BackupInspector.
   *
   * @param backupDir a a previous backup for a member.
   * @throws IOException an error occurred while parsing the restore script.
   */
  BackupInspector(final File backupDir) throws IOException {
    if (!backupDir.exists()) {
      throw new IOException("Backup directory " + backupDir.getAbsolutePath() + " does not exist.");
    }

    File restoreFile = getRestoreFile(backupDir);
    if (!restoreFile.exists()) {
      throw new IOException("Restore file " + restoreFile.getName() + " does not exist.");
    }

    try (BufferedReader reader = new BufferedReader(new FileReader(restoreFile))) {
      parseRestoreFile(reader);
    }
  }

  void addOplogLine(String filename, String line) {
    oplogFileNames.add(filename);
    oplogLineMap.put(filename, line);
  }

  String getOplogLineFromFilename(String filename) {
    return oplogLineMap.get(filename);
  }

  /**
   * Searches for the incremental backup marker and parses the incremental portion.
   *
   * @param reader restore file reader.
   */
  private void parseRestoreFile(final BufferedReader reader) throws IOException {
    boolean markerFound = false;

    String line;
    while (!markerFound && null != (line = reader.readLine())) {
      markerFound = line.contains(RestoreScript.INCREMENTAL_MARKER_COMMENT);
    }

    if (markerFound) {
      parseOplogLines(reader);
    }
  }

  /**
   * Returns true if the restore script is incremental.
   */
  public boolean isIncremental() {
    return !oplogFileNames.isEmpty();
  }

  /**
   * Returns the restore script line that restores a particular operation log file.
   *
   * @param oplogFileName an operation log file.
   */
  String getScriptLineForOplogFile(final String oplogFileName) {
    return oplogLineMap.get(oplogFileName);
  }

  /**
   * Returns the set of operation log files copied in the incremental backup section of the restore
   * script.
   */
  Set<String> getIncrementalOplogFileNames() {
    return Collections.unmodifiableSet(oplogFileNames);
  }

  /**
   * Returns the restore script for the backup.
   *
   * @param backupDir a member's backup directory.
   */
  abstract File getRestoreFile(final File backupDir);

  /**
   * Returns the copyTo operation log file path for an operation log file name.
   *
   * @param oplogFileName an operation log file.
   */
  abstract String getCopyToForOplogFile(final String oplogFileName);

  /**
   * Returns the copy from operation log file path for an operation log file name.
   *
   * @param oplogFileName an operation log file.
   */
  abstract String getCopyFromForOplogFile(final String oplogFileName);

  /**
   * Parses out operation log data from the incremental backup portion of the restore script.
   *
   * @param reader restore file reader.
   */
  abstract void parseOplogLines(final BufferedReader reader) throws IOException;
}
