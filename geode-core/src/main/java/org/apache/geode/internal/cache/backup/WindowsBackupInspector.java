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
import java.io.IOException;

/**
 * A BackupInspector for the Windows platform(s).
 */
class WindowsBackupInspector extends BackupInspector {

  /**
   * Restore file for windows platform.
   */
  private static final String RESTORE_FILE = "restore.bat";

  WindowsBackupInspector(final File backupDir) throws IOException {
    super(backupDir);
  }

  @Override
  public String getCopyFromForOplogFile(final String oplogFileName) {
    String line = getOplogLineFromFilename(oplogFileName);
    if (line == null) {
      return null;
    }

    String[] parts = line.split("\\s");
    return parts[1].substring(1, parts[1].length() - 1) + File.separator + parts[3];
  }

  @Override
  public String getCopyToForOplogFile(final String oplogFileName) {
    String line = getOplogLineFromFilename(oplogFileName);
    if (line == null) {
      return null;
    }

    String[] parts = line.split("\\s");
    return parts[2].substring(1, parts[2].length() - 1) + File.separator + parts[3];
  }

  @Override
  void parseOplogLines(final BufferedReader reader) throws IOException {
    String line;

    int beginIndex, endIndex;
    String oplogName;
    while (null != (line = reader.readLine())) {
      if (line.startsWith("IF")) {
        // skip IF statements as they have oplog file exists checks.
        continue;
      } else if (line.contains(WindowsScriptGenerator.EXIT_MARKER)) {
        break;
      } else {
        beginIndex = line.lastIndexOf("\"") + 1;
        endIndex = line.indexOf(WindowsScriptGenerator.ROBOCOPY_NO_JOB_HEADER, beginIndex) - 1;
        oplogName = line.substring(beginIndex, endIndex).trim();
        addOplogLine(oplogName, line);
      }
    }
  }

  @Override
  File getRestoreFile(final File backupDir) {
    return new File(backupDir, RESTORE_FILE);
  }
}
