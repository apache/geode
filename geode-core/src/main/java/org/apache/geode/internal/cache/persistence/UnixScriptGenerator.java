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
package org.apache.geode.internal.cache.persistence;

import java.io.File;
import java.io.PrintWriter;

class UnixScriptGenerator implements ScriptGenerator {

  private static final String SCRIPT_FILE_NAME = "restore.sh";

  public void writePreamble(final PrintWriter writer) {
    writer.println("#!/bin/bash -e");
    writer.println("cd `dirname $0`");
  }

  public void writeComment(final PrintWriter writer, final String string) {
    writer.println("# " + string);
  }

  public void writeCopyDirectoryContents(final PrintWriter writer, final File backup,
      final File original, final boolean backupHasFiles) {
    writer.println("mkdir -p '" + original + "'");
    if (backupHasFiles) {
      writer.println("cp -rp '" + backup + "'/* '" + original + "'");
    }
  }

  public void writeCopyFile(final PrintWriter writer, final File backup, final File original) {
    writer.println("cp -p '" + backup + "' '" + original + "'");
  }

  public void writeExistenceTest(final PrintWriter writer, final File file) {
    writer.println("test -e '" + file + "' && echo '" + RestoreScript.REFUSE_TO_OVERWRITE_MESSAGE
        + file + "' && exit 1 ");
  }

  public void writeExit(final PrintWriter writer) {
    // do nothing
  }

  @Override
  public String getScriptName() {
    return SCRIPT_FILE_NAME;
  }
}
