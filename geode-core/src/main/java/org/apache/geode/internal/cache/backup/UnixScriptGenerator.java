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

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;

class UnixScriptGenerator implements ScriptGenerator {

  private static final String SCRIPT_FILE_NAME = "restore.sh";

  @Override
  public void writePreamble(final BufferedWriter writer) throws IOException {
    writer.write("#!/bin/bash -e");
    writer.newLine();
    writer.write("cd `dirname $0`");
    writer.newLine();
  }

  @Override
  public void writeComment(final BufferedWriter writer, final String string) throws IOException {
    writer.write("# " + string);
    writer.newLine();
  }

  @Override
  public void writeCopyDirectoryContents(final BufferedWriter writer, final File backup,
      final File original, final boolean backupHasFiles) throws IOException {
    if (backupHasFiles) {
      writer.write("mkdir -p '" + original + "'");
      writer.newLine();
      writer.write("cp -rp '" + backup + "'/* '" + original + "'");
      writer.newLine();
    }
  }

  @Override
  public void writeCopyFile(final BufferedWriter writer, final File backup, final File original)
      throws IOException {
    writer.write("mkdir -p '" + original.getParent() + "'");
    writer.newLine();
    writer.write("cp -p '" + backup + "' '" + original + "'");
    writer.newLine();
  }

  @Override
  public void writeExistenceTest(final BufferedWriter writer, final File file) throws IOException {
    writer.write("test -e '" + file + "' && echo '" + RestoreScript.REFUSE_TO_OVERWRITE_MESSAGE
        + file + "' && exit 1 ");
    writer.newLine();
  }

  @Override
  public void writeExit(final BufferedWriter writer) {
    // do nothing
  }

  @Override
  public String getScriptName() {
    return SCRIPT_FILE_NAME;
  }
}
