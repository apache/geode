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

class WindowsScriptGenerator implements ScriptGenerator {

  static final String ROBOCOPY_NO_JOB_HEADER = "/njh";
  static final String EXIT_MARKER = "Exit Functions";

  private static final String ERROR_CHECK = "IF %ERRORLEVEL% GEQ 4 GOTO Exit_Bad";
  private static final String ROBOCOPY_COMMAND = "Robocopy.exe";
  private static final String ROBOCOPY_NO_JOB_SUMMARY = "/njs";
  private static final String ROBOCOPY_COPY_SUBDIRS = "/e";
  private static final String SCRIPT_FILE_NAME = "restore.bat";
  private static final String ECHO_OFF = "echo off";
  private static final String CD_TO_SCRIPT_DIR = "cd %~dp0";
  private static final String MKDIR = "mkdir";
  private static final String EXIT_BLOCK = ":Exit_Good\nexit /B 0\n\n:Exit_Bad\nexit /B 1";

  @Override
  public void writePreamble(PrintWriter writer) {
    writer.println(ECHO_OFF);
    writer.println(CD_TO_SCRIPT_DIR);
  }

  @Override
  public void writeComment(PrintWriter writer, String string) {
    writer.println("rem " + string);
  }

  @Override
  public void writeCopyDirectoryContents(PrintWriter writer, File backup, File original,
      boolean backupHasFiles) {
    writer.println(MKDIR + " \"" + original + "\"");
    writer.println(ROBOCOPY_COMMAND + " \"" + backup + "\" \"" + original + "\" "
        + ROBOCOPY_COPY_SUBDIRS + " " + ROBOCOPY_NO_JOB_HEADER + " " + ROBOCOPY_NO_JOB_SUMMARY);
    writer.println(ERROR_CHECK);
  }

  @Override
  public void writeCopyFile(PrintWriter writer, File source, File destination) {
    String fileName = source.getName();
    String sourcePath = source.getParent() == null ? "." : source.getParent();
    String destinationPath = destination.getParent() == null ? "." : destination.getParent();
    writer.println(ROBOCOPY_COMMAND + " \"" + sourcePath + "\" \"" + destinationPath + "\" "
        + fileName + " " + ROBOCOPY_NO_JOB_HEADER + " " + ROBOCOPY_NO_JOB_SUMMARY);
    writer.println(ERROR_CHECK);
  }

  @Override
  public void writeExistenceTest(PrintWriter writer, File file) {
    writer.println("IF EXIST \"" + file + "\" echo \"" + RestoreScript.REFUSE_TO_OVERWRITE_MESSAGE
        + file + "\" && exit /B 1 ");
  }

  @Override
  public void writeExit(PrintWriter writer) {
    writeComment(writer, WindowsScriptGenerator.EXIT_MARKER);
    writer.println(EXIT_BLOCK);
  }

  @Override
  public String getScriptName() {
    return SCRIPT_FILE_NAME;
  }
}
