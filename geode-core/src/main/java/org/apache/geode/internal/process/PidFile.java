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
package org.apache.geode.internal.process;

import static org.apache.commons.lang3.Validate.isTrue;
import static org.apache.commons.lang3.Validate.notEmpty;
import static org.apache.commons.lang3.Validate.notNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * File wrapper that adds support for reading process id (pid) from a pid file written to disk by
 * GemFire processes.
 *
 * @since GemFire 8.2
 */
public class PidFile {

  private final File pidFile;

  /**
   * Constructs a PidFile for reading pid stored in a file.
   *
   * @param file the file containing the pid of the process
   *
   * @throws IllegalArgumentException if the specified file is null or does not exist
   */
  public PidFile(final File file) {
    notNull(file, "Invalid file '" + file + "' specified");
    isTrue(file.exists(), "Nonexistent file '" + file + "' specified");

    pidFile = file;
  }

  /**
   * Constructs a PidFile for reading pid stored in a file.
   *
   * @param directory directory containing a file of name pidFileName
   * @param filename name of the file containing the pid of the process to stop
   *
   * @throws FileNotFoundException if the specified filename is not found within the directory
   * @throws IllegalArgumentException if directory is null, does not exist or is not a directory
   */
  public PidFile(final File directory, final String filename) throws FileNotFoundException {
    notNull(directory, "Invalid directory '" + directory + "' specified");
    notEmpty(filename, "Invalid filename '" + filename + "' specified");
    isTrue(directory.isDirectory() && directory.exists(),
        "Nonexistent directory '" + directory + "' specified");

    File file = new File(directory, filename);
    if (!file.exists() || file.isDirectory()) {
      throw new FileNotFoundException(
          "Unable to find PID file '" + filename + "' in directory '" + directory + "'");
    }

    pidFile = file;
  }

  /**
   * Reads in the pid from the specified file.
   *
   * @return the process id (pid) contained within the pidFile
   *
   * @throws IllegalArgumentException if the pid in the pidFile is not a positive integer
   * @throws IOException if unable to read from the specified file
   */
  public int readPid() throws IOException {
    String pidValue = null;
    try (BufferedReader fileReader = new BufferedReader(new FileReader(pidFile))) {
      pidValue = fileReader.readLine();

      int pid = Integer.parseInt(pidValue);

      if (pid < 1) {
        throw new IllegalArgumentException(
            "Invalid pid '" + pid + "' found in " + pidFile.getCanonicalPath());
      }

      return pid;
    } catch (NumberFormatException ignored) {
      throw new IllegalArgumentException(
          "Invalid pid '" + pidValue + "' found in " + pidFile.getCanonicalPath());
    }
  }

  File getFile() {
    return pidFile;
  }

}
