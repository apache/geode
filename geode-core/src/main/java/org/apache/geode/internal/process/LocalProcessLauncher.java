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

import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.geode.internal.process.ProcessUtils.identifyPid;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.geode.util.internal.GeodeGlossary;

/**
 * Creates a pid file and writes the process id to the pid file.
 *
 * <p>
 * Related articles and libraries:
 *
 * <ul>
 * <li>http://barelyenough.org/blog/2005/03/java-daemon/
 * <li>http://stackoverflow.com/questions/534648/how-to-daemonize-a-java-program
 * <li>http://commons.apache.org/daemon/
 * <li>http://wrapper.tanukisoftware.com/
 * <li>http://weblogs.java.net/blog/kohsuke/archive/2009/01/writing_a_unix.html
 * <li>http://www.enderunix.org/docs/eng/daemon.php
 * </ul>
 *
 * @since GemFire 7.0
 */
class LocalProcessLauncher {

  static final String PROPERTY_IGNORE_IS_PID_ALIVE =
      GeodeGlossary.GEMFIRE_PREFIX + "test.LocalProcessLauncher.ignoreIsPidAlive";

  private final int pid;
  private final File pidFile;

  /**
   * Constructs a new ProcessLauncher. Parses this process's RuntimeMXBean name for the pid (process
   * id).
   *
   * @param pidFile the file to create and write pid into
   * @param force if true then the pid file will be replaced if it already exists
   *
   * @throws FileAlreadyExistsException if the pid file already exists and force is false
   * @throws IOException if unable to write pid (process id) to pid file
   * @throws PidUnavailableException if the pid cannot be parsed from the RuntimeMXBean name
   *
   * @see java.lang.management.RuntimeMXBean
   */
  LocalProcessLauncher(final File pidFile, final boolean force)
      throws FileAlreadyExistsException, IOException, PidUnavailableException {
    notNull(pidFile, "Invalid pidFile '" + pidFile + "' specified");

    pid = identifyPid();
    this.pidFile = pidFile;
    writePid(force);
  }

  /**
   * Returns the process id (pid).
   *
   * @return the process id (pid)
   */
  int getPid() {
    return pid;
  }

  /**
   * Returns the pid file.
   *
   * @return the pid file
   */
  File getPidFile() {
    return pidFile;
  }

  /**
   * Delete the pid file now. {@link java.io.File#deleteOnExit()} is set on the pid file.
   *
   */
  void close() {
    pidFile.delete();
  }

  /**
   * Delete the pid file now. {@link java.io.File#deleteOnExit()} is set on the pid file.
   *
   * @param deletePidFileOnClose if true then the pid file will be deleted now instead of during JVM
   *        exit
   */
  void close(final boolean deletePidFileOnClose) {
    if (deletePidFileOnClose) {
      pidFile.delete();
    }
  }

  /**
   * Creates a new pid file and writes this process's pid into it.
   *
   * @param force if true then the pid file will be replaced if it already exists it
   *
   * @throws FileAlreadyExistsException if the pid file already exists and force is false
   * @throws IOException if unable to create or write to the file
   */
  private void writePid(final boolean force) throws FileAlreadyExistsException, IOException {
    if (pidFile.exists()) {
      if (!force) {
        checkOtherPid(readOtherPid());
      }
      pidFile.delete();
    }

    File tempPidFile = new File(pidFile.getParent(), pidFile.getName() + ".tmp");
    tempPidFile.createNewFile();

    try (FileWriter writer = new FileWriter(tempPidFile)) {
      writer.write(String.valueOf(pid));
      writer.flush();
    }

    tempPidFile.renameTo(pidFile);
    pidFile.deleteOnExit();
  }

  private int readOtherPid() {
    int otherPid = 0;
    try {
      otherPid = ProcessUtils.readPid(pidFile);
    } catch (NumberFormatException | IOException ignore) {
      // suppress
    }
    return otherPid;
  }

  private void checkOtherPid(final int otherPid) throws FileAlreadyExistsException {
    if (ignoreIsPidAlive() || otherPid != 0 && isProcessAlive(otherPid)) {
      throw new FileAlreadyExistsException("Pid file already exists: " + pidFile + " for "
          + (otherPid > 0 ? "process " + otherPid : "unknown process"));
    }
  }

  private boolean isProcessAlive(final int pid) {
    return ignoreIsPidAlive() || ProcessUtils.isProcessAlive(pid);
  }

  private static boolean ignoreIsPidAlive() {
    return Boolean.getBoolean(PROPERTY_IGNORE_IS_PID_ALIVE);
  }
}
