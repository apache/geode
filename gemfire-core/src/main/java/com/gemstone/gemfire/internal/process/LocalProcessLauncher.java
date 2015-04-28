/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.process;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Creates a pid file and writes the process id to the pid file.
 * <p/>
 * Related articles and libraries:
 * <ul>
 * <li>http://barelyenough.org/blog/2005/03/java-daemon/
 * <li>http://stackoverflow.com/questions/534648/how-to-daemonize-a-java-program
 * <li>http://commons.apache.org/daemon/
 * <li>http://wrapper.tanukisoftware.com/
 * <li>http://weblogs.java.net/blog/kohsuke/archive/2009/01/writing_a_unix.html
 * <li>http://www.enderunix.org/docs/eng/daemon.php
 * 
 * @author Kirk Lund
 * @since 7.0
 */
public final class LocalProcessLauncher {

  public static final String PROPERTY_IGNORE_IS_PID_ALIVE = "gemfire.test.LocalProcessLauncher.ignoreIsPidAlive";
  
  private final int pid;
  private final File pidFile;
  
  /**
   * Constructs a new ProcessLauncher. Parses this process's RuntimeMXBean name 
   * for the pid (process id).
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
  public LocalProcessLauncher(final File pidFile, final boolean force) 
      throws FileAlreadyExistsException, IOException, PidUnavailableException {
    this.pid = ProcessUtils.identifyPid();
    this.pidFile = pidFile;
    writePid(force);
  }
  
  /**
   * Returns the process id (pid).
   * 
   * @return the process id (pid)
   */
  public int getPid() {
    return this.pid;
  }
  
  /**
   * Returns the pid file.
   * 
   * @return the pid file
   */
  public File getPidFile() {
    return this.pidFile;
  }
  
  /**
   * Delete the pid file now. {@link java.io.File#deleteOnExit()} is set on the pid file.
   */
  void close() {
    this.pidFile.delete();
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
    final boolean created = this.pidFile.createNewFile();
    if (!created && !force) {
      int otherPid = 0;
      try {
        otherPid = ProcessUtils.readPid(this.pidFile);
      } catch(IOException e) {
        // suppress
      } catch (NumberFormatException e) {
        // suppress
      }
      boolean ignorePidFile = false;
      if (otherPid != 0 && !ignoreIsPidAlive()) {
        ignorePidFile = !ProcessUtils.isProcessAlive(otherPid);
      }
      if (!ignorePidFile) {
        throw new FileAlreadyExistsException("Pid file already exists: " + this.pidFile + 
            " for " + (otherPid > 0 ? "process " + otherPid : "unknown process"));
      }
    }
    this.pidFile.deleteOnExit();
    final FileWriter writer = new FileWriter(this.pidFile);
    writer.write(String.valueOf(this.pid));
    writer.flush();
    writer.close();
  }
  
  private static boolean ignoreIsPidAlive() {
    return Boolean.getBoolean(PROPERTY_IGNORE_IS_PID_ALIVE);
  }
}
