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

import static org.apache.commons.lang.Validate.notNull;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.AbstractLauncher.ServiceState;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.process.ControlFileWatchdog.ControlRequestHandler;

/**
 * Creates the {@link PidFile} and uses {@link ControlFileWatchdog} to monitor the directory for
 * creation of stop or status request files.
 * 
 * @since GemFire 8.0
 */
public class ControllableProcess {
  private static final Logger logger = LogService.getLogger();

  private final File directory;
  private final LocalProcessLauncher launcher;
  private final ControlFileWatchdog stopRequestFileWatchdog;
  private final ControlFileWatchdog statusRequestFileWatchdog;

  public ControllableProcess(final ControlNotificationHandler handler, final File directory,
      final ProcessType processType, final boolean force)
      throws FileAlreadyExistsException, IOException, PidUnavailableException {
    this(directory, processType, force, createPidFile(directory, processType),
        createStopHandler(handler), createStatusHandler(handler, directory, processType));
  }

  private ControllableProcess(final File directory, final ProcessType processType,
      final boolean force, final File pidFile, final ControlRequestHandler stopHandler,
      final ControlRequestHandler statusHandler)
      throws FileAlreadyExistsException, IOException, PidUnavailableException {
    this(directory, processType, createLocalProcessLauncher(pidFile, force),
        createStopRequestFileWatchdog(directory, processType, stopHandler),
        createStatusRequestFileWatchdog(directory, processType, statusHandler));
  }

  ControllableProcess(final File directory, final ProcessType processType,
      final LocalProcessLauncher launcher, final ControlFileWatchdog stopRequestFileWatchdog,
      final ControlFileWatchdog statusRequestFileWatchdog) {
    notNull(directory, "Invalid directory '" + directory + "' specified");
    notNull(processType, "Invalid processType '" + processType + "' specified");
    notNull(launcher, "Invalid launcher '" + launcher + "' specified");
    notNull(stopRequestFileWatchdog,
        "Invalid stopRequestFileWatchdog '" + stopRequestFileWatchdog + "' specified");
    notNull(statusRequestFileWatchdog,
        "Invalid statusRequestFileWatchdog '" + statusRequestFileWatchdog + "' specified");

    this.directory = directory;
    this.launcher = launcher;
    this.stopRequestFileWatchdog = stopRequestFileWatchdog;
    this.statusRequestFileWatchdog = statusRequestFileWatchdog;

    deleteFiles(this.directory, processType);
    this.stopRequestFileWatchdog.start();
    this.statusRequestFileWatchdog.start();
  }

  /**
   * Returns the process id (PID).
   * 
   * @return the process id (PID)
   */
  public int getPid() {
    return this.launcher.getPid();
  }

  /**
   * Returns the PID file.
   * 
   * @return the PID file
   */
  public File getPidFile() {
    return this.launcher.getPidFile();
  }

  public File getDirectory() {
    return this.directory;
  }

  public void stop() {
    boolean interrupted = false;
    try {
      interrupted = stop(this.statusRequestFileWatchdog);
      interrupted = stop(this.stopRequestFileWatchdog) || interrupted;
      this.launcher.close();
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public void stop(final boolean deletePidFileOnStop) {
    boolean interrupted = false;
    try {
      interrupted = stop(this.statusRequestFileWatchdog);
      interrupted = stop(this.stopRequestFileWatchdog) || interrupted;
      this.launcher.close(deletePidFileOnStop);
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private boolean stop(final ControlFileWatchdog fileWatchdog) {
    boolean interrupted = false;
    try {
      fileWatchdog.stop();
    } catch (InterruptedException e) {
      interrupted = true;
      logger.warn("Interrupted while stopping status handler for controllable process.", e);
    }
    return interrupted;
  }

  private void deleteFiles(final File directory, final ProcessType processType) {
    deleteFile(directory, processType.getStatusRequestFileName());
    deleteFile(directory, processType.getStatusFileName());
    deleteFile(directory, processType.getStopRequestFileName());
  }

  private void deleteFile(final File directory, final String fileName) {
    File file = new File(directory, fileName);
    if (file.exists()) {
      file.delete();
    }
  }

  private static File createPidFile(final File directory, final ProcessType processType) {
    return new File(directory, processType.getPidFileName());
  }

  private static LocalProcessLauncher createLocalProcessLauncher(final File pidFile,
      final boolean force) throws FileAlreadyExistsException, IOException, PidUnavailableException {
    return new LocalProcessLauncher(pidFile, force);
  }

  private static ControlRequestHandler createStopHandler(final ControlNotificationHandler handler) {
    return handler::handleStop;
  }

  private static ControlRequestHandler createStatusHandler(final ControlNotificationHandler handler,
      final File directory, final ProcessType processType) {
    return () -> {
      ServiceState<?> state = handler.handleStatus();

      File statusFile = new File(directory, processType.getStatusFileName());
      if (statusFile.exists()) {
        boolean deleted = statusFile.delete();
        assert deleted;
      }

      File statusFileTmp = new File(directory, processType.getStatusFileName() + ".tmp");
      if (statusFileTmp.exists()) {
        boolean deleted = statusFileTmp.delete();
        assert deleted;
      }

      boolean created = statusFileTmp.createNewFile();
      assert created;

      FileWriter writer = new FileWriter(statusFileTmp);
      writer.write(state.toJson());
      writer.flush();
      writer.close();

      boolean renamed = statusFileTmp.renameTo(statusFile);
      assert renamed;
    };
  }

  private static ControlFileWatchdog createStopRequestFileWatchdog(final File directory,
      final ProcessType processType, final ControlRequestHandler stopHandler) {
    return new ControlFileWatchdog(directory, processType.getStopRequestFileName(), stopHandler,
        false);
  }

  private static ControlFileWatchdog createStatusRequestFileWatchdog(final File directory,
      final ProcessType processType, final ControlRequestHandler statusHandler) {
    return new ControlFileWatchdog(directory, processType.getStatusRequestFileName(), statusHandler,
        false);
  }
}
