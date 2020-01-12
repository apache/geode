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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.AbstractLauncher.ServiceState;
import org.apache.geode.internal.process.ControlFileWatchdog.ControlRequestHandler;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Creates the {@link PidFile} and uses {@link ControlFileWatchdog} to monitor the directory for
 * creation of stop or status request files.
 *
 * @since GemFire 8.0
 */
public class FileControllableProcess implements ControllableProcess {
  private static final Logger logger = LogService.getLogger();

  private final File directory;
  private final LocalProcessLauncher launcher;
  private final ControlFileWatchdog stopRequestFileWatchdog;
  private final ControlFileWatchdog statusRequestFileWatchdog;

  public FileControllableProcess(final ControlNotificationHandler handler, final File directory,
      final ProcessType processType, final boolean force)
      throws FileAlreadyExistsException, IOException, PidUnavailableException {
    this(directory, processType, force, createPidFile(directory, processType),
        createStopHandler(handler), createStatusHandler(handler, directory, processType));
  }

  private FileControllableProcess(final File directory, final ProcessType processType,
      final boolean force, final File pidFile, final ControlRequestHandler stopHandler,
      final ControlRequestHandler statusHandler)
      throws FileAlreadyExistsException, IOException, PidUnavailableException {
    this(directory, processType, createLocalProcessLauncher(pidFile, force),
        createStopRequestFileWatchdog(directory, processType, stopHandler),
        createStatusRequestFileWatchdog(directory, processType, statusHandler));
  }

  FileControllableProcess(final File directory, final ProcessType processType,
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

    deleteFiles(directory, processType);
    stopRequestFileWatchdog.start();
    statusRequestFileWatchdog.start();
  }

  /**
   * Returns the process id (PID).
   *
   * @return the process id (PID)
   */
  @Override
  public int getPid() {
    return launcher.getPid();
  }

  /**
   * Returns the PID file.
   *
   * @return the PID file
   */
  @Override
  public File getPidFile() {
    return launcher.getPidFile();
  }

  @Override
  public File getDirectory() {
    return directory;
  }

  @Override
  public void stop() {
    boolean interrupted = false;
    try {
      interrupted = stop(statusRequestFileWatchdog);
      interrupted = stop(stopRequestFileWatchdog) || interrupted;
      launcher.close();
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void stop(final boolean deletePidFileOnStop) {
    boolean interrupted = false;
    try {
      interrupted = stop(statusRequestFileWatchdog);
      interrupted = stop(stopRequestFileWatchdog) || interrupted;
      launcher.close(deletePidFileOnStop);
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
    try {
      deleteFileWithValidation(new File(directory, processType.getStatusRequestFileName()),
          "statusRequestFile");
      deleteFileWithValidation(new File(directory, processType.getStatusFileName()), "statusFile");
      deleteFileWithValidation(new File(directory, processType.getStopRequestFileName()),
          "stopRequestFile");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
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
      writeStatusToFile(fetchStatusWithValidation(handler), directory, processType);
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

  static String fetchStatusWithValidation(final ControlNotificationHandler handler) {
    ServiceState<?> state = handler.handleStatus();
    if (state == null) {
      throw new IllegalStateException("Null ServiceState is invalid");
    }

    String jsonContent = state.toJson();
    if (jsonContent == null) {
      throw new IllegalStateException("Null JSON for status is invalid");
    } else if (jsonContent.trim().isEmpty()) {
      throw new IllegalStateException("Empty JSON for status is invalid");
    }

    return jsonContent;
  }

  private static void deleteFileWithValidation(final File file, final String fileNameForMessage)
      throws IOException {
    if (file.exists()) {
      if (!file.delete()) {
        throw new IOException(
            "Unable to delete " + fileNameForMessage + "'" + file.getCanonicalPath() + "'");
      }
    }
  }

  private static void writeStatusToFile(final String jsonContent, final File directory,
      final ProcessType processType) throws IOException {
    File statusFile = new File(directory, processType.getStatusFileName());
    File statusFileTmp = new File(directory, processType.getStatusFileName() + ".tmp");

    deleteFileWithValidation(statusFile, "statusFile");
    deleteFileWithValidation(statusFileTmp, "statusFileTmp");

    if (!statusFileTmp.createNewFile()) {
      throw new IOException(
          "Unable to create statusFileTmp '" + statusFileTmp.getCanonicalPath() + "'");
    }

    FileWriter writer = new FileWriter(statusFileTmp);
    writer.write(jsonContent);
    writer.flush();
    writer.close();

    if (!statusFileTmp.renameTo(statusFile)) {
      throw new IOException("Unable to rename statusFileTmp '" + statusFileTmp.getCanonicalPath()
          + "' to '" + statusFile.getCanonicalPath() + "'");
    }
  }
}
