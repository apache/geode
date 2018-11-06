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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang.Validate.notNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.geode.internal.process.ControlFileWatchdog.ControlRequestHandler;
import org.apache.geode.lang.AttachAPINotFoundException;

/**
 * Controls a {@link ControllableProcess} using files to communicate between processes.
 *
 * @since GemFire 8.0
 */
class FileProcessController implements ProcessController {

  static final long DEFAULT_STATUS_TIMEOUT_MILLIS = 60 * 1000;

  private final long statusTimeoutMillis;
  private final FileControllerParameters parameters;
  private final int pid;

  /**
   * Constructs an instance for controlling a local process.
   *
   * @param parameters details about the controllable process
   * @param pid process id identifying the process to control
   *
   * @throws IllegalArgumentException if pid is not a positive integer
   */
  FileProcessController(final FileControllerParameters parameters, final int pid) {
    this(parameters, pid, DEFAULT_STATUS_TIMEOUT_MILLIS, MILLISECONDS);
  }

  /**
   * Constructs an instance for controlling a local process.
   *
   * @param parameters details about the controllable process
   * @param pid process id identifying the process to control
   * @param timeout the timeout that operations must complete within
   * @param units the units of the timeout
   *
   * @throws IllegalArgumentException if pid is not a positive integer
   */
  FileProcessController(final FileControllerParameters parameters, final int pid,
      final long timeout, final TimeUnit units) {
    notNull(parameters, "Invalid parameters '" + parameters + "' specified");
    isTrue(pid > 0, "Invalid pid '" + pid + "' specified");
    isTrue(timeout >= 0, "Invalid timeout '" + timeout + "' specified");
    notNull(units, "Invalid units '" + units + "' specified");

    this.pid = pid;
    this.parameters = parameters;
    this.statusTimeoutMillis = units.toMillis(timeout);
  }

  @Override
  public int getProcessId() {
    return pid;
  }

  @Override
  public String status()
      throws UnableToControlProcessException, IOException, InterruptedException, TimeoutException {
    return status(parameters.getDirectory(), parameters.getProcessType().getStatusRequestFileName(),
        parameters.getProcessType().getStatusFileName());
  }

  @Override
  public void stop() throws UnableToControlProcessException, IOException {
    stop(parameters.getDirectory(), parameters.getProcessType().getStopRequestFileName());
  }

  @Override
  public void checkPidSupport() {
    throw new AttachAPINotFoundException(
        "The Attach API classes could not be found on the classpath.  Please include JDK tools.jar on the classpath or add the JDK tools.jar to the jre/lib/ext directory.");
  }

  long getStatusTimeoutMillis() {
    return statusTimeoutMillis;
  }

  private void stop(final File workingDir, final String stopRequestFileName) throws IOException {
    File stopRequestFile = new File(workingDir, stopRequestFileName);
    if (!stopRequestFile.exists()) {
      stopRequestFile.createNewFile();
    }
  }

  private String status(final File workingDir, final String statusRequestFileName,
      final String statusFileName) throws IOException, InterruptedException, TimeoutException {
    // monitor for statusFile
    File statusFile = new File(workingDir, statusFileName);
    AtomicReference<String> statusRef = new AtomicReference<>();

    ControlRequestHandler statusHandler = () -> {
      // read the statusFile
      StringBuilder lines = new StringBuilder();
      try (BufferedReader reader = new BufferedReader(new FileReader(statusFile))) {
        reader.lines().forEach(lines::append);
      } finally {
        statusRef.set(lines.toString());
      }
    };

    ControlFileWatchdog statusFileWatchdog =
        new ControlFileWatchdog(workingDir, statusFileName, statusHandler, true);
    statusFileWatchdog.start();

    File statusRequestFile = new File(workingDir, statusRequestFileName);
    if (!statusRequestFile.exists()) {
      statusRequestFile.createNewFile();
    }

    // if timeout invoke stop and then throw TimeoutException
    long start = System.currentTimeMillis();
    while (statusFileWatchdog.isAlive()) {
      Thread.sleep(10);
      if (System.currentTimeMillis() >= start + statusTimeoutMillis) {
        statusFileWatchdog.stop();
        throw new TimeoutException("Timed out waiting for process to create " + statusFile);
      }
    }

    String lines = statusRef.get();
    if (isBlank(lines)) {
      throw new IllegalStateException("Status file '" + statusFile + "' is blank");
    }
    return lines;
  }
}
