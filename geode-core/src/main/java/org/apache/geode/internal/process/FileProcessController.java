/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.process;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.process.ControlFileWatchdog.ControlRequestHandler;
import org.apache.geode.lang.AttachAPINotFoundException;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Controls a {@link ControllableProcess} using files to communicate between
 * processes.
 * 
 * @since GemFire 8.0
 */
public class FileProcessController implements ProcessController {
  private static final Logger logger = LogService.getLogger();

  public static final String STATUS_TIMEOUT_PROPERTY = DistributionConfig.GEMFIRE_PREFIX + "FileProcessController.STATUS_TIMEOUT";
  
  private final long statusTimeoutMillis;
  private final FileControllerParameters arguments;
  private final int pid;

  /**
   * Constructs an instance for controlling a local process.
   * 
   * @param arguments details about the controllable process
   * @param pid process id identifying the process to control
   * 
   * @throws IllegalArgumentException if pid is not a positive integer
   */
  public FileProcessController(final FileControllerParameters arguments, final int pid) {
    this(arguments, pid, Long.getLong(STATUS_TIMEOUT_PROPERTY, 60*1000), TimeUnit.MILLISECONDS);
  }

  /**
   * Constructs an instance for controlling a local process.
   * 
   * @param arguments details about the controllable process
   * @param pid process id identifying the process to control
   * @param timeout the timeout that operations must complete within
   * @param units the units of the timeout
   * 
   * @throws IllegalArgumentException if pid is not a positive integer
   */
  public FileProcessController(final FileControllerParameters arguments, final int pid, final long timeout, final TimeUnit units) {
    if (pid < 1) {
      throw new IllegalArgumentException("Invalid pid '" + pid + "' specified");
    }
    this.pid = pid;
    this.arguments = arguments;
    this.statusTimeoutMillis = units.toMillis(timeout);
  }

  @Override
  public int getProcessId() {
    return this.pid;
  }
  
  @Override
  public String status() throws UnableToControlProcessException, IOException, InterruptedException, TimeoutException {
    return status(this.arguments.getWorkingDirectory(), this.arguments.getProcessType().getStatusRequestFileName(), this.arguments.getProcessType().getStatusFileName());
  }

  @Override
  public void stop() throws UnableToControlProcessException, IOException {
    stop(this.arguments.getWorkingDirectory(), this.arguments.getProcessType().getStopRequestFileName());
  }
  
  @Override
  public void checkPidSupport() {
    throw new AttachAPINotFoundException(LocalizedStrings.Launcher_ATTACH_API_NOT_FOUND_ERROR_MESSAGE.toLocalizedString());
  }
  
  private void stop(final File workingDir, final String stopRequestFileName) throws UnableToControlProcessException, IOException {
    final File stopRequestFile = new File(workingDir, stopRequestFileName);
    if (!stopRequestFile.exists()) {
      stopRequestFile.createNewFile();
    }
  }
  
  private String status(final File workingDir, final String statusRequestFileName, final String statusFileName) throws UnableToControlProcessException, IOException, InterruptedException, TimeoutException {
    // monitor for statusFile
    final File statusFile = new File(workingDir, statusFileName);
    final AtomicReference<String> statusRef = new AtomicReference<String>();
    
    final ControlRequestHandler statusHandler = new ControlRequestHandler() {
      @Override
      public void handleRequest() throws IOException {
        // read the statusFile
        final BufferedReader reader = new BufferedReader(new FileReader(statusFile));
        final StringBuilder lines = new StringBuilder();
        try {
          String line = null;
          while ((line = reader.readLine()) != null) {
            lines.append(line);
          }
        } finally {
          statusRef.set(lines.toString());
          reader.close();
        }
      }
    };

    final ControlFileWatchdog statusFileWatchdog = new ControlFileWatchdog(workingDir, statusFileName, statusHandler, true);
    statusFileWatchdog.start();

    final File statusRequestFile = new File(workingDir, statusRequestFileName);
    if (!statusRequestFile.exists()) {
      statusRequestFile.createNewFile();
    }
    
    // if timeout invoke stop and then throw TimeoutException
    final long start = System.currentTimeMillis();
    while (statusFileWatchdog.isAlive()) {
      Thread.sleep(10);
      if (System.currentTimeMillis() >= start + this.statusTimeoutMillis) {
        final TimeoutException te = new TimeoutException("Timed out waiting for process to create " + statusFile);
        try {
          statusFileWatchdog.stop();
        } catch (InterruptedException e) {
          logger.info("Interrupted while stopping status file watchdog.", e);
        } catch (RuntimeException e) {
          logger.info("Unexpected failure while stopping status file watchdog.", e);
        }
        throw te;
      }
    }
    
    final String lines = statusRef.get();
    if (null == lines || lines.trim().isEmpty()) {
      throw new IllegalStateException("Failed to read status file");
    }
    return lines;
  }
}
