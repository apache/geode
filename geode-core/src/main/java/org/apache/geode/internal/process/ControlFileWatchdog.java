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

import static org.apache.commons.lang.Validate.notEmpty;
import static org.apache.commons.lang.Validate.notNull;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThread;

/**
 * Invokes a ControlRequestHandler when a control file has been created.
 *
 * @since GemFire 8.0
 */
class ControlFileWatchdog implements Runnable {
  private static final Logger logger = LogService.getLogger();

  private static final long STOP_TIMEOUT_MILLIS = 60 * 1000;
  private static final long LOOP_INTERVAL_MILLIS = 1000;

  private final File directory;
  private final File file;
  private final ControlRequestHandler requestHandler;
  private final boolean stopAfterRequest;

  private Thread thread;
  private boolean alive;

  ControlFileWatchdog(final File directory, final String fileName,
      final ControlRequestHandler requestHandler, final boolean stopAfterRequest) {
    notNull(directory, "Invalid directory '" + directory + "' specified");
    notEmpty(fileName, "Invalid fileName '" + fileName + "' specified");
    notNull(requestHandler, "Invalid requestHandler '" + requestHandler + "' specified");

    this.directory = directory;
    this.file = new File(directory, fileName);
    this.requestHandler = requestHandler;
    this.stopAfterRequest = stopAfterRequest;
  }

  @Override
  public void run() {
    try { // always set alive before stopping
      while (isAlive()) {
        doWork();
      }
    } finally {
      synchronized (this) {
        alive = false;
      }
    }
  }

  private void doWork() {
    try { // handle handle exceptions
      if (file.exists()) {
        try { // always check stopAfterRequest after handleRequest
          handleRequest();
        } finally {
          if (stopAfterRequest) {
            stopMe();
          }
        }
      }
      Thread.sleep(LOOP_INTERVAL_MILLIS);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
      // allow to loop around and check isAlive()
    } catch (IOException ignored) {
      logger.error(
          "Unable to control process with {}. Please add tools.jar from JDK to classpath for improved process control.",
          file);
      // allow to loop around and check isAlive()
    }
  }

  private void handleRequest() throws IOException {
    try { // always delete file after invoking handler
      requestHandler.handleRequest();
    } finally {
      try {
        file.delete();
      } catch (SecurityException e) {
        logger.warn("Unable to delete {}", file, e);
      }
    }
  }

  void start() {
    synchronized (this) {
      if (thread == null) {
        thread = new LoggingThread(createThreadName(), this);
        alive = true;
        thread.start();
      }
    }
  }

  void stop() throws InterruptedException {
    Thread stopping = null;
    synchronized (this) {
      if (thread != null) {
        alive = false;
        if (thread != Thread.currentThread()) {
          thread.interrupt();
          stopping = thread;
        }
        thread = null;
      }
    }
    if (stopping != null) {
      stopping.join(STOP_TIMEOUT_MILLIS);
    }
  }

  boolean isAlive() {
    synchronized (this) {
      return alive;
    }
  }

  private void stopMe() {
    synchronized (this) {
      if (thread != null) {
        alive = false;
        thread = null;
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append('@').append(System.identityHashCode(this)).append('{');
    sb.append("directory=").append(directory);
    sb.append(", file=").append(file);
    sb.append(", alive=").append(alive); // not synchronized
    sb.append(", stopAfterRequest=").append(stopAfterRequest);
    return sb.append('}').toString();
  }

  private String createThreadName() {
    return getClass().getSimpleName() + '@' + Integer.toHexString(hashCode()) + " monitoring "
        + file.getName();
  }

  /**
   * Defines the callback to be invoked when the control file exists.
   */
  interface ControlRequestHandler {
    void handleRequest() throws IOException;
  }
}
