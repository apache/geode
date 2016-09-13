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

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;

/**
 * Invokes a ControlRequestHandler when a control file has been created.
 * 
 * @since GemFire 8.0
 */
final class ControlFileWatchdog implements Runnable {
  private static final Logger logger = LogService.getLogger();

  private static final long STOP_TIMEOUT_MILLIS = 60*1000;
  private static final long SLEEP_MILLIS = 1000;
  
  private final File workingDir;
  private final File file;
  private final ControlRequestHandler requestHandler;
  private final boolean stopAfterRequest;
  private Thread thread;
  private boolean alive;
  
  ControlFileWatchdog(final File workingDir, final String fileName, final ControlRequestHandler requestHandler, final boolean stopAfterRequest) {
    this.workingDir = workingDir;
    this.file = new File(this.workingDir, fileName);
    this.requestHandler = requestHandler;
    this.stopAfterRequest = stopAfterRequest;
  }

  @Override
  public void run() {
    try { // always set this.alive before stopping
      while (isAlive()) {
        try { // handle handle exceptions
          Thread.sleep(SLEEP_MILLIS);
          if (this.file.exists()) {
            try { // always check stopAfterRequest after main work
              work();
            } finally {
              if (this.stopAfterRequest) {
                stopMe();
              }
            }
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          // allow to loop around and check isAlive()
        } catch (IOException e) {
          logger.error("Unable to control process with {}. Please add tools.jar from JDK to classpath for improved process control.", this.file);
          // allow to loop around and check isAlive()
        }
      }
    } finally {
      synchronized (this) {
        this.alive = false;
      }
    }
  }
  
  private void work() throws IOException {
    try { // always delete file after invoking handler
      this.requestHandler.handleRequest();
    } finally {
      try {
        this.file.delete();
      } catch (SecurityException e) {
        logger.warn("Unable to delete {}", this.file, e);
      }
    }
  }
  
  void start() {
    synchronized (this) {
      if (this.thread == null) {
        this.thread = new Thread(this, createThreadName());
        this.thread.setDaemon(true);
        this.alive = true;
        this.thread.start();
      }
    }
  }

  void stop() throws InterruptedException {
    Thread stopping = null;
    synchronized (this) {
      if (this.thread != null) {
        this.alive = false;
        if (this.thread != Thread.currentThread()) {
          this.thread.interrupt();
          stopping = this.thread;
        }
        this.thread = null;
      }
    }
    if (stopping != null) {
      stopping.join(STOP_TIMEOUT_MILLIS);
    }
  }
  
  boolean isAlive() {
    synchronized (this) {
      return this.alive;
    }
  }
  
  private void stopMe() {
    synchronized (this) {
      if (this.thread != null) {
        this.alive = false;
        this.thread = null;
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
    sb.append("@").append(System.identityHashCode(this)).append("{");
    sb.append("workingDir=").append(this.workingDir);
    sb.append(", file=").append(this.file);
    sb.append(", alive=").append(this.alive);
    sb.append(", stopAfterRequest=").append(this.stopAfterRequest);
    return sb.append("}").toString();
  }
  
  private String createThreadName() {
    return getClass().getSimpleName() + "@" + Integer.toHexString(hashCode()) + " monitoring " + this.file.getName();
  }
  
  /**
   * Defines the callback to be invoked when the control file exists.
   */
  interface ControlRequestHandler {
    public void handleRequest() throws IOException;
  }
}
