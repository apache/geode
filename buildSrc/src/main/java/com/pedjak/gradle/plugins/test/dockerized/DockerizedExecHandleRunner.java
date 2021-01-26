/*
 * Copyright 2010 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pedjak.gradle.plugins.test.dockerized;

import java.util.concurrent.Executor;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.process.internal.StreamsHandler;

public class DockerizedExecHandleRunner implements Runnable {
  private static final Logger LOGGER =
      Logging.getLogger(org.gradle.process.internal.ExecHandleRunner.class);

  private final DockerizedExecHandle execHandle;
  private final Lock lock = new ReentrantLock();
  private final Executor executor;

  private Process process;
  private boolean aborted;
  private final StreamsHandler streamsHandler;

  public DockerizedExecHandleRunner(DockerizedExecHandle execHandle, StreamsHandler streamsHandler,
                                    Executor executor) {
    this.executor = executor;
    if (execHandle == null) {
      throw new IllegalArgumentException("execHandle == null!");
    }
    this.streamsHandler = streamsHandler;
    this.execHandle = execHandle;
  }

  public void abortProcess() {
    lock.lock();
    try {
      aborted = true;
      if (process != null) {
        LOGGER.debug("Abort requested. Destroying process: {}.", execHandle.getDisplayName());
        process.destroy();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void run() {
    try {
      process = execHandle.runContainer();
      streamsHandler.connectStreams(process, execHandle.getDisplayName(), executor);

      execHandle.started();

      LOGGER.debug("waiting until streams are handled...");
      streamsHandler.start();
      if (execHandle.isDaemon()) {
        streamsHandler.stop();
        detached();
      } else {
        int exitValue = process.waitFor();
        streamsHandler.stop();
        completed(exitValue);
      }
    } catch (Throwable t) {
      execHandle.failed(t);
    }
  }

  private void completed(int exitValue) {
    if (aborted) {
      execHandle.aborted(exitValue);
    } else {
      execHandle.finished(exitValue);
    }
  }

  private void detached() {
    execHandle.detached();
  }

  public String toString() {
    return "Handler for " + process.toString();
  }
}

