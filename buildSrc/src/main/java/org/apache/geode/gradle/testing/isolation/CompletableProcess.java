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
 *
 */
package org.apache.geode.gradle.testing.isolation;

import java.io.InputStream;
import java.io.OutputStream;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

public class CompletableProcess extends Process {
  private static final Logger LOGGER = Logging.getLogger(CompletableProcess.class);
  private final String description;
  private final Process delegate;
  private Runnable onCompletion;

  public CompletableProcess(String description, Process delegate, Runnable onCompletion) {
    this.description = description;
    this.delegate = delegate;
    this.onCompletion = onCompletion;
    LOGGER.debug("{} started", this);
  }

  @Override
  public OutputStream getOutputStream() {
    return delegate.getOutputStream();
  }

  @Override
  public InputStream getInputStream() {
    return delegate.getInputStream();
  }

  @Override
  public InputStream getErrorStream() {
    return delegate.getErrorStream();
  }

  @Override
  public int waitFor() throws InterruptedException {
    try {
      LOGGER.debug("{} waiting for process to finish", this);
      return delegate.waitFor();
    } finally {
      LOGGER.debug("{} finished", this);
      cleanUp();
    }
  }

  @Override
  public int exitValue() {
    int exitValue = delegate.exitValue();
    LOGGER.debug("{} reporting exit value {}", this, exitValue);
    return exitValue;
  }

  @Override
  public void destroy() {
    LOGGER.debug("Destroying {}", this);
    delegate.destroy();
    LOGGER.debug("{} destroyed", this);
    cleanUp();
  }

  @Override
  public String toString() {
    return "CompletableProcess{" + description + '}';
  }

  private synchronized void cleanUp() {
    if (onCompletion == null) {
      return;
    }
    LOGGER.debug("{} cleaning up", this);
    onCompletion.run();
    onCompletion = null;
    LOGGER.debug("{} cleaned up", this);
  }
}
