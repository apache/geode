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
package org.apache.geode.test.junit.rules.gfsh.internal;

import static java.util.concurrent.Executors.newSingleThreadExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

class StreamGobbler implements AutoCloseable {
  private final InputStream inputStream;
  private final Consumer<String> consumeInputLine;
  private final ExecutorService executorService;

  private Future<?> processInputTask;

  StreamGobbler(InputStream inputStream, Consumer<String> consumeInputLine) {
    this.inputStream = inputStream;
    this.consumeInputLine = consumeInputLine;

    executorService = newSingleThreadExecutor();
  }

  public void start() {
    processInputTask = executorService.submit(this::processInputStream);
  }

  @Override
  public void close() {
    executorService.shutdown();
  }

  /**
   * Blocks until the StreamGobbler finishes processing the input stream
   *
   * @throws InterruptedException if the StreamGobbler thread was interrupted
   * @throws ExecutionException if the StreamGobbler thread threw an exception
   * @throws TimeoutException if the StreamGobbler does not finish processing input before the
   *         timeout
   */
  public void awaitTermination(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    if (processInputTask != null) {
      processInputTask.get(timeout, unit);
    }
  }

  private void processInputStream() {
    try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream))) {
      bufferedReader.lines().forEach(consumeInputLine);
    } catch (UncheckedIOException | IOException ignored) {
      // If this gobbler is reading the System.out stream from a process that gets killed,
      // we will occasionally see an exception here than can be ignored.
    }
  }
}
