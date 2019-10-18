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

import static java.lang.System.lineSeparator;
import static java.util.stream.Collectors.joining;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;

public class ProcessLogger implements AutoCloseable {

  private final Logger logger;
  private final Queue<OutputLine> outputLines;
  private final StreamGobbler stdoutGobbler;
  private final StreamGobbler stderrGobbler;

  public ProcessLogger(Process process, String name) {
    logger = LogService.getLogger(name);
    outputLines = new ConcurrentLinkedQueue<>();
    stdoutGobbler = new StreamGobbler(process.getInputStream(), this::consumeInfoMessage);
    stderrGobbler = new StreamGobbler(process.getErrorStream(), this::consumeErrorMessage);
  }

  public void start() {
    stdoutGobbler.start();
    stderrGobbler.start();
  }

  public String getOutputText() {
    return outputLines.stream()
        .map(OutputLine::getLine)
        .collect(joining(lineSeparator()));
  }

  @Override
  public void close() {
    stdoutGobbler.close();
    stderrGobbler.close();
  }

  /**
   * Blocks until the ProcessLogger finishes collecting the process's output
   *
   * @throws InterruptedException if the ProcessLogger's threads are interrupted
   * @throws ExecutionException if the ProcessLogger's threads throw
   * @throws TimeoutException if the ProcessLogger does not finish collecting output before the
   *         timeout
   */
  public void awaitTermination(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    stdoutGobbler.awaitTermination(timeout, unit);
    stderrGobbler.awaitTermination(timeout, unit);
  }

  private void consumeInfoMessage(String message) {
    logger.info(message);
    outputLines.add(OutputLine.fromStdOut(message));
  }

  private void consumeErrorMessage(String message) {
    logger.error(message);
    outputLines.add(OutputLine.fromStdErr(message));
  }
}
