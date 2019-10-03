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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.LogService;

public class ProcessLogger {

  private final Logger logger;
  private final Queue<OutputLine> outputLines = new ConcurrentLinkedQueue<>();

  public ProcessLogger(Process process, String name) {
    logger = LogService.getLogger(name);

    StreamGobbler stdOutGobbler =
        new StreamGobbler(process.getInputStream(), this::consumeInfoMessage);
    StreamGobbler stdErrGobbler =
        new StreamGobbler(process.getErrorStream(), this::consumeErrorMessage);

    stdOutGobbler.startInNewThread();
    stdErrGobbler.startInNewThread();
  }

  private void consumeInfoMessage(String message) {
    logger.info(message);
    outputLines.add(OutputLine.fromStdOut(message));
  }

  private void consumeErrorMessage(String message) {
    logger.error(message);
    outputLines.add(OutputLine.fromStdErr(message));
  }

  public List<String> getStdOutLines() {
    return getOutputLines(OutputLine.OutputSource.STD_OUT);
  }

  public List<String> getStdErrLines() {
    return getOutputLines(OutputLine.OutputSource.STD_ERR);
  }

  public List<String> getOutputLines() {
    return outputLines.stream().map(OutputLine::getLine).collect(toList());
  }

  public String getOutputText() {
    return outputLines.stream().map(OutputLine::getLine)
        .collect(joining(System.lineSeparator()));
  }

  private List<String> getOutputLines(OutputLine.OutputSource source) {
    return outputLines.stream().filter(line -> line.getSource().equals(source))
        .map(OutputLine::getLine).collect(toList());
  }
}
