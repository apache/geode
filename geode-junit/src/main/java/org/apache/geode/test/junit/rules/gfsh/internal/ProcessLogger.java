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

import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

public class ProcessLogger {
  private static final LoggerContext LOGGER_CONTEXT = createLoggerContext();
  private final Logger logger;

  private final Queue<OutputLine> outputLines = new ConcurrentLinkedQueue<>();

  public ProcessLogger(Process process, String name) {
    this.logger = LOGGER_CONTEXT.getLogger(name);

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

  private static LoggerContext createLoggerContext() {
    ConfigurationBuilder<BuiltConfiguration> builder =
        ConfigurationBuilderFactory.newConfigurationBuilder();
    builder.setStatusLevel(Level.ERROR);
    builder.add(builder.newFilter("ThresholdFilter", Filter.Result.ACCEPT, Filter.Result.NEUTRAL)
        .addAttribute("level", Level.DEBUG));
    AppenderComponentBuilder appenderBuilder = builder.newAppender("Stdout", "CONSOLE")
        .addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT);
    appenderBuilder.add(builder.newLayout("PatternLayout").addAttribute("pattern",
        "[%-5level %d{HH:mm:ss.SSS z}] (%c): %msg%n%throwable"));
    appenderBuilder.add(builder.newFilter("MarkerFilter", Filter.Result.DENY, Filter.Result.NEUTRAL)
        .addAttribute("marker", "FLOW"));
    builder.add(appenderBuilder);
    builder.add(builder.newLogger("org.apache.logging.log4j", Level.ERROR)
        .add(builder.newAppenderRef("Stdout")).addAttribute("additivity", false));
    builder.add(builder.newRootLogger(Level.ERROR).add(builder.newAppenderRef("Stdout")));

    return Configurator.initialize(builder.build());
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
        .collect(joining(SystemUtils.LINE_SEPARATOR));
  }

  private List<String> getOutputLines(OutputLine.OutputSource source) {
    return outputLines.stream().filter(line -> line.getSource().equals(source))
        .map(OutputLine::getLine).collect(toList());
  }
}
