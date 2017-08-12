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
package org.apache.geode.test.dunit.rules.gfsh;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.Lists;
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

  private final Queue<String> stdOutLines = new ConcurrentLinkedQueue<>();
  private final Queue<String> stdErrorLines = new ConcurrentLinkedQueue<>();
  private final StreamGobbler stdOutGobbler;
  private final StreamGobbler stdErrGobbler;

  public ProcessLogger(Process process, String name) {
    this.logger = LOGGER_CONTEXT.getLogger(name);

    this.stdOutGobbler = new StreamGobbler(process.getInputStream(), this::consumeInfoMessage);
    this.stdErrGobbler = new StreamGobbler(process.getErrorStream(), this::consumeErrorMessage);

    stdOutGobbler.startInNewThread();
    stdErrGobbler.startInNewThread();
  }

  private void consumeInfoMessage(String message) {
    logger.info(message);
    stdOutLines.add(message);
  }

  private void consumeErrorMessage(String message) {
    logger.error(message);
    stdErrorLines.add(message);
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
    return Lists.newArrayList(stdOutLines.iterator());
  }

  public List<String> getStdErrLines() {
    return Lists.newArrayList(stdOutLines.iterator());
  }

}
