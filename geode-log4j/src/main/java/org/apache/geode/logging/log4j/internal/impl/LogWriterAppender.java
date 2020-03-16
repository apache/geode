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
package org.apache.geode.logging.log4j.internal.impl;

import static java.lang.Boolean.FALSE;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;

import org.apache.geode.internal.logging.ManagerLogWriter;
import org.apache.geode.internal.logging.ManagerLogWriterFactory;
import org.apache.geode.internal.logging.ManagerLogWriterFactory.LogFileRolloverDetails;
import org.apache.geode.internal.statistics.StatisticsConfig;
import org.apache.geode.logging.internal.LoggingSessionRegistryProvider;
import org.apache.geode.logging.internal.spi.LogConfig;
import org.apache.geode.logging.internal.spi.LogConfigListener;
import org.apache.geode.logging.internal.spi.LogConfigSupplier;
import org.apache.geode.logging.internal.spi.LogFile;
import org.apache.geode.logging.internal.spi.LoggingSessionListener;
import org.apache.geode.logging.internal.spi.LoggingSessionRegistry;
import org.apache.geode.logging.internal.spi.SessionContext;

@Plugin(name = LogWriterAppender.PLUGIN_NAME, category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE, printObject = true)
@SuppressWarnings("unused")
public class LogWriterAppender extends AbstractAppender
    implements PausableAppender, DebuggableAppender, LoggingSessionListener, LogConfigListener {

  public static final String PLUGIN_NAME = "GeodeLogWriter";

  private static final boolean START_PAUSED_BY_DEFAULT = true;

  /**
   * True if this thread is in the process of appending.
   */
  private static final ThreadLocal<Boolean> APPENDING = ThreadLocal.withInitial(() -> FALSE);

  private final String eagerMemberName;
  private volatile String lazyMemberName;
  private final MemberNameSupplier memberNameSupplier;

  /**
   * appendLog used to be controlled by undocumented system property gemfire.append-log
   */
  private final boolean appendLog;
  private final boolean security;
  private final boolean debug;
  private final List<LogEvent> events;
  private final LoggingSessionRegistry loggingSessionRegistry;

  private volatile ManagerLogWriter logWriter;
  private volatile LogConfigSupplier logConfigSupplier;
  private volatile LogFileRolloverDetails logFileRolloverDetails;
  private volatile boolean paused;

  protected LogWriterAppender(final String name,
      final Layout<? extends Serializable> layout,
      final Filter filter,
      final ManagerLogWriter logWriter) {
    this(name, layout, filter, MemberNamePatternConverter.INSTANCE.getMemberNameSupplier(), null,
        true, false, START_PAUSED_BY_DEFAULT, false, LoggingSessionRegistryProvider.get());
  }

  protected LogWriterAppender(final String name,
      final Layout<? extends Serializable> layout,
      final Filter filter,
      final MemberNameSupplier memberNameSupplier,
      final String eagerMemberName,
      final boolean appendLog,
      final boolean security,
      final boolean startPaused,
      final boolean debug,
      final LoggingSessionRegistry loggingSessionRegistry) {
    super(name, filter, layout, true, Property.EMPTY_ARRAY);
    this.memberNameSupplier = memberNameSupplier;
    if (eagerMemberName != null) {
      memberNameSupplier.set(eagerMemberName);
    }
    this.eagerMemberName = eagerMemberName;
    this.appendLog = appendLog;
    this.security = security;
    this.debug = debug;
    if (debug) {
      events = Collections.synchronizedList(new ArrayList<>());
    } else {
      events = Collections.emptyList();
    }
    this.loggingSessionRegistry = loggingSessionRegistry;
    paused = startPaused;
  }

  @PluginBuilderFactory
  public static <B extends LogWriterAppender.Builder<B>> B newBuilder() {
    return new LogWriterAppender.Builder<B>().asBuilder();
  }

  /**
   * Builds LogWriterAppender instances.
   *
   * @param <B> The type to build
   */
  public static class Builder<B extends Builder<B>> extends AbstractAppender.Builder<B>
      implements org.apache.logging.log4j.core.util.Builder<LogWriterAppender> {

    @PluginBuilderAttribute
    private String memberName;

    @PluginBuilderAttribute
    private boolean security;

    @PluginBuilderAttribute
    private boolean appendLog = true;

    @PluginBuilderAttribute
    private boolean startPaused = START_PAUSED_BY_DEFAULT;

    @PluginBuilderAttribute
    private boolean debug;

    // GEODE-5785: add file permissions support similar to FileAppender

    public B withMemberName(final String memberName) {
      this.memberName = memberName;
      return asBuilder();
    }

    public String getMemberName() {
      return memberName;
    }

    public B setSecurity(final boolean security) {
      this.security = security;
      return asBuilder();
    }

    public boolean isSecurity() {
      return security;
    }

    public B setAppendLog(final boolean shouldAppendLog) {
      appendLog = shouldAppendLog;
      return asBuilder();
    }

    public boolean isAppendLog() {
      return appendLog;
    }

    public B setStartPaused(final boolean shouldStartPaused) {
      startPaused = shouldStartPaused;
      return asBuilder();
    }

    public boolean isStartPaused() {
      return debug;
    }

    public B setDebug(final boolean shouldDebug) {
      debug = shouldDebug;
      return asBuilder();
    }

    public boolean isDebug() {
      return debug;
    }

    @Override
    public LogWriterAppender build() {
      Layout<? extends Serializable> layout = getOrCreateLayout();
      return new LogWriterAppender(getName(), layout, getFilter(),
          MemberNamePatternConverter.INSTANCE.getMemberNameSupplier(), memberName, appendLog,
          security, startPaused, debug, LoggingSessionRegistryProvider.get());
    }
  }

  @Override
  public void append(final LogEvent event) {
    if (isPaused()) {
      return;
    }
    doAppendIfNotAppending(event);
  }

  @Override
  public void start() {
    LOGGER.info("Starting {}.", this);
    LOGGER.debug("Adding {} to {}.", this, loggingSessionRegistry);
    loggingSessionRegistry.addLoggingSessionListener(this);
    super.start();
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping {}.", this);

    // stop LogEvents from coming to this appender
    super.stop();

    // clean up
    loggingSessionRegistry.removeLoggingSessionListener(this);
    stopSession();

    LOGGER.info("{} has stopped.", this);
  }

  @Override
  public void pause() {
    LOGGER.debug("Pausing {}.", this);
    paused = true;
  }

  @Override
  public void resume() {
    LOGGER.debug("Resuming {}.", this);
    paused = false;
  }

  @Override
  public boolean isPaused() {
    return paused;
  }

  @Override
  public void clearLogEvents() {
    events.clear();
  }

  @Override
  public List<LogEvent> getLogEvents() {
    return events;
  }

  @Override
  public synchronized void createSession(final SessionContext sessionContext) {
    logConfigSupplier = sessionContext.getLogConfigSupplier();

    LOGGER.info("Creating session in {} with {}.", this, logConfigSupplier);

    logConfigSupplier.addLogConfigListener(this);

    LogConfig logConfig = logConfigSupplier.getLogConfig();
    if (eagerMemberName == null && lazyMemberName == null) {
      String memberName = logConfig.getName();
      memberNameSupplier.set(memberName);
      lazyMemberName = memberName;
    }

    StatisticsConfig statisticsConfig = logConfigSupplier.getStatisticsConfig();
    ManagerLogWriterFactory managerLogWriterFactory = new ManagerLogWriterFactory()
        .setSecurity(security).setAppendLog(appendLog);

    logWriter = managerLogWriterFactory.create(logConfig, statisticsConfig);

    if (logWriter == null) {
      logWriter = new NullLogWriter();
    }

    logFileRolloverDetails = managerLogWriterFactory.getLogFileRolloverDetails();
  }

  @Override
  public synchronized void startSession() {
    LOGGER.info("Starting session in {}.", this);

    logWriter.startupComplete();

    resume();

    logRolloverDetails(logFileRolloverDetails);

    logFileRolloverDetails = null;
    logConfigSupplier = null;
  }

  @Override
  public synchronized void stopSession() {
    LOGGER.info("Stopping session in {}.", this);
    logWriter.shuttingDown();
    pause();
    logWriter.closingLogFile();
  }

  @Override
  public Optional<LogFile> getLogFile() {
    return Optional.of(new LogFile(logWriter));
  }

  @Override
  public void configChanged() {
    logWriter.configChanged();
  }

  @Override
  public String toString() {
    return getClass().getName() + "@" + Integer.toHexString(hashCode()) + ":" + getName()
        + " {eagerMemberName=" + eagerMemberName + ", lazyMemberName=" + lazyMemberName
        + "appendLog=" + appendLog + ", security=" + security + ", paused=" + paused
        + ", loggingSessionRegistry=" + loggingSessionRegistry + ", logWriter=" + logWriter
        + ", debug=" + debug + "}";
  }

  ManagerLogWriter getLogWriter() {
    return logWriter;
  }

  private void doAppendIfNotAppending(final LogEvent event) {
    if (APPENDING.get()) {
      // If already appending then don't send to avoid infinite recursion
      return;
    }
    APPENDING.set(Boolean.TRUE);
    try {
      ManagerLogWriter currentLogWriter = logWriter;
      if (currentLogWriter == null || currentLogWriter instanceof NullLogWriter) {
        return;
      }
      doAppendToLogWriter(currentLogWriter, event);
    } finally {
      APPENDING.set(FALSE);
    }
  }

  private void doAppendToLogWriter(final ManagerLogWriter logWriter, final LogEvent event) {
    byte[] bytes = getLayout().toByteArray(event);
    if (bytes != null && bytes.length > 0) {
      logWriter.writeFormattedMessage(new String(bytes, Charset.defaultCharset()));
    }
    if (debug) {
      events.add(event);
    }
  }

  private void logRolloverDetails(final LogFileRolloverDetails logFileRolloverDetails) {
    // log the first msg about renaming logFile for rolling if it pre-existed
    if (logFileRolloverDetails.exists()) {
      if (logFileRolloverDetails.isWarning()) {
        LogManager.getLogger().warn(logFileRolloverDetails.getMessage());
      } else {
        LogManager.getLogger().info(logFileRolloverDetails.getMessage());
      }
    }
  }
}
