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
package org.apache.geode.internal.logging.log4j;

import static java.lang.Boolean.FALSE;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;

import org.apache.geode.internal.Banner;
import org.apache.geode.internal.logging.Configuration;
import org.apache.geode.internal.logging.LogConfig;
import org.apache.geode.internal.logging.LogConfigListener;
import org.apache.geode.internal.logging.LogConfigSupplier;
import org.apache.geode.internal.logging.LogFile;
import org.apache.geode.internal.logging.LoggingSessionListener;
import org.apache.geode.internal.logging.LoggingSessionListeners;
import org.apache.geode.internal.logging.ManagerLogWriter;
import org.apache.geode.internal.logging.ManagerLogWriterFactory;
import org.apache.geode.internal.logging.ManagerLogWriterFactory.LogFileRolloverDetails;
import org.apache.geode.internal.logging.NullLogWriter;
import org.apache.geode.internal.statistics.StatisticsConfig;

@Plugin(name = PausableLogWriterAppender.PLUGIN_NAME, category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE, printObject = true)
@SuppressWarnings("unused")
public class PausableLogWriterAppender extends AbstractAppender
    implements PausableAppender, DebuggableAppender, LoggingSessionListener, LogConfigListener {

  public static final String PLUGIN_NAME = "GeodeLogWriter";

  private static final boolean START_PAUSED_BY_DEFAULT = true;

  /** Is this thread in the process of appending? */
  private static final ThreadLocal<Boolean> APPENDING = ThreadLocal.withInitial(() -> FALSE);

  private final String eagerMemberName;
  private volatile String lazyMemberName;
  private final MemberNameSupplier memberNameSupplier;

  /**
   * appendLog used to be controlled by undocumented system property gemfire.append-log
   */
  private final boolean appendLog;
  private final boolean logBanner;
  private final boolean logConfiguration;
  private final boolean security;
  private final boolean debug;
  private final List<LogEvent> events;
  private final LoggingSessionListeners loggingSessionListeners;

  private volatile ManagerLogWriter logWriter;
  private volatile LogConfigSupplier logConfigSupplier;
  private volatile LogFileRolloverDetails logFileRolloverDetails;
  private volatile boolean paused;

  protected PausableLogWriterAppender(final String name,
      final Layout<? extends Serializable> layout, final Filter filter,
      final ManagerLogWriter logWriter) {
    this(name, layout, filter, MemberNamePatternConverter.INSTANCE.getMemberNameSupplier(), null,
        true, true, true, false, START_PAUSED_BY_DEFAULT, false, LoggingSessionListeners.get());
  }

  protected PausableLogWriterAppender(final String name,
      final Layout<? extends Serializable> layout,
      final Filter filter,
      final MemberNameSupplier memberNameSupplier,
      final String eagerMemberName,
      final boolean appendLog,
      final boolean logBanner,
      final boolean logConfiguration,
      final boolean security,
      final boolean startPaused,
      final boolean debug,
      final LoggingSessionListeners loggingSessionListeners) {
    super(name, filter, layout);
    this.memberNameSupplier = memberNameSupplier;
    if (eagerMemberName != null) {
      memberNameSupplier.set(eagerMemberName);
    }
    this.eagerMemberName = eagerMemberName;
    this.appendLog = appendLog;
    this.logBanner = logBanner;
    this.logConfiguration = logConfiguration;
    this.security = security;
    this.debug = debug;
    if (debug) {
      events = Collections.synchronizedList(new ArrayList<>());
    } else {
      events = Collections.emptyList();
    }
    this.loggingSessionListeners = loggingSessionListeners;
    paused = startPaused;
  }

  @PluginBuilderFactory
  public static <B extends PausableLogWriterAppender.Builder<B>> B newBuilder() {
    return new PausableLogWriterAppender.Builder<B>().asBuilder();
  }

  /**
   * Builds PausableLogWriterAppender instances.
   *
   * @param <B> The type to build
   */
  public static class Builder<B extends Builder<B>> extends AbstractAppender.Builder<B>
      implements org.apache.logging.log4j.core.util.Builder<PausableLogWriterAppender> {

    @PluginBuilderAttribute
    private String memberName;

    @PluginBuilderAttribute
    private boolean security;

    @PluginBuilderAttribute
    private boolean appendLog = true;

    @PluginBuilderAttribute
    private boolean logBanner = true;

    @PluginBuilderAttribute
    private boolean logConfiguration = true;

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

    public B setLogBanner(final boolean shouldLogBanner) {
      logBanner = shouldLogBanner;
      return asBuilder();
    }

    public boolean isLogBanner() {
      return logBanner;
    }

    public B setLogConfiguration(final boolean shouldLogConfiguration) {
      logConfiguration = shouldLogConfiguration;
      return asBuilder();
    }

    public boolean isLogConfiguration() {
      return logConfiguration;
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
    public PausableLogWriterAppender build() {
      Layout<? extends Serializable> layout = getOrCreateLayout();
      return new PausableLogWriterAppender(getName(), layout, getFilter(),
          MemberNamePatternConverter.INSTANCE.getMemberNameSupplier(), memberName, appendLog,
          logBanner, logConfiguration, security, startPaused, debug, LoggingSessionListeners.get());
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
    LOGGER.debug("Adding {} to {}.", this, loggingSessionListeners);
    loggingSessionListeners.addLoggingLifecycleListener(this);
    super.start();
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping {}.", this);

    // stop LogEvents from coming to this appender
    super.stop();

    // clean up
    loggingSessionListeners.removeLoggingLifecycleListener(this);
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

  /**
   * TODO:KIRK: createSession might be called more than once for reconnect
   */
  @Override
  public synchronized void createSession(final LogConfigSupplier logConfigSupplier) {
    LOGGER.info("Creating session in {} with {}.", this, logConfigSupplier);

    this.logConfigSupplier = logConfigSupplier;
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

  /**
   * TODO:KIRK: createSession might be called more than once for reconnect
   */
  @Override
  public void startSession() {
    LOGGER.info("Starting session in {}.", this);

    logWriter.startupComplete();

    resume();

    if (logBanner) {
      // TODO: add boolean logBanner and also pass in way to skip for reconnect
      logBanner();
    }

    logRolloverDetails(logFileRolloverDetails);

    if (logConfiguration) {
      logConfiguration(logConfigSupplier.getLogConfig());
    }

    logFileRolloverDetails = null;
    logConfigSupplier = null;
  }

  @Override
  public void stopSession() {
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
        + "appendLog=" + appendLog + ", logBanner=" + logBanner + ", logConfiguration="
        + logConfiguration + ", security=" + security + ", paused=" + paused
        + ", loggingSessionListeners=" + loggingSessionListeners + ", logWriter=" + logWriter
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
      // TODO:PERF: avoid creating new String from bytes - event.getMessage().getFormattedMessage()?
      logWriter.writeFormattedMessage(new String(bytes, Charset.defaultCharset()));
    }
    if (debug) {
      events.add(event);
    }
  }

  private void logBanner() {
    LogManager.getLogger().info(LogMarker.CONFIG_MARKER, Banner.getString(null));
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

  /**
   * TODO:KIRK: loner and stand-alone locator previously suppressed logConfiguration
   */
  private void logConfiguration(final LogConfig logConfig) {
    // log the config
    boolean logTheConfig = !security; // or if stand-alone locator
    if (logTheConfig) {
      // if (!logConfig.isLoner()) {
      LogManager.getLogger().info(Configuration.STARTUP_CONFIGURATION + logConfig.toLoggerString());
      // }
    }
  }

  // TODO: delete toCharArray
  private static char[] toCharArray(final byte[] input) {
    char[] result = new char[input.length];
    for (int i = 0; i < input.length; i++) {
      result[i] = (char) input[i];
    }
    return result;
  }

  // TODO: delete MemberNameReference
  private interface MemberNameReference {

    void setMemberName(final String memberName);

    String getMemberName();

    boolean checkMemberName();
  }

  // TODO: delete FinalMemberNameReference
  private static class FinalMemberNameReference implements MemberNameReference {

    private final String memberName;

    FinalMemberNameReference(final String memberName) {
      this.memberName = memberName;
    }

    @Override
    public void setMemberName(final String memberName) {
      throw new UnsupportedOperationException(
          getClass().getSimpleName() + " does not support setMemberName");
    }

    @Override
    public String getMemberName() {
      return memberName;
    }

    @Override
    public boolean checkMemberName() {
      return false;
    }
  }

  // TODO: delete FinalMemberNameReference
  private static class AtomicMemberNameReference implements MemberNameReference {

    private final AtomicReference<String> memberNameRef = new AtomicReference<>();

    @Override
    public void setMemberName(final String memberName) {
      memberNameRef.set(memberName);
    }

    @Override
    public String getMemberName() {
      return memberNameRef.get();
    }

    @Override
    public boolean checkMemberName() {
      return memberNameRef.get() == null;
    }
  }
}
