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
package org.apache.geode.alerting.log4j.internal.impl;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.exception.ExceptionUtils;
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

import org.apache.geode.alerting.internal.AlertingSessionRegistryProvider;
import org.apache.geode.alerting.internal.NullAlertingService;
import org.apache.geode.alerting.internal.api.AlertingService;
import org.apache.geode.alerting.internal.log4j.AlertLevelConverter;
import org.apache.geode.alerting.internal.spi.AlertLevel;
import org.apache.geode.alerting.internal.spi.AlertingAction;
import org.apache.geode.alerting.internal.spi.AlertingSessionListener;
import org.apache.geode.alerting.internal.spi.AlertingSessionRegistry;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.logging.log4j.internal.impl.DebuggableAppender;
import org.apache.geode.logging.log4j.internal.impl.PausableAppender;

@Plugin(name = AlertAppender.PLUGIN_NAME, category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE, printObject = true)
@SuppressWarnings("unused")
public class AlertAppender extends AbstractAppender
    implements PausableAppender, DebuggableAppender, AlertingSessionListener {

  static final String PLUGIN_NAME = "GeodeAlert";

  private static final boolean START_PAUSED_BY_DEFAULT = true;

  private final AtomicReference<AlertingService> alertingServiceRef =
      new AtomicReference<>(NullAlertingService.get());

  private final boolean debug;
  private final List<LogEvent> events;
  private final AlertingSessionRegistry alertingSessionRegistry;

  private volatile boolean paused;

  @VisibleForTesting
  AlertAppender(final String name,
      final Layout<? extends Serializable> layout,
      final Filter filter) {
    this(name, layout, filter, START_PAUSED_BY_DEFAULT, false,
        AlertingSessionRegistryProvider.get());
  }

  private AlertAppender(final String name,
      final Layout<? extends Serializable> layout,
      final Filter filter,
      final boolean startPaused,
      final boolean debug,
      final AlertingSessionRegistry alertingSessionRegistry) {
    super(name, filter, layout, true, Property.EMPTY_ARRAY);
    this.debug = debug;
    if (debug) {
      events = Collections.synchronizedList(new ArrayList<>());
    } else {
      events = Collections.emptyList();
    }
    paused = true;
    this.alertingSessionRegistry = alertingSessionRegistry;
  }

  @PluginBuilderFactory
  public static <B extends AlertAppender.Builder<B>> B newBuilder() {
    return new AlertAppender.Builder<B>().asBuilder();
  }

  /**
   * Builds AlertAppender instances.
   *
   * @param <B> The type to build
   */
  public static class Builder<B extends Builder<B>> extends AbstractAppender.Builder<B>
      implements org.apache.logging.log4j.core.util.Builder<AlertAppender> {

    @PluginBuilderAttribute
    private boolean debug;

    @PluginBuilderAttribute
    private boolean startPaused = START_PAUSED_BY_DEFAULT;

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
    public AlertAppender build() {
      Layout<? extends Serializable> layout = getOrCreateLayout();
      return new AlertAppender(getName(), layout, getFilter(), startPaused, debug,
          AlertingSessionRegistryProvider.get());
    }
  }

  @Override
  public void append(final LogEvent event) {
    LOGGER.trace("Handling append of {} in {}.", event, this);
    if (isPaused()) {
      LOGGER.trace("Skipping append of {} because {} is paused.", event, this);
      return;
    }
    if (!AlertLevelConverter.hasAlertLevel(event.getLevel())) {
      LOGGER.trace("Skipping append of {} because level is {}.", event, event.getLevel());
      return;
    }
    if (AlertingAction.isThreadAlerting()) {
      // If already appending then don't send to avoid infinite recursion
      LOGGER.trace("Skipping append of {} because {} is alerting.", event, Thread.currentThread());
      return;
    }
    doAppend(event);
  }

  private void doAppend(final LogEvent event) {
    AlertingService alertingService = getAlertingService();
    if (alertingService.hasAlertListeners()) {
      sendAlerts(event);
    } else {
      LOGGER.trace("Skipping alert messaging for {} because listeners is empty.", event);
    }
    if (debug) {
      events.add(event);
    }
  }

  private void sendAlerts(final LogEvent event) {
    AlertingService alertingService = getAlertingService();

    AlertLevel alertLevel = AlertLevelConverter.fromLevel(event.getLevel());
    Instant instant = Instant.ofEpochMilli(event.getTimeMillis());
    String threadName = event.getThreadName();
    long threadId = Thread.currentThread().getId();
    String formattedMessage = event.getMessage().getFormattedMessage();
    String stackTrace = getStackTrace(event);

    alertingService.sendAlerts(alertLevel, instant, threadName, threadId, formattedMessage,
        stackTrace);
  }

  private String getStackTrace(final LogEvent event) {
    return event.getThrown() == null ? null : ExceptionUtils.getStackTrace(event.getThrown());
  }

  @Override
  public void start() {
    LOGGER.info("Starting {}.", this);
    LOGGER.debug("Adding {} to {}.", this, alertingSessionRegistry);
    alertingSessionRegistry.addAlertingSessionListener(this);
    super.start();
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping {}.", this);

    // stop LogEvents from coming to this appender
    super.stop();

    alertingSessionRegistry.removeAlertingSessionListener(this);

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
    return Collections.unmodifiableList(events);
  }

  @Override
  public synchronized void createSession(final AlertingService alertingService) {
    LOGGER.info("Creating session in {} with {}.", this, alertingService);
    setAlertingService(alertingService);
  }

  @Override
  public synchronized void startSession() {
    LOGGER.info("Starting session in {}.", this);
    resume();
  }

  @Override
  public synchronized void stopSession() {
    LOGGER.info("Stopping session in {}.", this);
    pause();
    setAlertingService(NullAlertingService.get());
  }

  @Override
  public String toString() {
    return getClass().getName() + "@" + Integer.toHexString(hashCode()) + ":" + getName()
        + " {alertingService=" + getAlertingService()
        + ", paused=" + paused
        + ", alertingSessionRegistry=" + alertingSessionRegistry + ", debug=" + debug + "}";
  }

  @VisibleForTesting
  void setAlertingService(final AlertingService alertingService) {
    alertingServiceRef.set(alertingService);
  }

  @VisibleForTesting
  AlertingService getAlertingService() {
    return alertingServiceRef.get();
  }
}
