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

import static org.apache.geode.internal.logging.log4j.AlertLevelConverter.hasAlertLevel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.alerting.AlertLevel;
import org.apache.geode.internal.alerting.AlertMessaging;
import org.apache.geode.internal.alerting.AlertingAction;
import org.apache.geode.internal.alerting.AlertingProvider;
import org.apache.geode.internal.alerting.AlertingProviderRegistry;

@Plugin(name = AlertAppender.PLUGIN_NAME, category = Core.CATEGORY_NAME,
    elementType = Appender.ELEMENT_TYPE, printObject = true)
@SuppressWarnings("unused")
public class AlertAppender extends AbstractAppender
    implements PausableAppender, DebuggableAppender, AlertingProvider {

  public static final String PLUGIN_NAME = "GeodeAlert";

  private static final boolean START_PAUSED_BY_DEFAULT = true;

  @MakeNotStatic
  private static final AtomicReference<AlertAppender> instanceRef = new AtomicReference<>();

  private final AtomicReference<AlertMessaging> alertMessagingRef = new AtomicReference<>();

  // Listeners are ordered with the narrowest levels (e.g. FATAL) at the end
  private final CopyOnWriteArrayList<AlertListener> listeners;

  private final AlertingProviderRegistry alertingProviderRegistry;

  private final boolean debug;
  private final List<LogEvent> events;

  private volatile boolean paused;

  protected AlertAppender(final String name,
      final Layout<? extends Serializable> layout,
      final Filter filter) {
    this(name, layout, filter, AlertingProviderRegistry.get(), START_PAUSED_BY_DEFAULT, false);
  }

  protected AlertAppender(final String name,
      final Layout<? extends Serializable> layout,
      final Filter filter,
      final AlertingProviderRegistry alertingProviderRegistry,
      final boolean startPaused,
      final boolean debug) {
    super(name, filter, layout);
    listeners = new CopyOnWriteArrayList<>();
    this.alertingProviderRegistry = alertingProviderRegistry;
    this.debug = debug;
    if (debug) {
      events = Collections.synchronizedList(new ArrayList<>());
    } else {
      events = Collections.emptyList();
    }
    paused = true;
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
      instanceRef.set(new AlertAppender(getName(), layout, getFilter(),
          AlertingProviderRegistry.get(), startPaused, debug));
      return instanceRef.get();
    }
  }

  @Override
  public void append(final LogEvent event) {
    LOGGER.trace("Handling append of {} in {}.", event, this);
    if (isPaused()) {
      LOGGER.trace("Skipping append of {} because {} is paused.", event, this);
      return;
    }
    if (!hasAlertLevel(event.getLevel())) {
      LOGGER.trace("Skipping append of {} because level is {}.", event, event.getLevel());
      return;
    }
    if (AlertingAction.isThreadAlerting()) {
      // If already appending then don't send to avoid infinite recursion
      LOGGER.trace("Skipping append of {} because {} is alerting.", event, Thread.currentThread());
      return;
    }
    AlertingAction.execute(() -> doAppend(event));
  }

  private void doAppend(final LogEvent event) {
    sendAlertMessage(event);
    if (debug) {
      events.add(event);
    }
  }

  private void sendAlertMessage(final LogEvent event) {
    AlertMessaging alertMessaging = alertMessagingRef.get();
    if (alertMessaging == null || listeners.isEmpty()) {
      LOGGER.trace("Skipping alert messaging for {} because listeners is empty.", event);
      return;
    }

    AlertLevel alertLevel = AlertLevelConverter.fromLevel(event.getLevel());
    Date date = new Date(event.getTimeMillis());
    String threadName = event.getThreadName();
    String formattedMessage = event.getMessage().getFormattedMessage();
    String stackTrace = getStackTrace(event);

    for (AlertListener listener : listeners) {
      if (event.getLevel().intLevel() > listener.getLevel().intLevel()) {
        break;
      }

      LOGGER.trace("Sending alert message for {} to {}.", event, listener.getMember());
      alertMessaging.sendAlert(listener.getMember(), alertLevel, date, threadName, formattedMessage,
          stackTrace);
    }
  }

  private String getStackTrace(final LogEvent event) {
    return event.getThrown() == null ? null : ExceptionUtils.getStackTrace(event.getThrown());
  }

  @Override
  public void start() {
    LOGGER.info("Starting {}.", this);
    LOGGER.debug("Registering {} with AlertingProviderRegistry.", this);
    try {
      alertingProviderRegistry.registerAlertingProvider(this);
    } finally {
      super.start();
    }
  }

  @Override
  public void stop() {
    LOGGER.info("Stopping {}.", this);

    // stop LogEvents from coming to this appender
    super.stop();

    // unregister as provider
    cleanUp(true);

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
  public synchronized void createSession(final AlertMessaging alertMessaging) {
    LOGGER.info("Creating session in {} with {}.", this, alertMessaging);
    setAlertMessaging(alertMessaging);
  }

  @Override
  public synchronized void startSession() {
    LOGGER.info("Starting session in {}.", this);
    resume();
  }

  @Override
  public synchronized void stopSession() {
    LOGGER.info("Stopping session in {}.", this);
    cleanUp(false);
  }

  private synchronized void cleanUp(boolean unregister) {
    pause();
    if (unregister) {
      LOGGER.debug("Unregistering {} with AlertingProviderRegistry.", this);
      alertingProviderRegistry.unregisterAlertingProvider(this);
    }
    listeners.clear();
    setAlertMessaging(null);
  }

  void setAlertMessaging(final AlertMessaging alertMessaging) {
    alertMessagingRef.set(alertMessaging);
  }

  AlertMessaging getAlertMessaging() {
    return alertMessagingRef.get();
  }

  @Override
  public synchronized void addAlertListener(final DistributedMember member,
      final AlertLevel alertLevel) {
    if (alertLevel == AlertLevel.NONE) {
      return;
    }
    Level level = AlertLevelConverter.toLevel(alertLevel);
    AlertListener listener = new AlertListener(level, member);

    // Add (or replace) a listener to the list of sorted listeners such that listeners with a
    // narrower level (e.g. FATAL) will be at the end of the list.
    listeners.remove(listener);
    for (int i = 0; i < listeners.size(); i++) {
      if (listener.getLevel().compareTo(listeners.get(i).getLevel()) >= 0) {
        listeners.add(i, listener);
        return;
      }
    }
    listeners.add(listener);

    LOGGER.debug("Added/Replaced alert listener for member {} at level {}.", member, level);
  }

  @Override
  public synchronized boolean removeAlertListener(final DistributedMember member) {
    boolean memberWasFound = listeners.remove(new AlertListener(null, member));
    if (memberWasFound) {
      LOGGER.debug("Removed alert listener for member {}.", member);
    }
    return memberWasFound;
  }

  @Override
  public synchronized boolean hasAlertListener(final DistributedMember member,
      final AlertLevel alertLevel) {
    Level level = AlertLevelConverter.toLevel(alertLevel);

    for (AlertListener listener : listeners) {
      if (listener.getMember().equals(member) && listener.getLevel().equals(level)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public String toString() {
    return getClass().getName() + "@" + Integer.toHexString(hashCode()) + ":" + getName()
        + " {alertMessaging=" + alertMessagingRef.get() + ", listeners=" + listeners + ", paused="
        + paused + ", debug=" + debug + "}";
  }

  public synchronized List<AlertListener> getAlertListeners() {
    return listeners;
  }

  @VisibleForTesting
  static AlertAppender getInstance() {
    return instanceRef.get();
  }

  @VisibleForTesting
  static void setInstance(AlertAppender alertAppender) {
    instanceRef.set(alertAppender);
  }

  public static void stopSessionIfRunning() {
    AlertAppender instance = instanceRef.get();
    if (instance != null) {
      instance.stopSession();
    }
  }
}
