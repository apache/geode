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

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Date;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.admin.Alert;
import org.apache.geode.internal.admin.remote.AlertListenerMessage;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.tcp.ReenteredConnectException;

/**
 * A Log4j Appender which will notify listeners whenever a message of the requested level is written
 * to the log file.
 * 
 */
public class AlertAppender extends AbstractAppender implements PropertyChangeListener {
  private static final String APPENDER_NAME = AlertAppender.class.getName();
  private static final Logger logger = LogService.getLogger();
  private static final AlertAppender instance = createAlertAppender();

  /** Is this thread in the process of alerting? */
  private static final ThreadLocal<Boolean> alerting = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };

  // Listeners are ordered with the narrowest levels (e.g. FATAL) at the end
  private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<Listener>();

  private final AppenderContext appenderContext = LogService.getAppenderContext();

  private final AtomicReference<InternalDistributedSystem> systemRef = new AtomicReference<>();

  // This can be set by a loner distributed sytem to disable alerting
  private volatile boolean alertingDisabled = false;

  private static AlertAppender createAlertAppender() {
    AlertAppender alertAppender = new AlertAppender();
    alertAppender.start();
    return alertAppender;
  }

  private AlertAppender() {
    super(APPENDER_NAME, null, PatternLayout.createDefaultLayout());
  }

  public void onConnect(final InternalDistributedSystem system) {
    this.systemRef.set(system);
  }

  public static AlertAppender getInstance() {
    return instance;
  }

  /**
   * Returns true if the current thread is in the process of delivering an alert message.
   */
  public static boolean isThreadAlerting() {
    return alerting.get();
  }

  public boolean isAlertingDisabled() {
    return alertingDisabled;
  }

  public void setAlertingDisabled(final boolean alertingDisabled) {
    this.alertingDisabled = alertingDisabled;
  }

  public static void setIsAlerting(boolean isAlerting) {
    alerting.set(isAlerting ? Boolean.TRUE : Boolean.FALSE);
  }

  /**
   * This method is optimized with the assumption that at least one listener has set a level which
   * requires that the event be sent. This is ensured by modifying the appender's configuration
   * whenever a listener is added or removed.
   */
  @Override
  public void append(final LogEvent event) {
    if (this.alertingDisabled) {
      return;
    }

    // If already appending then don't send to avoid infinite recursion
    if ((alerting.get())) {
      return;
    }
    setIsAlerting(true);

    try {

      final boolean isDebugEnabled = logger.isDebugEnabled();
      if (isDebugEnabled) {
        logger.debug("Delivering an alert event: {}", event);
      }

      InternalDistributedSystem ds = this.systemRef.get();
      if (ds == null) {
        // Use info level to avoid triggering another alert
        logger.info("Did not append alert event because the distributed system is set to null.");
        return;
      }
      DistributionManager distMgr = (DistributionManager) ds.getDistributionManager();

      final int intLevel = logLevelToAlertLevel(event.getLevel().intLevel());
      final Date date = new Date(event.getTimeMillis());
      final String threadName = event.getThreadName();
      final String logMessage = event.getMessage().getFormattedMessage();
      final String stackTrace =
          (event.getThrown() == null) ? null : ExceptionUtils.getStackTrace(event.getThrown());
      final String connectionName = ds.getConfig().getName();

      for (Listener listener : this.listeners) {
        if (event.getLevel().intLevel() > listener.getLevel().intLevel()) {
          break;
        }

        try {
          AlertListenerMessage alertMessage =
              AlertListenerMessage.create(listener.getMember(), intLevel, date, connectionName,
                  threadName, Thread.currentThread().getId(), logMessage, stackTrace);

          if (listener.getMember().equals(distMgr.getDistributionManagerId())) {
            if (isDebugEnabled) {
              logger.debug("Delivering local alert message: {}, {}, {}, {}, {}, [{}], [{}].",
                  listener.getMember(), intLevel, date, connectionName, threadName, logMessage,
                  stackTrace);
            }
            alertMessage.process(distMgr);
          } else {
            if (isDebugEnabled) {
              logger.debug("Delivering remote alert message: {}, {}, {}, {}, {}, [{}], [{}].",
                  listener.getMember(), intLevel, date, connectionName, threadName, logMessage,
                  stackTrace);
            }
            distMgr.putOutgoing(alertMessage);
          }
        } catch (ReenteredConnectException e) {
          // OK. We can't send to this recipient because we're in the middle of
          // trying to connect to it.
        }
      }
    } finally {
      setIsAlerting(false);
    }
  }

  public synchronized void addAlertListener(final DistributedMember member, final int alertLevel) {
    final Level level = LogService.toLevel(alertLevelToLogLevel(alertLevel));

    if (this.listeners.size() == 0) {
      this.appenderContext.getLoggerContext().addPropertyChangeListener(this);
    }

    addListenerToSortedList(new Listener(level, member));

    LoggerConfig loggerConfig = this.appenderContext.getLoggerConfig();
    loggerConfig.addAppender(this, this.listeners.get(0).getLevel(), null);
    if (logger.isDebugEnabled()) {
      logger.debug("Added/Replaced alert listener for member {} at level {}", member, level);
    }
  }

  public synchronized boolean removeAlertListener(final DistributedMember member) {
    final boolean memberWasFound = this.listeners.remove(new Listener(null, member));

    if (memberWasFound) {
      if (this.listeners.size() == 0) {
        this.appenderContext.getLoggerContext().removePropertyChangeListener(this);
        this.appenderContext.getLoggerConfig().removeAppender(APPENDER_NAME);

      } else {
        LoggerConfig loggerConfig = this.appenderContext.getLoggerConfig();
        loggerConfig.addAppender(this, this.listeners.get(0).getLevel(), null);
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Removed alert listener for member {}", member);
      }
    }

    return memberWasFound;
  }

  public synchronized boolean hasAlertListener(final DistributedMember member,
      final int alertLevel) {
    final Level level = LogService.toLevel(alertLevelToLogLevel(alertLevel));

    for (Listener listener : this.listeners) {
      if (listener.getMember().equals(member) && listener.getLevel().equals(level)) {
        return true;
      }
    }

    // Special case for alert level Alert.OFF (NONE_LEVEL), because we can never have an actual
    // listener with
    // this level (see AlertLevelChangeMessage.process()).
    if (alertLevel == Alert.OFF) {
      for (Listener listener : this.listeners) {
        if (listener.getMember().equals(member)) {
          return false;
        }
      }
      return true;
    }

    return false;
  }

  @Override
  public synchronized void propertyChange(final PropertyChangeEvent evt) {
    if (logger.isDebugEnabled()) {
      logger.debug("Responding to a property change event. Property name is {}.",
          evt.getPropertyName());
    }
    if (evt.getPropertyName().equals(LoggerContext.PROPERTY_CONFIG)) {
      LoggerConfig loggerConfig = this.appenderContext.getLoggerConfig();
      if (!loggerConfig.getAppenders().containsKey(APPENDER_NAME)) {
        loggerConfig.addAppender(this, this.listeners.get(0).getLevel(), null);
      }
    }
  }

  /**
   * Will add (or replace) a listener to the list of sorted listeners such that listeners with a
   * narrower level (e.g. FATAL) will be at the end of the list.
   * 
   * @param listener The listener to add to the list.
   */
  private void addListenerToSortedList(final Listener listener) {
    if (this.listeners.contains(listener)) {
      this.listeners.remove(listener);
    }

    for (int i = 0; i < this.listeners.size(); i++) {
      if (listener.getLevel().compareTo(this.listeners.get(i).getLevel()) >= 0) {
        this.listeners.add(i, listener);
        return;
      }
    }

    this.listeners.add(listener);
  }

  /**
   * Converts an int alert level to an int log level.
   * 
   * @param alertLevel The int value for the alert level
   * @return The int value for the matching log level
   * @throws java.lang.IllegalArgumentException If there is no matching log level
   */
  public static int alertLevelToLogLevel(final int alertLevel) {
    switch (alertLevel) {
      case Alert.SEVERE:
        return Level.FATAL.intLevel();
      case Alert.ERROR:
        return Level.ERROR.intLevel();
      case Alert.WARNING:
        return Level.WARN.intLevel();
      case Alert.OFF:
        return Level.OFF.intLevel();
    }

    throw new IllegalArgumentException("Unknown Alert level [" + alertLevel + "].");
  }

  /**
   * Converts an int log level to an int alert level.
   * 
   * @param logLevel The int value for the log level
   * @return The int value for the matching alert level
   * @throws java.lang.IllegalArgumentException If there is no matching log level
   */
  public static int logLevelToAlertLevel(final int logLevel) {
    if (logLevel == Level.FATAL.intLevel()) {
      return Alert.SEVERE;
    } else if (logLevel == Level.ERROR.intLevel()) {
      return Alert.ERROR;
    } else if (logLevel == Level.WARN.intLevel()) {
      return Alert.WARNING;
    } else if (logLevel == Level.OFF.intLevel()) {
      return Alert.OFF;
    }

    throw new IllegalArgumentException("Unknown Log level [" + logLevel + "].");
  }

  public synchronized void shuttingDown() {
    this.listeners.clear();
    this.appenderContext.getLoggerContext().removePropertyChangeListener(this);
    this.appenderContext.getLoggerConfig().removeAppender(APPENDER_NAME);
    this.systemRef.set(null);
  }

  /**
   * Simple value object which holds an InteralDistributedMember and Level pair.
   */
  static class Listener {
    private Level level;
    private DistributedMember member;

    public Level getLevel() {
      return this.level;
    }

    public DistributedMember getMember() {
      return this.member;
    }

    Listener(final Level level, final DistributedMember member) {
      this.level = level;
      this.member = member;
    }

    /**
     * Never used, but maintain the hashCode/equals contract.
     */
    @Override
    public int hashCode() {
      return 31 + ((this.member == null) ? 0 : this.member.hashCode());
    }

    /**
     * Ignore the level when determining equality.
     */
    @Override
    public boolean equals(Object other) {
      return (this.member.equals(((Listener) other).member)) ? true : false;
    }

    @Override
    public String toString() {
      return "Listener [level=" + this.level + ", member=" + this.member + "]";
    }
  }
}
