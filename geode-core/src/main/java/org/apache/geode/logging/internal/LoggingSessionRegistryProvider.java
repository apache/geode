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
package org.apache.geode.logging.internal;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.logging.spi.LogFile;
import org.apache.geode.logging.spi.LoggingSessionListener;
import org.apache.geode.logging.spi.LoggingSessionRegistry;
import org.apache.geode.logging.spi.SessionContext;

public class LoggingSessionRegistryProvider
    implements LoggingSessionRegistry, LoggingSessionNotifier {

  @MakeNotStatic
  private static final LoggingSessionRegistryProvider INSTANCE =
      new LoggingSessionRegistryProvider();

  public static LoggingSessionRegistryProvider get() {
    return INSTANCE;
  }

  private final Set<LoggingSessionListener> listeners;

  @VisibleForTesting
  LoggingSessionRegistryProvider() {
    listeners = new LinkedHashSet<>();
  }

  /**
   * Adds the {@code LoggingSessionListener}.
   */
  public void addLoggingSessionListener(final LoggingSessionListener listener) {
    listeners.add(listener);
  }

  /**
   * Removes the {@code LoggingSessionListener}.
   */
  public void removeLoggingSessionListener(final LoggingSessionListener listener) {
    listeners.remove(listener);
  }

  /**
   * Removes all currently registered {@code LoggingSessionListener}s.
   */
  public void clear() {
    listeners.clear();
  }

  /**
   * Provides {@code createSession} notification to all registered listeners.
   */
  public void createSession(final SessionContext sessionContext) {
    for (LoggingSessionListener listener : listeners) {
      listener.createSession(sessionContext);
    }
  }

  /**
   * Provides {@code startSession} notification to all registered listeners.
   */
  public void startSession() {
    for (LoggingSessionListener listener : listeners) {
      listener.startSession();
    }
  }

  /**
   * Provides {@code stopSession} notification to all registered listeners.
   */
  public void stopSession() {
    for (LoggingSessionListener listener : listeners) {
      listener.stopSession();
    }
  }

  /**
   * Returns the system {@link LogFile} from any {@link LoggingSessionListener} that offers it.
   */
  public Optional<LogFile> getLogFile() {
    for (LoggingSessionListener listener : listeners) {
      if (listener.getLogFile().isPresent()) {
        return listener.getLogFile();
      }
    }
    return Optional.empty();
  }
}
