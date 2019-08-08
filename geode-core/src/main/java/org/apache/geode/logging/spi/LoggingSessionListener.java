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
package org.apache.geode.logging.spi;

import java.util.Optional;

import org.apache.geode.logging.internal.LoggingSession;

/**
 * Listens for state changes to a {@code LoggingSession}.
 *
 * <p>
 * Listeners include the custom Log4J2 Appenders. Appender actions in response to starting a new
 * session:
 * <ul>
 * <li>{@code GeodeConsoleAppender} pauses so that logging stops going to STDOUT.
 * <li>{@code LogWriterAppender}s (main log and security log) resume so that logging starts
 * going to the Geode log files.
 * </ul>
 * Appender actions in response to stopping a session:
 * <ul>
 * <li>{@code GeodeConsoleAppender} resumes so that logging resumes going to STDOUT.
 * <li>{@code LogWriterAppender}s (main log and security log) pause so that logging stops
 * going to the Geode log files.
 * </ul>
 */
public interface LoggingSessionListener {

  /**
   * Notifies listeners of a new logging session with new {@code LogConfig}.
   */
  void createSession(SessionContext sessionContext);

  /**
   * Starts the new logging session.
   */
  void startSession();

  /**
   * Stops the logging session.
   */
  void stopSession();

  /**
   * Optional: provide {@link LogFile} to {@link LoggingSession}.
   */
  Optional<LogFile> getLogFile();
}
