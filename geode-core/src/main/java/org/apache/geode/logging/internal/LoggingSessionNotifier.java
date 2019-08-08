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

import java.util.Optional;

import org.apache.geode.logging.spi.LogFile;
import org.apache.geode.logging.spi.LoggingSessionListener;
import org.apache.geode.logging.spi.SessionContext;

public interface LoggingSessionNotifier {

  /**
   * Removes all currently registered {@code LoggingSessionListener}s.
   */
  void clear();

  /**
   * Provides {@code createSession} notification to all registered listeners.
   */
  void createSession(final SessionContext sessionContext);

  /**
   * Provides {@code startSession} notification to all registered listeners.
   */
  void startSession();

  /**
   * Provides {@code stopSession} notification to all registered listeners.
   */
  void stopSession();

  /**
   * Returns the system {@link LogFile} from any {@link LoggingSessionListener} that offers it.
   */
  Optional<LogFile> getLogFile();
}
