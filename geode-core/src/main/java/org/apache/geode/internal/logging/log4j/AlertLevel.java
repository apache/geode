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

import org.apache.logging.log4j.Level;

import org.apache.geode.internal.admin.Alert;

public class AlertLevel {

  /**
   * Converts an {@link Alert} level to a LOG4J2 {@code Level}.
   *
   * @throws IllegalArgumentException if there is no matching LOG4J2 Level
   */
  static int alertLevelToLogLevel(final int alertLevel) {
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
   * Converts a LOG4J2 {@code Level} to an {@link Alert} level.
   *
   * @throws IllegalArgumentException if there is no matching Alert level
   */
  static int logLevelToAlertLevel(final Level logLevel) {
    if (logLevel == Level.FATAL) {
      return Alert.SEVERE;
    } else if (logLevel == Level.ERROR) {
      return Alert.ERROR;
    } else if (logLevel == Level.WARN) {
      return Alert.WARNING;
    } else if (logLevel == Level.OFF) {
      return Alert.OFF;
    }

    throw new IllegalArgumentException("Unknown LOG4J2 Level [" + logLevel + "].");
  }
}
