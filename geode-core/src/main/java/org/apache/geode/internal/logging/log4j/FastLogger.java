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
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.logging.log4j.util.MessageSupplier;
import org.apache.logging.log4j.util.Supplier;

/**
 * Overrides is-enabled checks for log levels below {@code INFO} to avoid performance penalties when
 * the log level is {@code INFO} or above.
 */
public class FastLogger extends ExtendedLoggerWrapper {
  private static final long serialVersionUID = 7084130827962463327L;

  private static volatile boolean delegating = true;

  public FastLogger(final Logger logger) {
    this((ExtendedLogger) logger, logger.getName(), logger.getMessageFactory());
  }

  public FastLogger(final ExtendedLogger logger, final String name,
      final MessageFactory messageFactory) {
    super(logger, name, messageFactory);
  }

  public static void setDelegating(final boolean newValue) {
    StatusLogger.getLogger().debug("Setting delegating to {}", newValue);
    delegating = newValue;
  }

  @Override
  public boolean isDebugEnabled() {
    return delegating && super.isDebugEnabled();
  }

  @Override
  public boolean isDebugEnabled(final Marker marker) {
    return delegating && super.isDebugEnabled(marker);
  }

  @Override
  public boolean isTraceEnabled() {
    return delegating && super.isTraceEnabled();
  }

  @Override
  public boolean isTraceEnabled(final Marker marker) {
    return delegating && super.isTraceEnabled(marker);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, Message message, Throwable t) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, CharSequence message, Throwable t) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, Object message, Throwable t) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Throwable t) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, t);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object... params) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, params);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, p0);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, p0, p1);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, p0, p1, p2);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, p0, p1, p2, p3);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3,
      Object p4) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3,
      Object p4, Object p5) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3,
      Object p4, Object p5, Object p6) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3,
      Object p4, Object p5, Object p6, Object p7) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3,
      Object p4, Object p5, Object p6, Object p7, Object p8) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public boolean isEnabled(Level level, Marker marker, String message, Object p0, Object p1,
      Object p2, Object p3,
      Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return false;
    }
    return super.isEnabled(level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, Message message, Throwable t) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, t);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, CharSequence message,
      Throwable t) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, t);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, Object message, Throwable t) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, t);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Throwable t) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, t);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message,
      Object... params) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, params);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, p0);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0,
      Object p1) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, p0, p1);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0,
      Object p1, Object p2) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, p0, p1, p2);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0,
      Object p1, Object p2,
      Object p3) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0,
      Object p1, Object p2,
      Object p3, Object p4) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3, p4);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0,
      Object p1, Object p2,
      Object p3, Object p4, Object p5) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3, p4, p5);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0,
      Object p1, Object p2,
      Object p3, Object p4, Object p5, Object p6) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3, p4, p5, p6);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0,
      Object p1, Object p2,
      Object p3, Object p4, Object p5, Object p6, Object p7) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0,
      Object p1, Object p2,
      Object p3, Object p4, Object p5, Object p6, Object p7, Object p8) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message, Object p0,
      Object p1, Object p2,
      Object p3, Object p4, Object p5, Object p6, Object p7, Object p8, Object p9) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, p0, p1, p2, p3, p4, p5, p6, p7, p8, p9);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, MessageSupplier msgSupplier,
      Throwable t) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, msgSupplier, t);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, String message,
      Supplier<?>... paramSuppliers) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, message, paramSuppliers);
  }

  @Override
  public void logIfEnabled(String fqcn, Level level, Marker marker, Supplier<?> msgSupplier,
      Throwable t) {
    if (isLevelDisabledAndNotDelegating(level)) {
      return;
    }
    super.logIfEnabled(fqcn, level, marker, msgSupplier, t);
  }

  boolean isDelegating() {
    return delegating;
  }

  ExtendedLogger getExtendedLogger() {
    return super.logger;
  }

  boolean isLevelDisabledAndNotDelegating(Level level) {
    return level.intLevel() > Level.INFO.intLevel() && !delegating;
  }
}
