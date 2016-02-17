/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.logging.log4j;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;
import org.apache.logging.log4j.status.StatusLogger;

/**
 * Overrides is-enabled checks for log levels below INFO to avoid performance
 * penalties when the log level is INFO or above. If delegating is true
 * then it will always delegate to ExtendedLoggerWrapper for is-enabled checks.
 * 
 */
public class FastLogger extends ExtendedLoggerWrapper {
  private static final long serialVersionUID = 7084130827962463327L;

  private static volatile boolean delegating = true;
  
  public FastLogger(final Logger logger) {
    this((ExtendedLogger) logger, logger.getName(), logger.getMessageFactory());
  }

  public FastLogger(final ExtendedLogger logger, final String name, final MessageFactory messageFactory) {
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
  
  public boolean isDelegating() {
    return delegating;
  }
  
  public ExtendedLogger getExtendedLogger() {
    return super.logger;
  }
}
