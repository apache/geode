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

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.logging.LogService;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * Provides the LoggerContext and LoggerConfig for GemFire appenders to attach
 * to. These appenders include AlertAppender and LogWriterAppender.
 * 
 */
public class AppenderContext {
  
  /** "com.gemstone" is a good alternative for limiting alerts to just gemstone packages, otherwise ROOT is used */
  public static final String LOGGER_PROPERTY = DistributionConfig.GEMFIRE_PREFIX + "logging.appenders.LOGGER";
  
  public AppenderContext() {
    this(System.getProperty(LOGGER_PROPERTY, ""));
  }
  
  private final String name;
  
  public AppenderContext(final String name) {
    this.name = name;
  }
  
  public String getName() {
    return this.name;
  }
  
  public LoggerContext getLoggerContext() {
    return getLogger().getContext();
  }

  public LoggerConfig getLoggerConfig() {
    final Logger logger = getLogger();
    final LoggerContext context = logger.getContext();
    return context.getConfiguration().getLoggerConfig(logger.getName());
  }
  
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("AppenderContext:");
    if ("".equals(this.name)) {
      sb.append("<ROOT>");
    } else {
      sb.append(this.name);
    }
    return sb.toString();
  }
  
  private Logger getLogger() {
    Logger logger = null;
    if ("".equals(name)) {
      logger = (Logger)LogService.getRootLogger();
    } else {
      logger = (Logger)((FastLogger)LogService.getLogger(name)).getExtendedLogger();
    }
    return logger;
  }
}
