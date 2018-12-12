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
package org.apache.geode.internal.logging;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.Banner;
import org.apache.geode.internal.ConfigSource;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.logging.log4j.LogWriterLogger;

/**
 * Creates LogWriterLogger instances.
 *
 */
public class LogWriterFactory {

  // LOG: RemoteGfManagerAgent and CacheCreation use this when there's no InternalDistributedSystem
  public static InternalLogWriter toSecurityLogWriter(final InternalLogWriter logWriter) {
    return new SecurityLogWriter(logWriter.getLogWriterLevel(), logWriter);
  }

  /**
   * Creates the log writer for a distributed system based on the system's parsed configuration. The
   * initial banner and messages are also entered into the log by this method.
   *
   * @param isLoner Whether the distributed system is a loner or not
   * @param isSecure Whether a logger for security related messages has to be created
   * @param config The DistributionConfig for the target distributed system
   * @param logConfig if true log the configuration
   */
  public static InternalLogWriter createLogWriterLogger(final boolean isLoner,
      final boolean isSecure, final LogConfig config, final boolean logConfig) {

    // if isSecurity then use "org.apache.geode.security" else use "org.apache.geode"
    String name = null;
    if (isSecure) {
      name = LogService.SECURITY_LOGGER_NAME;
    } else {
      name = LogService.MAIN_LOGGER_NAME;
    }

    // create the LogWriterLogger
    final LogWriterLogger logger =
        LogService.createLogWriterLogger(name, config.getName(), isSecure);

    if (isSecure) {
      logger.setLogWriterLevel(((DistributionConfig) config).getSecurityLogLevel());
    } else {
      boolean defaultSource = false;
      if (config instanceof DistributionConfig) {
        ConfigSource source = ((DistributionConfig) config).getConfigSource(LOG_LEVEL);
        if (source == null) {
          defaultSource = true;
        }
      }
      if (!defaultSource) {
        // LOG: fix bug #51709 by not setting if log-level was not specified
        // LOG: let log4j2.xml specify log level which defaults to INFO
        logger.setLogWriterLevel(config.getLogLevel());
      }
    }

    // log the banner
    if (!Boolean.getBoolean(InternalLocator.INHIBIT_DM_BANNER)
        && InternalDistributedSystem.getReconnectAttemptCounter() == 0 // avoid filling up logs
                                                                       // during auto-reconnect
        && !isSecure // && !isLoner /* do this on a loner to fix bug 35602 */
        && logConfig) {
      logger.info(LogMarker.CONFIG_MARKER, Banner.getString(null));
    } else {
      logger.debug("skipping banner - " + InternalLocator.INHIBIT_DM_BANNER + " is set to true");
    }

    // log the config
    if (logConfig && !isLoner) {
      logger.info(LogMarker.CONFIG_MARKER, "Startup Configuration: {}",
          config.toLoggerString());
    }
    return logger;
  }
}
