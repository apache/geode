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
package org.apache.geode.test.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;

import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.LogWriter;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.log4j.LogWriterLogger;

/**
 * {@code LogWriterUtils} provides static utility methods to access a <code>LogWriter</code>
 * within a test.
 *
 * These methods can be used directly: {@code LogWriterUtils.getLogWriter(...)}, however, they
 * are intended to be referenced through static import:
 *
 * <pre>
 * import static org.apache.geode.test.dunit.LogWriterUtils.*;
 *    ...
 *    LogWriter logWriter = getLogWriter(...);
 * </pre>
 *
 * Extracted from DistributedTestCase.
 *
 * @deprecated Please use a {@code Logger} from {@link LogService#getLogger()} instead.
 */
@Deprecated
public class LogWriterUtils {

  private static final Logger logger = LogService.getLogger();
  private static final LogWriterLogger oldLogger = LogWriterLogger.create(logger);

  protected LogWriterUtils() {
    // nothing
  }

  /**
   * Returns a {@code LogWriter} for logging information
   *
   * @deprecated Please use a {@code Logger} from {@link LogService#getLogger()} instead.
   */
  public static LogWriter getLogWriter() {
    return oldLogger;
  }

  /**
   * This finds the log level configured for the test run. It should be used when creating a new
   * distributed system if you want to specify a log level.
   *
   * @return the dunit log-level setting
   */
  public static String getDUnitLogLevel() {
    Properties dsProperties = DUnitEnv.get().getDistributedSystemProperties();
    String result = dsProperties.getProperty(LOG_LEVEL);
    if (result == null) {
      result = LogWriterLogger.levelToString(DistributionConfig.DEFAULT_LOG_LEVEL);
    }
    return result;
  }
}
