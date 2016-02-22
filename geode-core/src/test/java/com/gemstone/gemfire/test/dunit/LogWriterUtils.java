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
package com.gemstone.gemfire.test.dunit;

import java.util.Properties;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.LogWriterFactory;
import com.gemstone.gemfire.internal.logging.ManagerLogWriter;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterLogger;

/**
 * <code>LogWriterUtils</code> provides static utility methods to access a
 * <code>LogWriter</code> within a test. 
 * 
 * These methods can be used directly: <code>LogWriterUtils.getLogWriter(...)</code>, 
 * however, they are intended to be referenced through static import:
 *
 * <pre>
 * import static com.gemstone.gemfire.test.dunit.LogWriterUtils.*;
 *    ...
 *    LogWriter logWriter = getLogWriter(...);
 * </pre>
 *
 * Extracted from DistributedTestCase.
 * 
 * @deprecated Please use a <code>Logger</code> from  {@link LogService#getLogger()} instead.
 */
@Deprecated
public class LogWriterUtils {

  private static final Logger logger = LogService.getLogger();
  private static final LogWriterLogger oldLogger = LogWriterLogger.create(logger);
  
  protected LogWriterUtils() {
  }
  
  /**
   * Returns a <code>LogWriter</code> for logging information
   * 
   * @deprecated Please use a <code>Logger</code> from  {@link LogService#getLogger()} instead.
   */
  public static InternalLogWriter getLogWriter() {
    return LogWriterUtils.oldLogger;
  }

  /**
   * Creates a new LogWriter and adds it to the config properties. The config
   * can then be used to connect to DistributedSystem, thus providing early
   * access to the LogWriter before connecting. This call does not connect
   * to the DistributedSystem. It simply creates and returns the LogWriter
   * that will eventually be used by the DistributedSystem that connects using
   * config.
   * 
   * @param properties the DistributedSystem config properties to add LogWriter to
   * @return early access to the DistributedSystem LogWriter
   * @deprecated Please use a <code>Logger</code> from  {@link LogService#getLogger()} instead.
   */
  public static LogWriter createLogWriter(final Properties properties) {
    Properties nonDefault = properties;
    if (nonDefault == null) {
      nonDefault = new Properties();
    }
    DistributedTestUtils.addHydraProperties(nonDefault);
    
    DistributionConfig dc = new DistributionConfigImpl(nonDefault);
    LogWriter logger = LogWriterFactory.createLogWriterLogger(
        false/*isLoner*/, false/*isSecurityLog*/, dc, 
        false);        
    
    // if config was non-null, then these will be added to it...
    nonDefault.put(DistributionConfig.LOG_WRITER_NAME, logger);
    
    return logger;
  }

  /**
   * This finds the log level configured for the test run.  It should be used
   * when creating a new distributed system if you want to specify a log level.
   * 
   * @return the dunit log-level setting
   */
  public static String getDUnitLogLevel() {
    Properties dsProperties = DUnitEnv.get().getDistributedSystemProperties();
    String result = dsProperties.getProperty(DistributionConfig.LOG_LEVEL_NAME);
    if (result == null) {
      result = ManagerLogWriter.levelToString(DistributionConfig.DEFAULT_LOG_LEVEL);
    }
    return result;
  }
}
