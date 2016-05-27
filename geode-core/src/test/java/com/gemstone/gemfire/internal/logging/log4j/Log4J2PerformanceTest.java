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
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.logging.LoggingPerformanceTestCase;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.test.junit.categories.PerformanceTest;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import java.io.*;
import java.net.URL;

@Category(PerformanceTest.class)
@Ignore("Tests have no assertions")
public class Log4J2PerformanceTest extends LoggingPerformanceTestCase {

  protected static final int DEFAULT_LOG_FILE_SIZE_LIMIT = Integer.MAX_VALUE;
  protected static final int DEFAULT_LOG_FILE_COUNT_LIMIT = 20;
  
  protected static final String SYS_LOG_FILE = "gemfire-log-file";
  protected static final String SYS_LOG_FILE_SIZE_LIMIT = "gemfire-log-file-size-limit";
  protected static final String SYS_LOG_FILE_COUNT_LIMIT = "gemfire-log-file-count-limit";
  
  private File config = null;
  
  public Log4J2PerformanceTest(String name) {
    super(name);
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    this.config = null; // leave this file in place for now
  }
  
  protected void writeConfigFile(final File file) throws FileNotFoundException {
    final PrintWriter pw = new PrintWriter(new FileOutputStream(file));
    pw.println();
    pw.close();
  }

  protected void setPropertySubstitutionValues(final String logFile, 
                                               final int logFileSizeLimitMB,
                                               final int logFileCountLimit) {
      if (logFileSizeLimitMB < 0) {
        throw new IllegalArgumentException("logFileSizeLimitMB must be zero or positive integer");
      }
      if (logFileCountLimit < 0) {
        throw new IllegalArgumentException("logFileCountLimit must be zero or positive integer");
      }
      
      // flip \ to / if any exist
      final String logFileValue = logFile.replace("\\", "/");
      // append MB
      final String logFileSizeLimitMBValue = new StringBuilder(String.valueOf(logFileSizeLimitMB)).append(" MB").toString();
      final String logFileCountLimitValue = new StringBuilder(String.valueOf(logFileCountLimit)).toString();
      
      System.setProperty(SYS_LOG_FILE, logFileValue);
      System.setProperty(SYS_LOG_FILE_SIZE_LIMIT, logFileSizeLimitMBValue);
      System.setProperty(SYS_LOG_FILE_COUNT_LIMIT, logFileCountLimitValue);
  }
  
  protected Logger createLogger() throws IOException {
    // create configuration with log-file and log-level
    this.configDirectory = new File(getUniqueName());
    this.configDirectory.mkdir();
    assertTrue(this.configDirectory.isDirectory() && this.configDirectory.canWrite());

    // copy the log4j2-test.xml to the configDirectory
    //final URL srcURL = getClass().getResource("/com/gemstone/gemfire/internal/logging/log4j/log4j2-test.xml");
    final URL srcURL = getClass().getResource("log4j2-test.xml");
    final File src = new File(srcURL.getFile());
    FileUtils.copyFileToDirectory(src, this.configDirectory);
    this.config = new File(this.configDirectory, "log4j2-test.xml");
    assertTrue(this.config.exists());

    this.logFile = new File(this.configDirectory, DistributionConfig.GEMFIRE_PREFIX + "log");
    final String logFilePath = IOUtils.tryGetCanonicalPathElseGetAbsolutePath(logFile);
    final String logFileName = FileUtil.stripOffExtension(logFilePath);
    setPropertySubstitutionValues(logFileName, DEFAULT_LOG_FILE_SIZE_LIMIT, DEFAULT_LOG_FILE_COUNT_LIMIT);
    
    final String configPath = "file://" + IOUtils.tryGetCanonicalPathElseGetAbsolutePath(this.config);
    System.setProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY, configPath);
    
    final Logger logger = LogManager.getLogger();
    return logger;
  }
  
  protected PerformanceLogger createPerformanceLogger() throws IOException {
    final Logger logger = createLogger();
    
    final PerformanceLogger perfLogger = new PerformanceLogger() {
      @Override
      public void log(String message) {
        logger.info(message);
      }
      @Override
      public boolean isEnabled() {
        return logger.isEnabled(Level.INFO);
      }
    };
    
    return perfLogger;
  }

  @Override
  public void testCountBasedLogging() throws Exception {
    super.testCountBasedLogging();
  }

  @Override
  public void testTimeBasedLogging() throws Exception {
    super.testTimeBasedLogging();
  }

  @Override
  public void testCountBasedIsEnabled() throws Exception {
    super.testCountBasedIsEnabled();
  }

  @Override
  public void testTimeBasedIsEnabled() throws Exception {
    super.testTimeBasedIsEnabled();
  }
}
