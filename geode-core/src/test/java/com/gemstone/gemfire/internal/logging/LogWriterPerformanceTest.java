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
package com.gemstone.gemfire.internal.logging;

import static com.gemstone.gemfire.distributed.DistributedSystemConfigProperties.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionConfigImpl;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.test.junit.categories.PerformanceTest;

/**
 * Tests performance of logging when level is OFF.
 */
@Category(PerformanceTest.class)
@Ignore("Tests have no assertions")
public class LogWriterPerformanceTest extends LoggingPerformanceTestCase {

  protected Properties createGemFireProperties() {
    final Properties props = new Properties();
    this.logFile = new File(this.configDirectory, DistributionConfig.GEMFIRE_PREFIX + "log");
    final String logFilePath = IOUtils.tryGetCanonicalPathElseGetAbsolutePath(logFile);
    props.setProperty(LOG_FILE, logFilePath);
    props.setProperty(LOG_LEVEL, "info");
    return props;
  }
  
  protected void writeProperties(final Properties props, final File file) throws IOException {
    final FileOutputStream out = new FileOutputStream(file);
    try {
      props.store(out, null);
    }
    finally {
      out.close();
    }
  }

  protected LogWriter createLogWriter() {
    final Properties props = createGemFireProperties();
    
    // create configuration with log-file and log-level
    //this.configDirectory = new File(getUniqueName());
    
    this.configDirectory.mkdir();
    assertTrue(this.configDirectory.isDirectory() && this.configDirectory.canWrite());

    //this.gemfireProperties = new File(this.configDirectory, "gemfire.properties");
    //writeProperties(props, this.gemfireProperties);
    
    final DistributionConfig config = new DistributionConfigImpl(props, false, false);
    
    // create a LogWriter that writes to log-file
    final boolean appendToFile = false;
    final boolean isLoner = true;
    final boolean isSecurityLog = false;
    final boolean logConfig = true;
    final FileOutputStream[] fosHolder = null;
    
    final LogWriter logWriter = TestLogWriterFactory.createLogWriter(
        appendToFile, isLoner, isSecurityLog, config, logConfig, fosHolder);
    return logWriter;
  }

  @Override
  protected PerformanceLogger createPerformanceLogger() {
    final LogWriter logWriter = createLogWriter();
    
    final PerformanceLogger perfLogger = new PerformanceLogger() {
      @Override
      public void log(final String message) {
        logWriter.info(message);
      }
      @Override
      public boolean isEnabled() {
        return logWriter.infoEnabled();
      }
    };
    
    return perfLogger;
  }
}
