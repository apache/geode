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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Integration tests for FastLogger when using the default log4j2 config for GemFire.
 * 
 */
@Category(IntegrationTest.class)
public class FastLoggerWithDefaultConfigJUnitTest {

  private static final String TEST_LOGGER_NAME = FastLogger.class.getPackage().getName();
  
  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  private Logger logger;
  
  @Before
  public void setUp() throws Exception {
    System.clearProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
    LogService.reconfigure();
  }
  
  /**
   * System property "log4j.configurationFile" should be "/com/gemstone/gemfire/internal/logging/log4j/log4j2-default.xml"
   */
  @Test
  public void configurationFilePropertyIsDefaultConfig() {
    assertThat(System.getProperty(ConfigurationFactory.CONFIGURATION_FILE_PROPERTY), isEmptyOrNullString());
  }
  
  /**
   * LogService isUsingGemFireDefaultConfig should be true
   */
  @Test
  public void isUsingGemFireDefaultConfig() {
    assertThat(LogService.isUsingGemFireDefaultConfig(), is(true));
  }
  
  /**
   * LogService getLogger should return loggers wrapped in FastLogger
   */
  @Test
  public void logServiceReturnsFastLoggers() {
    this.logger = LogService.getLogger(TEST_LOGGER_NAME);
    
    assertThat(this.logger, is(instanceOf(FastLogger.class)));
  }
  
  /**
   * FastLogger isDelegating should be false
   */
  @Test
  public void isDelegatingShouldBeFalse() {
    this.logger = LogService.getLogger(TEST_LOGGER_NAME);
    
    assertThat(((FastLogger)this.logger).isDelegating(), is(false));
  }
}
