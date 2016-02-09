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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.logging.log4j.FastLogger;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterLogger;
import com.gemstone.gemfire.test.dunit.Wait;
import com.gemstone.gemfire.test.dunit.WaitCriterion;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Connects DistributedSystem and tests logging behavior at a high level.
 * 
 * @author Kirk Lund
 */
@Category(IntegrationTest.class)
public class DistributedSystemLogFileJUnitTest {
  @Rule public TestName name = new TestName();

  protected static final int TIMEOUT_MILLISECONDS = 180 * 1000; // 2 minutes
  protected static final int INTERVAL_MILLISECONDS = 100; // 100 milliseconds
  
  private DistributedSystem system;
  
  
  @Before
  public void setUp() throws Exception {
  }
  
  @After
  public void tearDown() throws Exception {
    if (this.system != null) {
      this.system.disconnect();
      this.system = null;
    }
    //We will want to remove this at some point but right now the log context 
    //does not clear out the security logconfig between tests
    LoggerContext context = (LoggerContext)LogManager.getContext(false);
    context.stop();
  }
  
  @Test
  public void testDistributedSystemCreatesLogFile() throws Exception {
    //final int port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final String logFileName = name.getMethodName() + "-system-0.log";

    final Properties properties = new Properties();
    properties.put("log-file", logFileName);
    properties.put("log-level", "config");
    properties.put("mcast-port", "0");
    properties.put("locators", "");
    properties.put("enable-network-partition-detection", "false");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    final File logFile = new File(logFileName);
    if (logFile.exists()) {
      logFile.delete();
    }
    assertFalse(logFile.exists());
    
    this.system = DistributedSystem.connect(properties);
    assertNotNull(this.system);
    
    DistributionConfig config = ((InternalDistributedSystem)this.system).getConfig();
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.CONFIG_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.CONFIG_LEVEL, config.getLogLevel());
    
    // CONFIG has been replaced with INFO -- all CONFIG statements are now logged at INFO as well
    InternalLogWriter logWriter = (InternalLogWriter) system.getLogWriter();
    assertNotNull(logWriter);
    assertTrue(logWriter instanceof LogWriterLogger);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.INFO_LEVEL) + " but was " + LogWriterImpl.levelToString(logWriter.getLogWriterLevel()),
        InternalLogWriter.INFO_LEVEL, logWriter.getLogWriterLevel());
    
    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return logFile.exists();
      }
      @Override
      public String description() {
        return "waiting for log file to exist: " + logFile;
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
    assertTrue(logFile.exists());
    
    // assert not empty
    FileInputStream fis = new FileInputStream(logFile);
    try {
      assertTrue(fis.available() > 0);
    } finally {
      fis.close();
    }
    
    final Logger logger = LogService.getLogger();
    final Logger appLogger = LogManager.getLogger("net.customer");
    assertEquals(Level.INFO, appLogger.getLevel());
    int i = 0;
    
    {
    i++;
    final String FINEST_STRING = "testLogLevels Message logged at FINEST level ["+i+"]";
    logWriter.finest(FINEST_STRING);
    assertFalse(fileContainsString(logFile, FINEST_STRING));

    i++;
    final String FINER_STRING = "testLogLevels Message logged at FINER level ["+i+"]";
    logWriter.finer(FINER_STRING);
    assertFalse(fileContainsString(logFile, FINER_STRING));

    i++;
    final String FINE_STRING = "testLogLevels Message logged at FINE level ["+i+"]";
    logWriter.fine(FINE_STRING);
    assertFalse(fileContainsString(logFile, FINE_STRING));

    i++;
    final String CONFIG_STRING = "testLogLevels Message logged at CONFIG level ["+i+"]";
    logWriter.config(CONFIG_STRING);
    assertTrue(fileContainsString(logFile, CONFIG_STRING));

    i++;
    final String INFO_STRING = "testLogLevels Message logged at INFO level ["+i+"]";
    logWriter.info(INFO_STRING);
    assertTrue(fileContainsString(logFile, INFO_STRING));

    i++;
    final String WARNING_STRING = "ExpectedStrings: testLogLevels Message logged at WARNING level ["+i+"]";
    logWriter.warning(WARNING_STRING);
    assertTrue(fileContainsString(logFile, WARNING_STRING));

    i++;
    final String ERROR_STRING = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logWriter.error(ERROR_STRING);
    assertTrue(fileContainsString(logFile, ERROR_STRING));

    i++;
    final String SEVERE_STRING = "ExpectedStrings: testLogLevels Message logged at SEVERE level ["+i+"]";
    logWriter.severe(SEVERE_STRING);
    assertTrue(fileContainsString(logFile, SEVERE_STRING));
    
    i++;
    final String TRACE_STRING = "ExpectedStrings: testLogLevels Message logged at TRACE level ["+i+"]";
    logger.trace(TRACE_STRING);
    assertFalse(fileContainsString(logFile, TRACE_STRING));
    
    i++;
    final String DEBUG_STRING = "testLogLevels Message logged at DEBUG level ["+i+"]";
    logger.debug(DEBUG_STRING);
    assertFalse(fileContainsString(logFile, DEBUG_STRING));
    
    i++;
    final String INFO_STRING_J = "testLogLevels Message logged at INFO level ["+i+"]";
    logger.info(INFO_STRING_J);
    assertTrue(fileContainsString(logFile, INFO_STRING_J));
    
    i++;
    final String WARN_STRING = "ExpectedStrings: testLogLevels Message logged at WARN level ["+i+"]";
    logger.warn(WARN_STRING);
    assertTrue(fileContainsString(logFile, WARN_STRING));
    
    i++;
    final String ERROR_STRING_J = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logger.error(ERROR_STRING_J);
    assertTrue(fileContainsString(logFile, ERROR_STRING_J));
    
    i++;
    final String FATAL_STRING = "ExpectedStrings: testLogLevels Message logged at FATAL level ["+i+"]";
    logger.fatal(FATAL_STRING);
    assertTrue(fileContainsString(logFile, FATAL_STRING));

    i++;
    final String TRACE_STRING_A = "Message logged at TRACE level ["+i+"]";
    appLogger.trace(TRACE_STRING_A);
    assertFalse(fileContainsString(logFile, TRACE_STRING_A));
    
    i++;
    final String DEBUG_STRING_A = "Message logged at DEBUG level ["+i+"]";
    appLogger.debug(DEBUG_STRING_A);
    assertFalse(fileContainsString(logFile, DEBUG_STRING_A));
    
    i++;
    final String INFO_STRING_A = "Message logged at INFO level ["+i+"]";
    appLogger.info(INFO_STRING_A);
    assertTrue(fileContainsString(logFile, INFO_STRING_A));
    
    i++;
    final String WARN_STRING_A = "ExpectedStrings: Message logged at WARN level ["+i+"]";
    appLogger.warn(WARN_STRING_A);
    assertTrue(fileContainsString(logFile, WARN_STRING_A));
    
    i++;
    final String ERROR_STRING_A = "ExpectedStrings: Message logged at ERROR level ["+i+"]";
    appLogger.error(ERROR_STRING_A);
    assertTrue(fileContainsString(logFile, ERROR_STRING_A));
    
    i++;
    final String FATAL_STRING_A = "ExpectedStrings: Message logged at FATAL level ["+i+"]";
    appLogger.fatal(FATAL_STRING_A);
    assertTrue(fileContainsString(logFile, FATAL_STRING_A));
    }
    
    // change log level to fine and verify
    config.setLogLevel(InternalLogWriter.FINE_LEVEL);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.FINE_LEVEL, config.getLogLevel());
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(logWriter.getLogWriterLevel()),
        InternalLogWriter.FINE_LEVEL, logWriter.getLogWriterLevel());

    assertEquals(Level.DEBUG, appLogger.getLevel());
    
    {
    i++;
    final String FINEST_STRING = "testLogLevels Message logged at FINEST level ["+i+"]";
    logWriter.finest(FINEST_STRING);
    assertFalse(fileContainsString(logFile, FINEST_STRING));

    i++;
    final String FINER_STRING = "testLogLevels Message logged at FINER level ["+i+"]";
    logWriter.finer(FINER_STRING);
    assertFalse(fileContainsString(logFile, FINER_STRING));

    i++;
    final String FINE_STRING = "testLogLevels Message logged at FINE level ["+i+"]";
    logWriter.fine(FINE_STRING);
    assertTrue(fileContainsString(logFile, FINE_STRING));

    i++;
    final String CONFIG_STRING = "testLogLevels Message logged at CONFIG level ["+i+"]";
    logWriter.config(CONFIG_STRING);
    assertTrue(fileContainsString(logFile, CONFIG_STRING));

    i++;
    final String INFO_STRING = "testLogLevels Message logged at INFO level ["+i+"]";
    logWriter.info(INFO_STRING);
    assertTrue(fileContainsString(logFile, INFO_STRING));

    i++;
    final String WARNING_STRING = "ExpectedStrings: testLogLevels Message logged at WARNING level ["+i+"]";
    logWriter.warning(WARNING_STRING);
    assertTrue(fileContainsString(logFile, WARNING_STRING));

    i++;
    final String ERROR_STRING = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logWriter.error(ERROR_STRING);
    assertTrue(fileContainsString(logFile, ERROR_STRING));

    i++;
    final String SEVERE_STRING = "ExpectedStrings: testLogLevels Message logged at SEVERE level ["+i+"]";
    logWriter.severe(SEVERE_STRING);
    assertTrue(fileContainsString(logFile, SEVERE_STRING));
    
    i++;
    final String TRACE_STRING = "testLogLevels Message logged at TRACE level ["+i+"]";
    logger.trace(TRACE_STRING);
    assertFalse(fileContainsString(logFile, TRACE_STRING));
    
    i++;
    final String DEBUG_STRING = "testLogLevels Message logged at DEBUG level ["+i+"]";
    logger.debug(DEBUG_STRING);
    assertTrue(fileContainsString(logFile, DEBUG_STRING));
    
    i++;
    final String INFO_STRING_J = "testLogLevels Message logged at INFO level ["+i+"]";
    logger.info(INFO_STRING_J);
    assertTrue(fileContainsString(logFile, INFO_STRING_J));
    
    i++;
    final String WARN_STRING = "ExpectedStrings: testLogLevels Message logged at WARN level ["+i+"]";
    logger.warn(WARN_STRING);
    assertTrue(fileContainsString(logFile, WARN_STRING));
    
    i++;
    final String ERROR_STRING_J = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logger.error(ERROR_STRING_J);
    assertTrue(fileContainsString(logFile, ERROR_STRING_J));
    
    i++;
    final String FATAL_STRING = "ExpectedStrings: testLogLevels Message logged at FATAL level ["+i+"]";
    logger.fatal(FATAL_STRING);
    assertTrue(fileContainsString(logFile, FATAL_STRING));

    i++;
    final String TRACE_STRING_A = "Message logged at TRACE level ["+i+"]";
    appLogger.trace(TRACE_STRING_A);
    assertFalse(fileContainsString(logFile, TRACE_STRING_A));
    
    i++;
    final String DEBUG_STRING_A = "Message logged at DEBUG level ["+i+"]";
    appLogger.debug(DEBUG_STRING_A);
    assertTrue(fileContainsString(logFile, DEBUG_STRING_A));
    
    i++;
    final String INFO_STRING_A = "Message logged at INFO level ["+i+"]";
    appLogger.info(INFO_STRING_A);
    assertTrue(fileContainsString(logFile, INFO_STRING_A));
    
    i++;
    final String WARN_STRING_A = "ExpectedStrings: Message logged at WARN level ["+i+"]";
    appLogger.warn(WARN_STRING_A);
    assertTrue(fileContainsString(logFile, WARN_STRING_A));
    
    i++;
    final String ERROR_STRING_A = "ExpectedStrings: Message logged at ERROR level ["+i+"]";
    appLogger.error(ERROR_STRING_A);
    assertTrue(fileContainsString(logFile, ERROR_STRING_A));
    
    i++;
    final String FATAL_STRING_A = "ExpectedStrings: Message logged at FATAL level ["+i+"]";
    appLogger.fatal(FATAL_STRING_A);
    assertTrue(fileContainsString(logFile, FATAL_STRING_A));
    }
    
    // change log level to error and verify
    config.setLogLevel(InternalLogWriter.ERROR_LEVEL);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.ERROR_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.ERROR_LEVEL, config.getLogLevel());
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.ERROR_LEVEL) + " but was " + LogWriterImpl.levelToString(logWriter.getLogWriterLevel()),
        InternalLogWriter.ERROR_LEVEL, logWriter.getLogWriterLevel());

    assertEquals(Level.ERROR, appLogger.getLevel());
    
    {
    i++;
    final String FINEST_STRING = "testLogLevels Message logged at FINEST level ["+i+"]";
    logWriter.finest(FINEST_STRING);
    assertFalse(fileContainsString(logFile, FINEST_STRING));

    i++;
    final String FINER_STRING = "testLogLevels Message logged at FINER level ["+i+"]";
    logWriter.finer(FINER_STRING);
    assertFalse(fileContainsString(logFile, FINER_STRING));

    i++;
    final String FINE_STRING = "testLogLevels Message logged at FINE level ["+i+"]";
    logWriter.fine(FINE_STRING);
    assertFalse(fileContainsString(logFile, FINE_STRING));

    i++;
    final String CONFIG_STRING = "testLogLevels Message logged at CONFIG level ["+i+"]";
    logWriter.config(CONFIG_STRING);
    assertFalse(fileContainsString(logFile, CONFIG_STRING));

    i++;
    final String INFO_STRING = "testLogLevels Message logged at INFO level ["+i+"]";
    logWriter.info(INFO_STRING);
    assertFalse(fileContainsString(logFile, INFO_STRING));

    i++;
    final String WARNING_STRING = "ExpectedStrings: testLogLevels Message logged at WARNING level ["+i+"]";
    logWriter.warning(WARNING_STRING);
    assertFalse(fileContainsString(logFile, WARNING_STRING));

    i++;
    final String ERROR_STRING = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logWriter.error(ERROR_STRING);
    assertTrue(fileContainsString(logFile, ERROR_STRING));

    i++;
    final String SEVERE_STRING = "ExpectedStrings: testLogLevels Message logged at SEVERE level ["+i+"]";
    logWriter.severe(SEVERE_STRING);
    assertTrue(fileContainsString(logFile, SEVERE_STRING));
    
    i++;
    final String TRACE_STRING = "testLogLevels Message logged at TRACE level ["+i+"]";
    logger.trace(TRACE_STRING);
    assertFalse(fileContainsString(logFile, TRACE_STRING));
    
    i++;
    final String DEBUG_STRING = "testLogLevels Message logged at DEBUG level ["+i+"]";
    logger.debug(DEBUG_STRING);
    assertFalse(fileContainsString(logFile, DEBUG_STRING));
    
    i++;
    final String INFO_STRING_J = "testLogLevels Message logged at INFO level ["+i+"]";
    logger.info(INFO_STRING_J);
    assertFalse(fileContainsString(logFile, INFO_STRING_J));
    
    i++;
    final String WARN_STRING = "ExpectedStrings: testLogLevels Message logged at WARN level ["+i+"]";
    logger.warn(WARN_STRING);
    assertFalse(fileContainsString(logFile, WARN_STRING));
    
    i++;
    final String ERROR_STRING_J = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logger.error(ERROR_STRING_J);
    assertTrue(fileContainsString(logFile, ERROR_STRING_J));
    
    i++;
    final String FATAL_STRING = "ExpectedStrings: testLogLevels Message logged at FATAL level ["+i+"]";
    logger.fatal(FATAL_STRING);
    assertTrue(fileContainsString(logFile, FATAL_STRING));

    i++;
    final String TRACE_STRING_A = "Message logged at TRACE level ["+i+"]";
    appLogger.trace(TRACE_STRING_A);
    assertFalse(fileContainsString(logFile, TRACE_STRING_A));
    
    i++;
    final String DEBUG_STRING_A = "Message logged at DEBUG level ["+i+"]";
    appLogger.debug(DEBUG_STRING_A);
    assertFalse(fileContainsString(logFile, DEBUG_STRING_A));
    
    i++;
    final String INFO_STRING_A = "Message logged at INFO level ["+i+"]";
    appLogger.info(INFO_STRING_A);
    assertFalse(fileContainsString(logFile, INFO_STRING_A));
    
    i++;
    final String WARN_STRING_A = "ExpectedStrings: Message logged at WARN level ["+i+"]";
    appLogger.warn(WARN_STRING_A);
    assertFalse(fileContainsString(logFile, WARN_STRING_A));
    
    i++;
    final String ERROR_STRING_A = "ExpectedStrings: Message logged at ERROR level ["+i+"]";
    appLogger.error(ERROR_STRING_A);
    assertTrue(fileContainsString(logFile, ERROR_STRING_A));
    
    i++;
    final String FATAL_STRING_A = "ExpectedStrings: Message logged at FATAL level ["+i+"]";
    appLogger.fatal(FATAL_STRING_A);
    assertTrue(fileContainsString(logFile, FATAL_STRING_A));
    }

    this.system.disconnect();
    this.system = null;
  }
  
  @Test
  public void testDistributedSystemWithFineLogLevel() throws Exception {
    //final int port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final String logFileName = name.getMethodName() + "-system-"+ System.currentTimeMillis()+".log";

    final Properties properties = new Properties();
    properties.put("log-file", logFileName);
    properties.put("log-level", "fine");
    properties.put("mcast-port", "0");
    properties.put("locators", "");
    properties.put("enable-network-partition-detection", "false");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    final File logFile = new File(logFileName);
    if (logFile.exists()) {
      logFile.delete();
    }
    assertFalse(logFile.exists());
    
    this.system = DistributedSystem.connect(properties);
    assertNotNull(this.system);
    
    DistributionConfig config = ((InternalDistributedSystem)this.system).getConfig();
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.FINE_LEVEL, config.getLogLevel());
    
    InternalLogWriter logWriter = (InternalLogWriter) system.getLogWriter();
    assertNotNull(logWriter);
    assertTrue(logWriter instanceof LogWriterLogger);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(logWriter.getLogWriterLevel()),
        InternalLogWriter.FINE_LEVEL, logWriter.getLogWriterLevel());
    assertTrue(logWriter.fineEnabled());
    assertTrue(((LogWriterLogger)logWriter).isDebugEnabled());
    assertTrue(logWriter instanceof FastLogger);
    assertTrue(((FastLogger)logWriter).isDelegating());

    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return logFile.exists();
      }
      @Override
      public String description() {
        return "waiting for log file to exist: " + logFile;
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
    assertTrue(logFile.exists());
    
    // assert not empty
    FileInputStream fis = new FileInputStream(logFile);
    try {
      assertTrue(fis.available() > 0);
    } finally {
      fis.close();
    }

    final Logger logger = LogService.getLogger();
    int i = 0;
    
    {
    i++;
    final String FINEST_STRING = "testLogLevels Message logged at FINEST level ["+i+"]";
    logWriter.finest(FINEST_STRING);
    assertFalse(fileContainsString(logFile, FINEST_STRING));

    i++;
    final String FINER_STRING = "testLogLevels Message logged at FINER level ["+i+"]";
    logWriter.finer(FINER_STRING);
    assertFalse(fileContainsString(logFile, FINER_STRING));

    i++;
    final String FINE_STRING = "testLogLevels Message logged at FINE level ["+i+"]";
    logWriter.fine(FINE_STRING);
    assertTrue(fileContainsString(logFile, FINE_STRING));

    i++;
    final String CONFIG_STRING = "testLogLevels Message logged at CONFIG level ["+i+"]";
    logWriter.config(CONFIG_STRING);
    assertTrue(fileContainsString(logFile, CONFIG_STRING));

    i++;
    final String INFO_STRING = "testLogLevels Message logged at INFO level ["+i+"]";
    logWriter.info(INFO_STRING);
    assertTrue(fileContainsString(logFile, INFO_STRING));

    i++;
    final String WARNING_STRING = "ExpectedStrings: testLogLevels Message logged at WARNING level ["+i+"]";
    logWriter.warning(WARNING_STRING);
    assertTrue(fileContainsString(logFile, WARNING_STRING));

    i++;
    final String ERROR_STRING = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logWriter.error(ERROR_STRING);
    assertTrue(fileContainsString(logFile, ERROR_STRING));

    i++;
    final String SEVERE_STRING = "ExpectedStrings: testLogLevels Message logged at SEVERE level ["+i+"]";
    logWriter.severe(SEVERE_STRING);
    assertTrue(fileContainsString(logFile, SEVERE_STRING));
    
    i++;
    final String TRACE_STRING = "testLogLevels Message logged at TRACE level ["+i+"]";
    logger.trace(TRACE_STRING);
    assertFalse(fileContainsString(logFile, TRACE_STRING));
    
    i++;
    final String DEBUG_STRING = "testLogLevels Message logged at DEBUG level ["+i+"]";
    logger.debug(DEBUG_STRING);
    assertTrue(fileContainsString(logFile, DEBUG_STRING));
    
    i++;
    final String INFO_STRING_J = "testLogLevels Message logged at INFO level ["+i+"]";
    logger.info(INFO_STRING_J);
    assertTrue(fileContainsString(logFile, INFO_STRING_J));
    
    i++;
    final String WARN_STRING = "ExpectedStrings: testLogLevels Message logged at WARN level ["+i+"]";
    logger.warn(WARN_STRING);
    assertTrue(fileContainsString(logFile, WARN_STRING));
    
    i++;
    final String ERROR_STRING_J = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logger.error(ERROR_STRING_J);
    assertTrue(fileContainsString(logFile, ERROR_STRING_J));
    
    i++;
    final String FATAL_STRING = "ExpectedStrings: testLogLevels Message logged at FATAL level ["+i+"]";
    logger.fatal(FATAL_STRING);
    assertTrue(fileContainsString(logFile, FATAL_STRING));
    }
    
    // change log level to error and verify
    config.setLogLevel(InternalLogWriter.ERROR_LEVEL);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.ERROR_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.ERROR_LEVEL, config.getLogLevel());
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.ERROR_LEVEL) + " but was " + LogWriterImpl.levelToString(logWriter.getLogWriterLevel()),
        InternalLogWriter.ERROR_LEVEL, logWriter.getLogWriterLevel());
    
    {
    i++;
    final String FINEST_STRING = "testLogLevels Message logged at FINEST level ["+i+"]";
    logWriter.finest(FINEST_STRING);
    assertFalse(fileContainsString(logFile, FINEST_STRING));

    i++;
    final String FINER_STRING = "testLogLevels Message logged at FINER level ["+i+"]";
    logWriter.finer(FINER_STRING);
    assertFalse(fileContainsString(logFile, FINER_STRING));

    i++;
    final String FINE_STRING = "testLogLevels Message logged at FINE level ["+i+"]";
    logWriter.fine(FINE_STRING);
    assertFalse(fileContainsString(logFile, FINE_STRING));

    i++;
    final String CONFIG_STRING = "testLogLevels Message logged at CONFIG level ["+i+"]";
    logWriter.config(CONFIG_STRING);
    assertFalse(fileContainsString(logFile, CONFIG_STRING));

    i++;
    final String INFO_STRING = "testLogLevels Message logged at INFO level ["+i+"]";
    logWriter.info(INFO_STRING);
    assertFalse(fileContainsString(logFile, INFO_STRING));

    i++;
    final String WARNING_STRING = "ExpectedStrings: testLogLevels Message logged at WARNING level ["+i+"]";
    logWriter.warning(WARNING_STRING);
    assertFalse(fileContainsString(logFile, WARNING_STRING));

    i++;
    final String ERROR_STRING = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logWriter.error(ERROR_STRING);
    assertTrue(fileContainsString(logFile, ERROR_STRING));

    i++;
    final String SEVERE_STRING = "ExpectedStrings: testLogLevels Message logged at SEVERE level ["+i+"]";
    logWriter.severe(SEVERE_STRING);
    assertTrue(fileContainsString(logFile, SEVERE_STRING));
    
    i++;
    final String TRACE_STRING = "testLogLevels Message logged at TRACE level ["+i+"]";
    logger.trace(TRACE_STRING);
    assertFalse(fileContainsString(logFile, TRACE_STRING));
    
    i++;
    final String DEBUG_STRING = "testLogLevels Message logged at DEBUG level ["+i+"]";
    logger.debug(DEBUG_STRING);
    assertFalse(fileContainsString(logFile, DEBUG_STRING));
    
    i++;
    final String INFO_STRING_J = "testLogLevels Message logged at INFO level ["+i+"]";
    logger.info(INFO_STRING_J);
    assertFalse(fileContainsString(logFile, INFO_STRING_J));
    
    i++;
    final String WARN_STRING = "ExpectedStrings: testLogLevels Message logged at WARN level ["+i+"]";
    logger.warn(WARN_STRING);
    assertFalse(fileContainsString(logFile, WARN_STRING));
    
    i++;
    final String ERROR_STRING_J = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logger.error(ERROR_STRING_J);
    assertTrue(fileContainsString(logFile, ERROR_STRING_J));
    
    i++;
    final String FATAL_STRING = "ExpectedStrings: testLogLevels Message logged at FATAL level ["+i+"]";
    logger.fatal(FATAL_STRING);
    assertTrue(fileContainsString(logFile, FATAL_STRING));
    }

    this.system.disconnect();
    this.system = null;
  }
  
  @Test
  public void testDistributedSystemWithDebugLogLevel() throws Exception {
    //final int port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final String logFileName = name.getMethodName() + "-system-"+ System.currentTimeMillis()+".log";

    final Properties properties = new Properties();
    properties.put("log-file", logFileName);
    properties.put("log-level", "debug");
    properties.put("mcast-port", "0");
    properties.put("locators", "");
    properties.put("enable-network-partition-detection", "false");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    final File logFile = new File(logFileName);
    if (logFile.exists()) {
      logFile.delete();
    }
    assertFalse(logFile.exists());
    
    this.system = DistributedSystem.connect(properties);
    assertNotNull(this.system);
    
    DistributionConfig config = ((InternalDistributedSystem)this.system).getConfig();
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.FINE_LEVEL, config.getLogLevel());
    
    InternalLogWriter logWriter = (InternalLogWriter) system.getLogWriter();
    assertNotNull(logWriter);
    assertTrue(logWriter instanceof LogWriterLogger);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(logWriter.getLogWriterLevel()),
        InternalLogWriter.FINE_LEVEL, logWriter.getLogWriterLevel());
    assertTrue(logWriter.fineEnabled());
    assertTrue(((LogWriterLogger)logWriter).isDebugEnabled());
    assertTrue(logWriter instanceof FastLogger);
    assertTrue(((FastLogger)logWriter).isDelegating());

    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return logFile.exists();
      }
      @Override
      public String description() {
        return "waiting for log file to exist: " + logFile;
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
    assertTrue(logFile.exists());
    
    // assert not empty
    FileInputStream fis = new FileInputStream(logFile);
    try {
      assertTrue(fis.available() > 0);
    } finally {
      fis.close();
    }

    final Logger logger = LogService.getLogger();
    int i = 0;
    
    {
    i++;
    final String FINEST_STRING = "testLogLevels Message logged at FINEST level ["+i+"]";
    logWriter.finest(FINEST_STRING);
    assertFalse(fileContainsString(logFile, FINEST_STRING));

    i++;
    final String FINER_STRING = "testLogLevels Message logged at FINER level ["+i+"]";
    logWriter.finer(FINER_STRING);
    assertFalse(fileContainsString(logFile, FINER_STRING));

    i++;
    final String FINE_STRING = "testLogLevels Message logged at FINE level ["+i+"]";
    logWriter.fine(FINE_STRING);
    assertTrue(fileContainsString(logFile, FINE_STRING));

    i++;
    final String CONFIG_STRING = "testLogLevels Message logged at CONFIG level ["+i+"]";
    logWriter.config(CONFIG_STRING);
    assertTrue(fileContainsString(logFile, CONFIG_STRING));

    i++;
    final String INFO_STRING = "testLogLevels Message logged at INFO level ["+i+"]";
    logWriter.info(INFO_STRING);
    assertTrue(fileContainsString(logFile, INFO_STRING));

    i++;
    final String WARNING_STRING = "ExpectedStrings: testLogLevels Message logged at WARNING level ["+i+"]";
    logWriter.warning(WARNING_STRING);
    assertTrue(fileContainsString(logFile, WARNING_STRING));

    i++;
    final String ERROR_STRING = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logWriter.error(ERROR_STRING);
    assertTrue(fileContainsString(logFile, ERROR_STRING));

    i++;
    final String SEVERE_STRING = "ExpectedStrings: testLogLevels Message logged at SEVERE level ["+i+"]";
    logWriter.severe(SEVERE_STRING);
    assertTrue(fileContainsString(logFile, SEVERE_STRING));
    
    i++;
    final String TRACE_STRING = "testLogLevels Message logged at TRACE level ["+i+"]";
    logger.trace(TRACE_STRING);
    assertFalse(fileContainsString(logFile, TRACE_STRING));
    
    i++;
    final String DEBUG_STRING = "testLogLevels Message logged at DEBUG level ["+i+"]";
    logger.debug(DEBUG_STRING);
    assertTrue(fileContainsString(logFile, DEBUG_STRING));
    
    i++;
    final String INFO_STRING_J = "testLogLevels Message logged at INFO level ["+i+"]";
    logger.info(INFO_STRING_J);
    assertTrue(fileContainsString(logFile, INFO_STRING_J));
    
    i++;
    final String WARN_STRING = "ExpectedStrings: testLogLevels Message logged at WARN level ["+i+"]";
    logger.warn(WARN_STRING);
    assertTrue(fileContainsString(logFile, WARN_STRING));
    
    i++;
    final String ERROR_STRING_J = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logger.error(ERROR_STRING_J);
    assertTrue(fileContainsString(logFile, ERROR_STRING_J));
    
    i++;
    final String FATAL_STRING = "ExpectedStrings: testLogLevels Message logged at FATAL level ["+i+"]";
    logger.fatal(FATAL_STRING);
    assertTrue(fileContainsString(logFile, FATAL_STRING));
    }
    
    // change log level to error and verify
    config.setLogLevel(InternalLogWriter.ERROR_LEVEL);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.ERROR_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.ERROR_LEVEL, config.getLogLevel());
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.ERROR_LEVEL) + " but was " + LogWriterImpl.levelToString(logWriter.getLogWriterLevel()),
        InternalLogWriter.ERROR_LEVEL, logWriter.getLogWriterLevel());
    
    {
    i++;
    final String FINEST_STRING = "testLogLevels Message logged at FINEST level ["+i+"]";
    logWriter.finest(FINEST_STRING);
    assertFalse(fileContainsString(logFile, FINEST_STRING));

    i++;
    final String FINER_STRING = "testLogLevels Message logged at FINER level ["+i+"]";
    logWriter.finer(FINER_STRING);
    assertFalse(fileContainsString(logFile, FINER_STRING));

    i++;
    final String FINE_STRING = "testLogLevels Message logged at FINE level ["+i+"]";
    logWriter.fine(FINE_STRING);
    assertFalse(fileContainsString(logFile, FINE_STRING));

    i++;
    final String CONFIG_STRING = "testLogLevels Message logged at CONFIG level ["+i+"]";
    logWriter.config(CONFIG_STRING);
    assertFalse(fileContainsString(logFile, CONFIG_STRING));

    i++;
    final String INFO_STRING = "testLogLevels Message logged at INFO level ["+i+"]";
    logWriter.info(INFO_STRING);
    assertFalse(fileContainsString(logFile, INFO_STRING));

    i++;
    final String WARNING_STRING = "ExpectedStrings: testLogLevels Message logged at WARNING level ["+i+"]";
    logWriter.warning(WARNING_STRING);
    assertFalse(fileContainsString(logFile, WARNING_STRING));

    i++;
    final String ERROR_STRING = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logWriter.error(ERROR_STRING);
    assertTrue(fileContainsString(logFile, ERROR_STRING));

    i++;
    final String SEVERE_STRING = "ExpectedStrings: testLogLevels Message logged at SEVERE level ["+i+"]";
    logWriter.severe(SEVERE_STRING);
    assertTrue(fileContainsString(logFile, SEVERE_STRING));
    
    i++;
    final String TRACE_STRING = "testLogLevels Message logged at TRACE level ["+i+"]";
    logger.trace(TRACE_STRING);
    assertFalse(fileContainsString(logFile, TRACE_STRING));
    
    i++;
    final String DEBUG_STRING = "testLogLevels Message logged at DEBUG level ["+i+"]";
    logger.debug(DEBUG_STRING);
    assertFalse(fileContainsString(logFile, DEBUG_STRING));
    
    i++;
    final String INFO_STRING_J = "testLogLevels Message logged at INFO level ["+i+"]";
    logger.info(INFO_STRING_J);
    assertFalse(fileContainsString(logFile, INFO_STRING_J));
    
    i++;
    final String WARN_STRING = "ExpectedStrings: testLogLevels Message logged at WARN level ["+i+"]";
    logger.warn(WARN_STRING);
    assertFalse(fileContainsString(logFile, WARN_STRING));
    
    i++;
    final String ERROR_STRING_J = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logger.error(ERROR_STRING_J);
    assertTrue(fileContainsString(logFile, ERROR_STRING_J));
    
    i++;
    final String FATAL_STRING = "ExpectedStrings: testLogLevels Message logged at FATAL level ["+i+"]";
    logger.fatal(FATAL_STRING);
    assertTrue(fileContainsString(logFile, FATAL_STRING));
    }

    this.system.disconnect();
    this.system = null;
  }
  
  @Test
  public void testDistributedSystemWithSecurityLogDefaultLevel() throws Exception {
    //final int port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final String logFileName = name.getMethodName() + "-system-"+ System.currentTimeMillis()+".log";
    final String securityLogFileName = "security" + name.getMethodName() + "-system-"+ System.currentTimeMillis()+".log";

    final Properties properties = new Properties();
    properties.put("log-file", logFileName);
    properties.put("log-level", "fine");
    properties.put("security-log-file", securityLogFileName);
    properties.put("mcast-port", "0");
    properties.put("locators", "");
    properties.put("enable-network-partition-detection", "false");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    final File securityLogFile = new File(securityLogFileName);
    if (securityLogFile.exists()) {
      securityLogFile.delete();
    }
    assertFalse(securityLogFile.exists());
    
    final File logFile = new File(logFileName);
    if (logFile.exists()) {
      logFile.delete();
    }
    assertFalse(logFile.exists());

    this.system = DistributedSystem.connect(properties);
    assertNotNull(this.system);
    
    DistributionConfig config = ((InternalDistributedSystem)this.system).getConfig();
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.CONFIG_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.CONFIG_LEVEL, config.getSecurityLogLevel());
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.FINE_LEVEL, config.getLogLevel());

    
    InternalLogWriter securityLogWriter = (InternalLogWriter) system.getSecurityLogWriter();
    InternalLogWriter logWriter = (InternalLogWriter) system.getLogWriter();
    assertNotNull(securityLogWriter);
    assertNotNull(logWriter);
    assertTrue(securityLogWriter instanceof LogWriterLogger);
    assertTrue(logWriter instanceof LogWriterLogger);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.INFO_LEVEL) + " but was " + LogWriterImpl.levelToString(securityLogWriter.getLogWriterLevel()),
        InternalLogWriter.INFO_LEVEL, securityLogWriter.getLogWriterLevel());
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(securityLogWriter.getLogWriterLevel()),
        InternalLogWriter.FINE_LEVEL, logWriter.getLogWriterLevel());
    assertFalse(securityLogWriter.fineEnabled());
    assertTrue(logWriter.fineEnabled());
    
    assertFalse(((LogWriterLogger)securityLogWriter).isDebugEnabled());
    assertTrue(((LogWriterLogger)logWriter).isDebugEnabled());
    assertTrue(securityLogWriter instanceof FastLogger);
    assertTrue(logWriter instanceof FastLogger);
    //Because debug available is a static volatile, it is shared between the two writers
    //However we should not see any debug level logging due to the config level set in 
    //the log writer itself
    assertTrue(((FastLogger)securityLogWriter).isDelegating());
    assertTrue(((FastLogger)logWriter).isDelegating());

    
    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return securityLogFile.exists() && logFile.exists();
      }
      @Override
      public String description() {
        return "waiting for log files to exist: " + securityLogFile + ", " + logFile;
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
    assertTrue(securityLogFile.exists());
    assertTrue(logFile.exists());
    
    securityLogWriter.info("test: security log file created at info");
    // assert not empty
    FileInputStream fis = new FileInputStream(securityLogFile);
    try {
      assertTrue(fis.available() > 0);
    } finally {
      fis.close();
    }

    final Logger logger = LogService.getLogger();
    int i = 0;
    
    {
      i++;
      final String FINEST_STRING = "testLogLevels Message logged at FINEST level ["+i+"]";
      securityLogWriter.finest(FINEST_STRING);
      assertFalse(fileContainsString(securityLogFile, FINEST_STRING));
      assertFalse(fileContainsString(logFile, FINEST_STRING));
      
      i++;
      final String FINE_STRING = "testLogLevels Message logged at FINE level ["+i+"]";
      securityLogWriter.fine(FINE_STRING);
      assertFalse(fileContainsString(securityLogFile, FINE_STRING));
      assertFalse(fileContainsString(logFile, FINE_STRING));
      
      i++;
      final String INFO_STRING = "testLogLevels Message logged at INFO level ["+i+"]";
      securityLogWriter.info(INFO_STRING);
      assertTrue(fileContainsString(securityLogFile, INFO_STRING));
      assertFalse(fileContainsString(logFile, INFO_STRING));

      i++;
      final String FINE_STRING_FOR_LOGGER = "testLogLevels Message logged at FINE level ["+i+"]";
      logger.debug(FINE_STRING_FOR_LOGGER);
      assertFalse(fileContainsString(securityLogFile, FINE_STRING_FOR_LOGGER));
      assertTrue(fileContainsString(logFile, FINE_STRING_FOR_LOGGER));
    }

    this.system.disconnect();
    this.system = null;
  }
  
  @Test
  public void testDistributedSystemWithSecurityLogFineLevel() throws Exception {
    //final int port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final String logFileName = name.getMethodName() + "-system-"+ System.currentTimeMillis()+".log";
    final String securityLogFileName = "security" + name.getMethodName() + "-system-"+ System.currentTimeMillis()+".log";

    final Properties properties = new Properties();
    properties.put("log-file", logFileName);
    properties.put("log-level", "fine");
    properties.put("security-log-file", securityLogFileName);
    properties.put("security-log-level", "fine");
    properties.put("mcast-port", "0");
    properties.put("locators", "");
    properties.put("enable-network-partition-detection", "false");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    final File securityLogFile = new File(securityLogFileName);
    if (securityLogFile.exists()) {
      securityLogFile.delete();
    }
    assertFalse(securityLogFile.exists());
    
    final File logFile = new File(logFileName);
    if (logFile.exists()) {
      logFile.delete();
    }
    assertFalse(logFile.exists());

    this.system = DistributedSystem.connect(properties);
    assertNotNull(this.system);
    
    DistributionConfig config = ((InternalDistributedSystem)this.system).getConfig();
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.FINE_LEVEL, config.getSecurityLogLevel());
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.FINE_LEVEL, config.getLogLevel());

    
    InternalLogWriter securityLogWriter = (InternalLogWriter) system.getSecurityLogWriter();
    InternalLogWriter logWriter = (InternalLogWriter) system.getLogWriter();
    assertNotNull(securityLogWriter);
    assertNotNull(logWriter);
    assertTrue(securityLogWriter instanceof LogWriterLogger);
    assertTrue(logWriter instanceof LogWriterLogger);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(securityLogWriter.getLogWriterLevel()),
        InternalLogWriter.FINE_LEVEL, securityLogWriter.getLogWriterLevel());
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(securityLogWriter.getLogWriterLevel()),
        InternalLogWriter.FINE_LEVEL, logWriter.getLogWriterLevel());
    assertTrue(securityLogWriter.fineEnabled());
    assertTrue(logWriter.fineEnabled());
    
    assertTrue(((LogWriterLogger)securityLogWriter).isDebugEnabled());
    assertTrue(((LogWriterLogger)logWriter).isDebugEnabled());
    assertTrue(securityLogWriter instanceof FastLogger);
    assertTrue(logWriter instanceof FastLogger);
    assertTrue(((FastLogger)securityLogWriter).isDelegating());
    assertTrue(((FastLogger)logWriter).isDelegating());

    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return securityLogFile.exists() && logFile.exists();
      }
      @Override
      public String description() {
        return "waiting for log files to exist: " + securityLogFile + ", " + logFile;
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
    assertTrue(securityLogFile.exists());
    assertTrue(logFile.exists());
    
    // assert not empty
    FileInputStream fis = new FileInputStream(securityLogFile);
    try {
      assertTrue(fis.available() > 0);
    } finally {
      fis.close();
    }

    final Logger logger = LogService.getLogger();
    int i = 0;
    
    {
    i++;
    final String FINEST_STRING = "testLogLevels Message logged at FINEST level ["+i+"]";
    securityLogWriter.finest(FINEST_STRING);
    assertFalse(fileContainsString(securityLogFile, FINEST_STRING));
    assertFalse(fileContainsString(logFile, FINEST_STRING));

    i++;
    final String FINER_STRING = "testLogLevels Message logged at FINER level ["+i+"]";
    securityLogWriter.finer(FINER_STRING);
    assertFalse(fileContainsString(securityLogFile, FINER_STRING));
    assertFalse(fileContainsString(logFile, FINER_STRING));

    i++;
    final String FINE_STRING = "testLogLevels Message logged at FINE level ["+i+"]";
    securityLogWriter.fine(FINE_STRING);
    assertTrue(fileContainsString(securityLogFile, FINE_STRING));
    assertFalse(fileContainsString(logFile, FINE_STRING));

    i++;
    final String CONFIG_STRING = "testLogLevels Message logged at CONFIG level ["+i+"]";
    securityLogWriter.config(CONFIG_STRING);
    assertTrue(fileContainsString(securityLogFile, CONFIG_STRING));
    assertFalse(fileContainsString(logFile, CONFIG_STRING));

    i++;
    final String INFO_STRING = "testLogLevels Message logged at INFO level ["+i+"]";
    securityLogWriter.info(INFO_STRING);
    assertTrue(fileContainsString(securityLogFile, INFO_STRING));
    assertFalse(fileContainsString(logFile, INFO_STRING));

    i++;
    final String WARNING_STRING = "ExpectedStrings: testLogLevels Message logged at WARNING level ["+i+"]";
    securityLogWriter.warning(WARNING_STRING);
    assertTrue(fileContainsString(securityLogFile, WARNING_STRING));
    assertFalse(fileContainsString(logFile, WARNING_STRING));

    i++;
    final String ERROR_STRING = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    securityLogWriter.error(ERROR_STRING);
    assertTrue(fileContainsString(securityLogFile, ERROR_STRING));
    assertFalse(fileContainsString(logFile, ERROR_STRING));

    i++;
    final String SEVERE_STRING = "ExpectedStrings: testLogLevels Message logged at SEVERE level ["+i+"]";
    securityLogWriter.severe(SEVERE_STRING);
    assertTrue(fileContainsString(securityLogFile, SEVERE_STRING));
    assertFalse(fileContainsString(logFile, SEVERE_STRING));
    
    i++;
    final String TRACE_STRING = "testLogLevels Message logged at TRACE level ["+i+"]";
    logger.trace(TRACE_STRING);
    assertFalse(fileContainsString(securityLogFile, TRACE_STRING));
    assertFalse(fileContainsString(logFile, TRACE_STRING));
    
    i++;
    final String DEBUG_STRING = "testLogLevels Message logged at DEBUG level ["+i+"]";
    logger.debug(DEBUG_STRING);
    assertFalse(fileContainsString(securityLogFile, DEBUG_STRING));
    assertTrue(fileContainsString(logFile, DEBUG_STRING));
    
    i++;
    final String INFO_STRING_J = "testLogLevels Message logged at INFO level ["+i+"]";
    logger.info(INFO_STRING_J);
    assertFalse(fileContainsString(securityLogFile, INFO_STRING_J));
    assertTrue(fileContainsString(logFile, INFO_STRING_J));
    
    i++;
    final String WARN_STRING = "ExpectedStrings: testLogLevels Message logged at WARN level ["+i+"]";
    logger.warn(WARN_STRING);
    assertFalse(fileContainsString(securityLogFile, WARN_STRING));
    assertTrue(fileContainsString(logFile, WARN_STRING));
    
    i++;
    final String ERROR_STRING_J = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logger.error(ERROR_STRING_J);
    assertFalse(fileContainsString(securityLogFile, ERROR_STRING_J));
    assertTrue(fileContainsString(logFile, ERROR_STRING_J));
    
    i++;
    final String FATAL_STRING = "ExpectedStrings: testLogLevels Message logged at FATAL level ["+i+"]";
    logger.fatal(FATAL_STRING);
    assertFalse(fileContainsString(securityLogFile, FATAL_STRING));
    assertTrue(fileContainsString(logFile, FATAL_STRING));
    }

    this.system.disconnect();
    this.system = null;
  }
  
  /**
   * tests scenario where security log has not been set but a level has 
   * been set to a less granular level than that of the regular log.
   * Verifies that the correct logs for security show up in the regular log as expected
   * @throws Exception
   */
  @Test
  public void testDistributedSystemWithSecurityInfoLevelAndLogAtFineLevelButNoSecurityLog() throws Exception {
    //final int port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final String logFileName = name.getMethodName() + "-system-"+ System.currentTimeMillis()+".log";

    final Properties properties = new Properties();
    properties.put("log-file", logFileName);
    properties.put("log-level", "fine");
    properties.put("security-log-level", "info");
    properties.put("mcast-port", "0");
    properties.put("locators", "");
    properties.put("enable-network-partition-detection", "false");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    final File logFile = new File(logFileName);
    if (logFile.exists()) {
      logFile.delete();
    }
    assertFalse(logFile.exists());

    this.system = DistributedSystem.connect(properties);
    assertNotNull(this.system);
    
    DistributionConfig config = ((InternalDistributedSystem)this.system).getConfig();
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.INFO_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.INFO_LEVEL, config.getSecurityLogLevel());
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.FINE_LEVEL, config.getLogLevel());

    
    InternalLogWriter securityLogWriter = (InternalLogWriter) system.getSecurityLogWriter();
    InternalLogWriter logWriter = (InternalLogWriter) system.getLogWriter();
    assertNotNull(securityLogWriter);
    assertNotNull(logWriter);
    assertTrue(securityLogWriter instanceof LogWriterLogger);
    assertTrue(logWriter instanceof LogWriterLogger);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.INFO_LEVEL) + " but was " + LogWriterImpl.levelToString(securityLogWriter.getLogWriterLevel()),
        InternalLogWriter.INFO_LEVEL, securityLogWriter.getLogWriterLevel());
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(securityLogWriter.getLogWriterLevel()),
        InternalLogWriter.FINE_LEVEL, logWriter.getLogWriterLevel());
    assertFalse(securityLogWriter.fineEnabled());
    assertTrue(logWriter.fineEnabled());
    
    assertFalse(((LogWriterLogger)securityLogWriter).isDebugEnabled());
    assertTrue(((LogWriterLogger)logWriter).isDebugEnabled());
    assertTrue(securityLogWriter instanceof FastLogger);
    assertTrue(logWriter instanceof FastLogger);
    assertTrue(((FastLogger)securityLogWriter).isDelegating());
    assertTrue(((FastLogger)logWriter).isDelegating());

    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return logFile.exists();
      }
      @Override
      public String description() {
        return "waiting for log files to exist: " + logFile;
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
    assertTrue(logFile.exists());

    final Logger logger = LogService.getLogger();
    int i = 0;
    
    {
    i++;
    final String FINEST_STRING = "testLogLevels Message logged at FINEST level ["+i+"]";
    securityLogWriter.finest(FINEST_STRING);
    assertFalse(fileContainsString(logFile, FINEST_STRING));

    i++;
    final String FINER_STRING = "testLogLevels Message logged at FINER level ["+i+"]";
    securityLogWriter.finer(FINER_STRING);
    assertFalse(fileContainsString(logFile, FINER_STRING));

    i++;
    final String FINE_STRING = "testLogLevels Message logged at FINE level ["+i+"]";
    securityLogWriter.fine(FINE_STRING);
    assertFalse(fileContainsString(logFile, FINE_STRING));

    i++;
    final String CONFIG_STRING = "testLogLevels Message logged at CONFIG level ["+i+"]";
    securityLogWriter.config(CONFIG_STRING);
    assertTrue(fileContainsString(logFile, CONFIG_STRING));

    i++;
    final String INFO_STRING = "testLogLevels Message logged at INFO level ["+i+"]";
    securityLogWriter.info(INFO_STRING);
    assertTrue(fileContainsString(logFile, INFO_STRING));

    i++;
    final String WARNING_STRING = "ExpectedStrings: testLogLevels Message logged at WARNING level ["+i+"]";
    securityLogWriter.warning(WARNING_STRING);
    assertTrue(fileContainsString(logFile, WARNING_STRING));

    i++;
    final String ERROR_STRING = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    securityLogWriter.error(ERROR_STRING);
    assertTrue(fileContainsString(logFile, ERROR_STRING));

    i++;
    final String SEVERE_STRING = "ExpectedStrings: testLogLevels Message logged at SEVERE level ["+i+"]";
    securityLogWriter.severe(SEVERE_STRING);
    assertTrue(fileContainsString(logFile, SEVERE_STRING));
    
    i++;
    final String TRACE_STRING = "testLogLevels Message logged at TRACE level ["+i+"]";
    logger.trace(TRACE_STRING);
    assertFalse(fileContainsString(logFile, TRACE_STRING));
    
    i++;
    final String DEBUG_STRING = "testLogLevels Message logged at DEBUG level ["+i+"]";
    logger.debug(DEBUG_STRING);
    assertTrue(fileContainsString(logFile, DEBUG_STRING));
    
    i++;
    final String INFO_STRING_J = "testLogLevels Message logged at INFO level ["+i+"]";
    logger.info(INFO_STRING_J);
    assertTrue(fileContainsString(logFile, INFO_STRING_J));
    
    i++;
    final String WARN_STRING = "ExpectedStrings: testLogLevels Message logged at WARN level ["+i+"]";
    logger.warn(WARN_STRING);
    assertTrue(fileContainsString(logFile, WARN_STRING));
    
    i++;
    final String ERROR_STRING_J = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logger.error(ERROR_STRING_J);
    assertTrue(fileContainsString(logFile, ERROR_STRING_J));
    
    i++;
    final String FATAL_STRING = "ExpectedStrings: testLogLevels Message logged at FATAL level ["+i+"]";
    logger.fatal(FATAL_STRING);
    assertTrue(fileContainsString(logFile, FATAL_STRING));
    }

    this.system.disconnect();
    this.system = null;
  }
  
  /**
   * tests scenario where security log has not been set but a level has 
   * been set to a more granular level than that of the regular log.
   * Verifies that the correct logs for security show up in the regular log as expected
   * @throws Exception
   */
  @Test
  public void testDistributedSystemWithSecurityFineLevelAndLogAtInfoLevelButNoSecurityLog() throws Exception {
    //final int port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final String logFileName = name.getMethodName() + "-system-"+ System.currentTimeMillis()+".log";

    final Properties properties = new Properties();
    properties.put("log-file", logFileName);
    properties.put("log-level", "info");
    properties.put("security-log-level", "fine");
    properties.put("mcast-port", "0");
    properties.put("locators", "");
    properties.put("enable-network-partition-detection", "false");
    properties.put("disable-auto-reconnect", "true");
    properties.put("member-timeout", "2000");
    properties.put(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    final File logFile = new File(logFileName);
    if (logFile.exists()) {
      logFile.delete();
    }
    assertFalse(logFile.exists());

    this.system = DistributedSystem.connect(properties);
    assertNotNull(this.system);
    
    DistributionConfig config = ((InternalDistributedSystem)this.system).getConfig();
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.FINE_LEVEL, config.getSecurityLogLevel());
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.INFO_LEVEL) + " but was " + LogWriterImpl.levelToString(config.getLogLevel()),
        InternalLogWriter.INFO_LEVEL, config.getLogLevel());

    
    InternalLogWriter securityLogWriter = (InternalLogWriter) system.getSecurityLogWriter();
    InternalLogWriter logWriter = (InternalLogWriter) system.getLogWriter();
    assertNotNull(securityLogWriter);
    assertNotNull(logWriter);
    assertTrue(securityLogWriter instanceof LogWriterLogger);
    assertTrue(logWriter instanceof LogWriterLogger);
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.FINE_LEVEL) + " but was " + LogWriterImpl.levelToString(securityLogWriter.getLogWriterLevel()),
        InternalLogWriter.FINE_LEVEL, securityLogWriter.getLogWriterLevel());
    assertEquals("Expected " + LogWriterImpl.levelToString(InternalLogWriter.INFO_LEVEL) + " but was " + LogWriterImpl.levelToString(securityLogWriter.getLogWriterLevel()),
        InternalLogWriter.INFO_LEVEL, logWriter.getLogWriterLevel());
    assertTrue(securityLogWriter.fineEnabled());
    assertFalse(logWriter.fineEnabled());
    
    assertTrue(((LogWriterLogger)securityLogWriter).isDebugEnabled());
    assertFalse(((LogWriterLogger)logWriter).isDebugEnabled());
    assertTrue(securityLogWriter instanceof FastLogger);
    assertTrue(logWriter instanceof FastLogger);
    assertTrue(((FastLogger)securityLogWriter).isDelegating());
    assertTrue(((FastLogger)logWriter).isDelegating());

    Wait.waitForCriterion(new WaitCriterion() {
      @Override
      public boolean done() {
        return logFile.exists();
      }
      @Override
      public String description() {
        return "waiting for log files to exist: " + logFile;
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
    assertTrue(logFile.exists());

    final Logger logger = LogService.getLogger();
    int i = 0;
    
    {
    i++;
    final String FINEST_STRING = "testLogLevels Message logged at FINEST level ["+i+"]";
    securityLogWriter.finest(FINEST_STRING);
    assertFalse(fileContainsString(logFile, FINEST_STRING));

    i++;
    final String FINER_STRING = "testLogLevels Message logged at FINER level ["+i+"]";
    securityLogWriter.finer(FINER_STRING);
    assertFalse(fileContainsString(logFile, FINER_STRING));

    i++;
    final String FINE_STRING = "testLogLevels Message logged at FINE level ["+i+"]";
    securityLogWriter.fine(FINE_STRING);
    assertTrue(fileContainsString(logFile, FINE_STRING));

    i++;
    final String CONFIG_STRING = "testLogLevels Message logged at CONFIG level ["+i+"]";
    securityLogWriter.config(CONFIG_STRING);
    assertTrue(fileContainsString(logFile, CONFIG_STRING));

    i++;
    final String INFO_STRING = "testLogLevels Message logged at INFO level ["+i+"]";
    securityLogWriter.info(INFO_STRING);
    assertTrue(fileContainsString(logFile, INFO_STRING));

    i++;
    final String WARNING_STRING = "ExpectedStrings: testLogLevels Message logged at WARNING level ["+i+"]";
    securityLogWriter.warning(WARNING_STRING);
    assertTrue(fileContainsString(logFile, WARNING_STRING));

    i++;
    final String ERROR_STRING = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    securityLogWriter.error(ERROR_STRING);
    assertTrue(fileContainsString(logFile, ERROR_STRING));

    i++;
    final String SEVERE_STRING = "ExpectedStrings: testLogLevels Message logged at SEVERE level ["+i+"]";
    securityLogWriter.severe(SEVERE_STRING);
    assertTrue(fileContainsString(logFile, SEVERE_STRING));
    
    i++;
    final String TRACE_STRING = "testLogLevels Message logged at TRACE level ["+i+"]";
    logger.trace(TRACE_STRING);
    assertFalse(fileContainsString(logFile, TRACE_STRING));
    
    i++;
    final String DEBUG_STRING = "testLogLevels Message logged at DEBUG level ["+i+"]";
    logger.debug(DEBUG_STRING);
    assertFalse(fileContainsString(logFile, DEBUG_STRING));
    
    i++;
    final String INFO_STRING_J = "testLogLevels Message logged at INFO level ["+i+"]";
    logger.info(INFO_STRING_J);
    assertTrue(fileContainsString(logFile, INFO_STRING_J));
    
    i++;
    final String WARN_STRING = "ExpectedStrings: testLogLevels Message logged at WARN level ["+i+"]";
    logger.warn(WARN_STRING);
    assertTrue(fileContainsString(logFile, WARN_STRING));
    
    i++;
    final String ERROR_STRING_J = "ExpectedStrings: testLogLevels Message logged at ERROR level ["+i+"]";
    logger.error(ERROR_STRING_J);
    assertTrue(fileContainsString(logFile, ERROR_STRING_J));
    
    i++;
    final String FATAL_STRING = "ExpectedStrings: testLogLevels Message logged at FATAL level ["+i+"]";
    logger.fatal(FATAL_STRING);
    assertTrue(fileContainsString(logFile, FATAL_STRING));
    }

    this.system.disconnect();
    this.system = null;
  }


  private static boolean fileContainsString(final File file, final String string) throws FileNotFoundException {
    Scanner scanner = new Scanner(file);
    try {
      while(scanner.hasNextLine()){
        if(scanner.nextLine().trim().contains(string)){
          return true;
        }
      }
    } finally {
      scanner.close();
    }
    return false;
  }
}
