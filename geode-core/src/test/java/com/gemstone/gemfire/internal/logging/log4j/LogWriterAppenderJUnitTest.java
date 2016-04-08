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

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.PureLogWriter;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests the LogWriterAppender.
 * 
 */
@Category(UnitTest.class)
public class LogWriterAppenderJUnitTest {

  private Level previousLogLevel;
  private LogWriterAppender appender;
  
  @Before
  public void setUp() {
    this.previousLogLevel = LogService.getBaseLogLevel();
  }
  
  @After
  public void tearDown() {
    LogService.setBaseLogLevel(this.previousLogLevel);
    if (this.appender != null) {
      this.appender.stop();
      Configurator.getLoggerConfig(LogService.BASE_LOGGER_NAME).getAppenders().remove(this.appender.getName());
    }
  }
  
  /**
   * Verifies that the appender is correctly added and removed from the Log4j
   * configuration and that when the configuration is changed the appender is
   * still there.
   */
  @Test
  public final void testAppenderToConfigHandling() throws IOException {
    LogService.setBaseLogLevel(Level.TRACE);

    final AppenderContext rootContext = LogService.getAppenderContext();
    
    // Find out home many appenders exist before we get started
    final int startingSize = rootContext.getLoggerConfig().getAppenders().size();
    
    //System.out.println("Appenders " + context.getLoggerConfig().getAppenders().values().toString());
    
    // Create the appender and verify it's part of the configuration
    final StringWriter stringWriter = new StringWriter();
    final PureLogWriter logWriter = new PureLogWriter(InternalLogWriter.FINE_LEVEL, new PrintWriter(stringWriter), "");
    
    final AppenderContext[] contexts = new AppenderContext[2];
    contexts[0] = rootContext; // root context
    contexts[1] = LogService.getAppenderContext(LogService.BASE_LOGGER_NAME); // "com.gemstone" context
    
    this.appender = LogWriterAppender.create(contexts, LogService.MAIN_LOGGER_NAME, logWriter, null);

    assertEquals(rootContext.getLoggerConfig().getAppenders().values().toString(), 
        startingSize+1, rootContext.getLoggerConfig().getAppenders().size());
    assertTrue(rootContext.getLoggerConfig().getAppenders().containsKey(this.appender.getName()));

    // Modify the config and verify that the appender still exists
    assertEquals(Level.TRACE, LogService.getLogger(LogService.BASE_LOGGER_NAME).getLevel());
    LogService.setBaseLogLevel(Level.DEBUG);
    assertEquals(Level.DEBUG, LogService.getLogger(LogService.BASE_LOGGER_NAME).getLevel());
    assertTrue(rootContext.getLoggerConfig().getAppenders().containsKey(this.appender.getName()));

    // Destroy the appender and verify that it was removed from log4j
    this.appender.destroy();
    assertEquals(rootContext.getLoggerConfig().getAppenders().values().toString(), startingSize, rootContext.getLoggerConfig().getAppenders().size());
    assertFalse(rootContext.getLoggerConfig().getAppenders().containsKey(this.appender.getName()));
  }

  /**
   * Verifies that writing to a Log4j logger will end up in the LogWriter's output.
   */
  @Test
  public final void testLogOutput() throws IOException {
    // Create the appender
    final StringWriter stringWriter = new StringWriter();
    final PureLogWriter logWriter = new PureLogWriter(InternalLogWriter.FINEST_LEVEL, new PrintWriter(stringWriter), "");

    final AppenderContext[] contexts = new AppenderContext[2];
    contexts[0] = LogService.getAppenderContext(); // root context
    contexts[1] = LogService.getAppenderContext(LogService.BASE_LOGGER_NAME); // "com.gemstone" context
    
    this.appender = LogWriterAppender.create(contexts, LogService.MAIN_LOGGER_NAME, logWriter, null);

    final Logger logger = LogService.getLogger();
    
    // set the level to TRACE
    Configurator.setLevel(LogService.BASE_LOGGER_NAME, Level.TRACE);
    Configurator.setLevel(LogService.MAIN_LOGGER_NAME, Level.TRACE);
    
    assertEquals(Level.TRACE, logger.getLevel());
    
    logger.trace("TRACE MESSAGE");
    assertTrue(Pattern.compile(".*\\[finest .*TRACE MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);

    logger.debug("DEBUG MESSAGE");
    assertTrue(Pattern.compile(".*\\[fine .*DEBUG MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);

    logger.info("INFO MESSAGE");
    assertTrue(Pattern.compile(".*\\[info .*INFO MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);

    logger.warn("ExpectedStrings: WARNING MESSAGE");
    assertTrue(Pattern.compile(".*\\[warning .*WARNING MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);

    logger.error("ExpectedStrings: ERROR MESSAGE");
    assertTrue(Pattern.compile(".*\\[error .*ERROR MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);

    logger.fatal("ExpectedStrings: FATAL MESSAGE");
    assertTrue(Pattern.compile(".*\\[severe .*FATAL MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);
    
    final Logger lowerLevelLogger = LogService.getLogger(LogService.BASE_LOGGER_NAME + ".subpackage");
    lowerLevelLogger.fatal("ExpectedStrings: FATAL MESSAGE");
    assertTrue(Pattern.compile(".*\\[severe .*FATAL MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);
    
    this.appender.destroy();
    assertFalse(Configurator.getLoggerConfig(LogService.BASE_LOGGER_NAME).getAppenders().containsKey(this.appender.getName()));
  }
  
  /**
   * Verifies that logging occurs at the levels set in the LogWriter
   */
  @Test
  public final void testLogWriterLevels() throws IOException {
    final String loggerName = LogService.MAIN_LOGGER_NAME; //this.getClass().getName();
    LogService.getLogger(); // Force logging to be initialized

    // Create the LogWriterLogger that will be attached to the appender
    final LogWriterLogger logWriterLogger = LogWriterLogger.create(loggerName, false);
    logWriterLogger.setLevel(Level.INFO);
    
    // Create the appender
    final StringWriter stringWriter = new StringWriter();
    final PureLogWriter logWriter = new PureLogWriter(InternalLogWriter.FINEST_LEVEL, new PrintWriter(stringWriter), "");

    final AppenderContext[] contexts = new AppenderContext[2];
    contexts[0] = LogService.getAppenderContext(); // root context
    contexts[1] = LogService.getAppenderContext(LogService.BASE_LOGGER_NAME); // "com.gemstone" context
    
    this.appender = LogWriterAppender.create(contexts, loggerName, logWriter, null);
    
    logWriter.finest("DIRECT MESSAGE");
    assertTrue(Pattern.compile(".*\\[finest .*DIRECT MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);
    
    LogEvent event = Log4jLogEvent.newBuilder().setLevel(Level.INFO).setLoggerFqcn("NAME").setLoggerName("NAME").setMessage(new ParameterizedMessage("LOGEVENT MESSAGE", null)).build();
    this.appender.append(event);
    assertTrue(Pattern.compile(".*\\[info .*LOGEVENT MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);
    
    logWriterLogger.finest("FINEST MESSAGE");
    assertFalse(Pattern.compile(".*\\[finest .*FINEST MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);

    logWriterLogger.fine("FINE MESSAGE");
    assertFalse(Pattern.compile(".*\\[fine .*FINE MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);

    logWriterLogger.info("INFO MESSAGE");
    assertTrue(stringWriter.toString(), Pattern.compile(".*\\[info .*INFO MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);

    // Change the level
    logWriterLogger.setLevel(Level.DEBUG);

    logWriterLogger.finest("FINEST MESSAGE");
    assertFalse(Pattern.compile(".*\\[finest .*FINEST MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);

    logWriterLogger.fine("FINE MESSAGE");
    assertTrue(Pattern.compile(".*\\[fine .*FINE MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);

    logWriterLogger.info("INFO MESSAGE");
    assertTrue(Pattern.compile(".*\\[info .*INFO MESSAGE.*", Pattern.DOTALL).matcher(stringWriter.toString()).matches());
    stringWriter.getBuffer().setLength(0);

    this.appender.destroy();
  }
}
