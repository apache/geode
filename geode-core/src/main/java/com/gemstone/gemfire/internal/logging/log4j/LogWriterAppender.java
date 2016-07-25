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

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

import com.gemstone.gemfire.internal.logging.LogConfig;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.ManagerLogWriter;
import com.gemstone.gemfire.internal.logging.PureLogWriter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * A Log4j Appender which will copy all output to a LogWriter.
 * 
 */
public class LogWriterAppender extends AbstractAppender implements PropertyChangeListener {
  private static final org.apache.logging.log4j.Logger logger = LogService.getLogger();

  /** Is this thread in the process of appending? */
  private static final ThreadLocal<Boolean> appending = new ThreadLocal<Boolean>() {
    @Override
    protected Boolean initialValue() {
      return Boolean.FALSE;
    }
  };
  
  private final PureLogWriter logWriter;
  private final FileOutputStream fos; // TODO:LOG:CLEANUP: why do we track this outside ManagerLogWriter? doesn't rolling invalidate it?
  
  private final AppenderContext[] appenderContexts;
  private final String appenderName;
  private final String logWriterLoggerName;
  
  private LogWriterAppender(final AppenderContext[] appenderContexts, final String name, final PureLogWriter logWriter, final FileOutputStream fos) {
    super(LogWriterAppender.class.getName() + "-" + name, null, PatternLayout.createDefaultLayout());
    this.appenderContexts = appenderContexts; 
    this.appenderName = LogWriterAppender.class.getName() + "-" + name;
    this.logWriterLoggerName = name;
    this.logWriter = logWriter;
    this.fos = fos;
  }
  
  /**
   * Used by LogWriterAppenders and tests to create a new instance.
   * 
   * @return The new instance.
   */
  static final LogWriterAppender create(final AppenderContext[] contexts, final String name, final PureLogWriter logWriter, final FileOutputStream fos) {
    LogWriterAppender appender = new LogWriterAppender(contexts, name, logWriter, fos);
    for (AppenderContext context : appender.appenderContexts) {
      context.getLoggerContext().addPropertyChangeListener(appender);
    }
    appender.start();
    for (AppenderContext context : appender.appenderContexts) {
      context.getLoggerConfig().addAppender(appender, Level.ALL, null);
    }
    return appender;
  }

  @Override
  public void append(final LogEvent event) {
    // If already appending then don't send to avoid infinite recursion
    if ((appending.get())) {
      return;
    }
    appending.set(Boolean.TRUE);
    try {
      this.logWriter.put(LogWriterLogger.log4jLevelToLogWriterLevel(event.getLevel()), event.getMessage().getFormattedMessage(),
          event.getThrown());
    } finally {
      appending.set(Boolean.FALSE);
    }
  }

  @Override
  public synchronized void propertyChange(final PropertyChangeEvent evt) {
    if (logger.isDebugEnabled()) {
      logger.debug("Responding to a property change event. Property name is {}.", evt.getPropertyName());
    }
    if (evt.getPropertyName().equals(LoggerContext.PROPERTY_CONFIG)) {
      for (AppenderContext context : this.appenderContexts) {
        LoggerConfig loggerConfig = context.getLoggerConfig();
        if (!loggerConfig.getAppenders().containsKey(this.appenderName)) {
          loggerConfig.addAppender(this, Level.ALL, null);
        }
      }
    }
  }
  
  /**
   * Stop the appender and remove it from the Log4j configuration.
   */
  protected void destroy() { // called 1st during disconnect
    // add stdout appender to MAIN_LOGGER_NAME only if isUsingGemFireDefaultConfig -- see #51819
    if (LogService.MAIN_LOGGER_NAME.equals(this.logWriterLoggerName) && LogService.isUsingGemFireDefaultConfig()) {
      LogService.restoreConsoleAppender();
    }
    for (AppenderContext context : this.appenderContexts) {
      context.getLoggerContext().removePropertyChangeListener(this);
      context.getLoggerConfig().removeAppender(appenderName);
    }
    for (AppenderContext context : this.appenderContexts) { // do this second as log4j 2.6+ will re-add
      context.getLoggerContext().updateLoggers();
    }
    stop();
    cleanUp(); // 3rd
    if (logger.isDebugEnabled()) {
      logger.debug("A LogWriterAppender has been destroyed and cleanup is finished.");
    }
  }

  private void cleanUp() { // was closingLogFile() -- called from destroy() as the final step 
    if (this.logWriter instanceof ManagerLogWriter) {
      ((ManagerLogWriter)this.logWriter).closingLogFile();
    }
    if (this.fos != null) {
      try {
        this.fos.close();
      }
      catch (IOException ignore) {
      }
    }
  }
  
  @Override
  public void stop() {
    try {
      if (this.logWriter instanceof ManagerLogWriter) {
        ((ManagerLogWriter)this.logWriter).shuttingDown();
      }
    } catch (RuntimeException e) {
      logger.warn("RuntimeException encountered while shuttingDown LogWriterAppender", e);
    }
    
    super.stop();
  }
  
  protected void startupComplete() {
    if (this.logWriter instanceof ManagerLogWriter) {
      ((ManagerLogWriter)this.logWriter).startupComplete();
    }
  }
  
  protected void setConfig(final LogConfig cfg) {
    if (this.logWriter instanceof ManagerLogWriter) {
      ((ManagerLogWriter)this.logWriter).setConfig(cfg);
    }
  }
  
  public File getChildLogFile() {
    if (this.logWriter instanceof ManagerLogWriter) {
      return ((ManagerLogWriter)this.logWriter).getChildLogFile();
    } else {
      return null;
    }
  }
  
  public File getLogDir() {
    if (this.logWriter instanceof ManagerLogWriter) {
      return ((ManagerLogWriter)this.logWriter).getLogDir();
    } else {
      return null;
    }
  }
  
  public int getMainLogId() {
    if (this.logWriter instanceof ManagerLogWriter) {
      return ((ManagerLogWriter)this.logWriter).getMainLogId();
    } else {
      return -1;
    }
  }
  
  public boolean useChildLogging() {
    if (this.logWriter instanceof ManagerLogWriter) {
      return ((ManagerLogWriter)this.logWriter).useChildLogging();
    } else {
      return false;
    }
  }
  
  protected void configChanged() {
    if (this.logWriter instanceof ManagerLogWriter) {
      ((ManagerLogWriter)this.logWriter).configChanged();
    }
  }
}
