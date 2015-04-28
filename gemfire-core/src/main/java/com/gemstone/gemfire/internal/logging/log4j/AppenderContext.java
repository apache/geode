package com.gemstone.gemfire.internal.logging.log4j;

import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;

import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Provides the LoggerContext and LoggerConfig for GemFire appenders to attach
 * to. These appenders include AlertAppender and LogWriterAppender.
 * 
 * @author Kirk Lund
 */
public class AppenderContext {
  
  /** "com.gemstone" is a good alternative for limiting alerts to just gemstone packages, otherwise ROOT is used */
  public static final String LOGGER_PROPERTY = "gemfire.logging.appenders.LOGGER";
  
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
