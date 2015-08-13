/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.logging;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Filter.Result;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.FileConfigurationMonitor;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.status.StatusConfiguration;
import org.apache.logging.log4j.core.config.xml.XmlConfiguration;
import org.apache.logging.log4j.core.filter.AbstractFilterable;
import org.apache.logging.log4j.core.lookup.Interpolator;
import org.apache.logging.log4j.core.lookup.StrLookup;
import org.apache.logging.log4j.core.lookup.StrSubstitutor;
import org.apache.logging.log4j.core.util.Closer;
import org.apache.logging.log4j.core.util.Loader;
import org.apache.logging.log4j.core.util.Patterns;
import org.apache.logging.log4j.status.StatusLogger;
import org.apache.logging.log4j.util.PropertiesUtil;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.logging.log4j.AppenderContext;
import com.gemstone.gemfire.internal.logging.log4j.ConfigLocator;
import com.gemstone.gemfire.internal.logging.log4j.Configurator;
import com.gemstone.gemfire.internal.logging.log4j.FastLogger;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterLogger;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.org.apache.logging.log4j.core.config.xml.GemFireXmlConfiguration;
import com.gemstone.org.apache.logging.log4j.core.config.xml.GemFireXmlConfigurationFactory;
import com.gemstone.org.apache.logging.log4j.message.GemFireParameterizedMessageFactory;

/**
 * Centralizes log configuration and initialization.
 * 
 * @author bakera
 * @author David Hoots
 */
@SuppressWarnings("unused")
public class LogService extends LogManager {
  // This is highest point in the hierarchy for all GemFire logging
  public static final String ROOT_LOGGER_NAME = "";
  public static final String BASE_LOGGER_NAME = "com.gemstone";
  public static final String MAIN_LOGGER_NAME = "com.gemstone.gemfire";
  public static final String SECURITY_LOGGER_NAME = "com.gemstone.gemfire.security";
  
  public static final String GEMFIRE_VERBOSE_FILTER = "{GEMFIRE_VERBOSE}";
  
  protected static final String STDOUT = "STDOUT";

  private static final PropertyChangeListener propertyChangeListener = new PropertyChangeListenerImpl();
  
  public static final String DEFAULT_CONFIG = "/log4j2.xml";
  public static final String CLI_CONFIG = "/log4j2-cli.xml";

  /**
   * Name of variable that is set to "true" in log4j2.xml to indicate that it is the default gemfire config xml.
   */
  private static final String GEMFIRE_DEFAULT_PROPERTY = "gemfire-default";
  
  /** Protected by static synchronization. Used for removal and adding stdout back in. */
  private static Appender stdoutAppender;
  
  static {
    init();
  }
  private static void init() {
    LoggerContext context = ((org.apache.logging.log4j.core.Logger) LogManager.getLogger(BASE_LOGGER_NAME, GemFireParameterizedMessageFactory.INSTANCE)).getContext();
    context.removePropertyChangeListener(propertyChangeListener);
    context.addPropertyChangeListener(propertyChangeListener);
    context.reconfigure(); // propertyChangeListener invokes configureFastLoggerDelegating
    configureLoggers(false, false);
  }
  
  public static void initialize() {
    new LogService();
  }
  
  public static void reconfigure() {
    init();
  }
  
  public static void configureLoggers(final boolean hasLogFile, final boolean hasSecurityLogFile) {
    Configurator.getOrCreateLoggerConfig(BASE_LOGGER_NAME, true, false);
    Configurator.getOrCreateLoggerConfig(MAIN_LOGGER_NAME, true, hasLogFile);
    final boolean useMainLoggerForSecurity = !hasSecurityLogFile;
    Configurator.getOrCreateLoggerConfig(SECURITY_LOGGER_NAME, useMainLoggerForSecurity, hasSecurityLogFile);
  }
  
  public static AppenderContext getAppenderContext() {
    return new AppenderContext();
  }
  
  public static AppenderContext getAppenderContext(final String name) {
    return new AppenderContext(name);
  }
  
  public static boolean isUsingGemFireDefaultConfig() {
    final Configuration config = ((org.apache.logging.log4j.core.Logger)
        LogManager.getLogger(ROOT_LOGGER_NAME, GemFireParameterizedMessageFactory.INSTANCE)).getContext().getConfiguration();
    
    final StrSubstitutor sub = config.getStrSubstitutor();
    final StrLookup resolver = sub.getVariableResolver();
    
    final String value = resolver.lookup(GEMFIRE_DEFAULT_PROPERTY);
    
    return "true".equals(value);
  }
  
  public static String getConfigInformation() {
    return getConfiguration().getConfigurationSource().toString();
  }

  /**
   * Finds a Log4j configuration file in the current directory.  The names of
   * the files to look for are the same as those that Log4j would look for on
   * the classpath.
   * 
   * @return A File for the configuration file or null if one isn't found.
   */
  public static File findLog4jConfigInCurrentDir() {    
    return ConfigLocator.findConfigInWorkingDirectory();
  }

  /**
   * Returns a Logger with the name of the calling class.
   * @return The Logger for the calling class.
   */
  public static Logger getLogger() {
    return new FastLogger(LogManager.getLogger(getClassName(2), GemFireParameterizedMessageFactory.INSTANCE));
  }
  
  public static Logger getLogger(final String name) {
    return new FastLogger(LogManager.getLogger(name, GemFireParameterizedMessageFactory.INSTANCE));
  }

  /**
   * Returns a LogWriterLogger that is decorated with the LogWriter and LogWriterI18n
   * methods.
   * 
   * This is the bridge to LogWriter and LogWriterI18n that we need to eventually
   * stop using in phase 1. We will switch over from a shared LogWriterLogger instance
   * to having every GemFire class own its own private static GemFireLogger
   * 
   * @return The LogWriterLogger for the calling class.
   */
  public static LogWriterLogger createLogWriterLogger(final String name, final String connectionName, final boolean isSecure) {
    return LogWriterLogger.create(name, connectionName, isSecure);
  }
  
  /**
   * Return the Log4j Level associated with the int level.
   * 
   * @param intLevel
   *          The int value of the Level to return.
   * @return The Level.
   * @throws java.lang.IllegalArgumentException if the Level int is not registered.
   */
  public static Level toLevel(final int intLevel) {
    for (Level level : Level.values()) {
      if (level.intLevel() == intLevel) {
        return level;
      }
    }

    throw new IllegalArgumentException("Unknown int level [" + intLevel + "].");
  }

  /**
   * Gets the class name of the caller in the current stack at the given {@code depth}.
   *
   * @param depth a 0-based index in the current stack.
   * @return a class name
   */
  public static String getClassName(final int depth) {
    return new Throwable().getStackTrace()[depth].getClassName();
  }

  public static Configuration getConfiguration() {
    final Configuration config = ((org.apache.logging.log4j.core.Logger)
        LogManager.getLogger(ROOT_LOGGER_NAME, GemFireParameterizedMessageFactory.INSTANCE)).getContext().getConfiguration();
    return config;
  }
  
  public static void configureFastLoggerDelegating() {
    final Configuration config = ((org.apache.logging.log4j.core.Logger)
        LogManager.getLogger(ROOT_LOGGER_NAME, GemFireParameterizedMessageFactory.INSTANCE)).getContext().getConfiguration();
    
    if (Configurator.hasContextWideFilter(config) || 
        Configurator.hasAppenderFilter(config) || 
        Configurator.hasDebugOrLower(config) || 
        Configurator.hasLoggerFilter(config) || 
        Configurator.hasAppenderRefFilter(config)) {
      FastLogger.setDelegating(true);
    } else {
      FastLogger.setDelegating(false);
    }
  }
  
  private static class PropertyChangeListenerImpl implements PropertyChangeListener {
    @SuppressWarnings("synthetic-access")
    @Override
    public void propertyChange(final PropertyChangeEvent evt) {
      StatusLogger.getLogger().debug("LogService responding to a property change event. Property name is {}.",
          evt.getPropertyName());
      
      if (evt.getPropertyName().equals(LoggerContext.PROPERTY_CONFIG)) {
        configureFastLoggerDelegating();
      }
    }
  }
  
  public static void setBaseLogLevel(Level level) {
    if (isUsingGemFireDefaultConfig()) {
      Configurator.setLevel(ROOT_LOGGER_NAME, level);
    }
    Configurator.setLevel(BASE_LOGGER_NAME, level);
    Configurator.setLevel(MAIN_LOGGER_NAME, level);
  }
  
  public static void setSecurityLogLevel(Level level) {
    Configurator.setLevel(SECURITY_LOGGER_NAME, level);
  }
  
  public static Level getBaseLogLevel() {
    return Configurator.getLevel(BASE_LOGGER_NAME);
  }
  
  public static LoggerConfig getRootLoggerConfig() {
    return Configurator.getLoggerConfig(LogManager.getRootLogger().getName());
  }
  
  /**
   * Removes STDOUT ConsoleAppender from ROOT logger. Only called when using
   * the log4j2-default.xml configuration. This is done when creating the
   * LogWriterAppender for log-file. The Appender instance is stored in 
   * stdoutAppender so it can be restored later using restoreConsoleAppender.
   */
  public static synchronized void removeConsoleAppender() {
    final AppenderContext appenderContext = LogService.getAppenderContext(LogService.ROOT_LOGGER_NAME);
    final LoggerConfig config = appenderContext.getLoggerConfig();
    Appender stdout = config.getAppenders().get(STDOUT);
    if (stdout != null) {
      config.removeAppender(STDOUT);
      stdoutAppender = stdout;
      appenderContext.getLoggerContext().updateLoggers();
    }
  }
  
  /**
   * Restores STDOUT ConsoleAppender to ROOT logger. Only called when using
   * the log4j2-default.xml configuration. This is done when the 
   * LogWriterAppender for log-file is destroyed. The Appender instance stored 
   * in stdoutAppender is used.
   */
  public static synchronized void restoreConsoleAppender() {
    if (stdoutAppender == null) {
      return;
    }
    final AppenderContext appenderContext = LogService.getAppenderContext(LogService.ROOT_LOGGER_NAME);
    final LoggerConfig config = appenderContext.getLoggerConfig();
    Appender stdout = config.getAppenders().get(STDOUT);
    if (stdout == null) {
      config.addAppender(stdoutAppender, Level.ALL, null);
      appenderContext.getLoggerContext().updateLoggers();
    }
  }
  
  private LogService() {
    // do not instantiate
  }
}
