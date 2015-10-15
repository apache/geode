package com.gemstone.gemfire.internal.logging.log4j;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilterable;

import com.gemstone.gemfire.internal.logging.LogService;

/**
 * Utility methods to programmatically alter the configuration of Log4J2. Used 
 * by LogService and tests.
 */
public class Configurator {

  /*public static void reconfigure() {
    LoggerContext context = (LoggerContext)LogManager.getContext(false);
    context.updateLoggers();
    //context.reconfigure();
  }*/
  
  public static void shutdown() {
    //LoggerContext context = (LoggerContext)LogManager.getContext(false);
    final LoggerContext context = ((org.apache.logging.log4j.core.Logger)LogManager.getRootLogger()).getContext();
    context.stop();
    org.apache.logging.log4j.core.config.Configurator.shutdown(context);
  }

  
  public static void setLevel(String name, Level level) {
    LoggerContext context = (LoggerContext)LogManager.getContext(false);
    LoggerConfig logConfig = getLoggerConfig(name);

    logConfig.setLevel(level);
    context.updateLoggers();
    
    if (level.isLessSpecificThan(Level.DEBUG)) {
      LogService.configureFastLoggerDelegating();
    }
  }
  
  public static Level getLevel(String name) {
    LoggerConfig logConfig = getOrCreateLoggerConfig(name);
    return logConfig.getLevel();
  }
  
  public static LoggerConfig getOrCreateLoggerConfig(String name) {
    LoggerContext context = (LoggerContext)LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    LoggerConfig logConfig = config.getLoggerConfig(name);
    boolean update = false;
    if (!logConfig.getName().equals(name)) {
      List<AppenderRef> appenderRefs = logConfig.getAppenderRefs();
      Map<Property, Boolean> properties = logConfig.getProperties();
      Set<Property> props = properties == null ? null : properties.keySet();
      logConfig = LoggerConfig.createLogger(
          String.valueOf(logConfig.isAdditive()), 
          logConfig.getLevel(), 
          name, 
          String.valueOf(logConfig.isIncludeLocation()), 
          appenderRefs == null ? null : appenderRefs.toArray(new AppenderRef[appenderRefs.size()]), 
          props == null ? null : props.toArray(new Property[props.size()]), 
          config, 
          null);
      config.addLogger(name, logConfig);
      update = true;
    }
    if (update) {
      context.updateLoggers();
    }
    return logConfig;
  }

  public static LoggerConfig getOrCreateLoggerConfig(String name, boolean additive, boolean forceAdditivity) {
    LoggerContext context = (LoggerContext)LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    LoggerConfig logConfig = config.getLoggerConfig(name);
    boolean update = false;
    if (!logConfig.getName().equals(name)) {
      List<AppenderRef> appenderRefs = logConfig.getAppenderRefs();
      Map<Property, Boolean> properties = logConfig.getProperties();
      Set<Property> props = properties == null ? null : properties.keySet();
      logConfig = LoggerConfig.createLogger(
          String.valueOf(additive), 
          logConfig.getLevel(), 
          name, 
          String.valueOf(logConfig.isIncludeLocation()), 
          appenderRefs == null ? null : appenderRefs.toArray(new AppenderRef[appenderRefs.size()]), 
          props == null ? null : props.toArray(new Property[props.size()]), 
          config, 
          null);
      config.addLogger(name, logConfig);
      update = true;
    }
    if (forceAdditivity && logConfig.isAdditive() != additive) {
      logConfig.setAdditive(additive);
      update = true;
    }
    if (update) {
      context.updateLoggers();
    }
    return logConfig;
  }
  
  public static LoggerConfig getLoggerConfig(final String name) {
    LoggerContext context = (LoggerContext)LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    LoggerConfig logConfig = config.getLoggerConfig(name);
    if (!logConfig.getName().equals(name)) {
      throw new IllegalStateException("LoggerConfig does not exist for " + name);
    }
    return logConfig;
  }

  public static boolean hasContextWideFilter(final Configuration config) {
    return config.hasFilter();
  }
  
  public static String getConfigurationSourceLocation(final Configuration config) {
    return config.getConfigurationSource().getLocation();
  }
  
  public static boolean hasAppenderFilter(final Configuration config) {
    for (Appender appender : config.getAppenders().values()) {
      if (appender instanceof AbstractFilterable) {
        if (((AbstractFilterable) appender).hasFilter()) {
          return true;
        }
      }
    }
    return false;
  }
  
  public static boolean hasDebugOrLower(final Configuration config) {
    for (LoggerConfig loggerConfig : config.getLoggers().values()) {
      boolean isDebugOrLower = loggerConfig.getLevel().isLessSpecificThan(Level.DEBUG);
      if (isDebugOrLower) {
        return true;
      }
    }
    return false;
  }
  
  public static boolean hasLoggerFilter(final Configuration config) {
    for (LoggerConfig loggerConfig : config.getLoggers().values()) {
      boolean isRoot = loggerConfig.getName().equals("");
      boolean isGemFire = loggerConfig.getName().startsWith(LogService.BASE_LOGGER_NAME);
      boolean hasFilter = loggerConfig.hasFilter();
      boolean isGemFireVerboseFilter = hasFilter && LogService.GEMFIRE_VERBOSE_FILTER.equals(loggerConfig.getFilter().toString());
      
      if (isRoot || isGemFire) {
        // check for Logger Filter
        if (hasFilter && !isGemFireVerboseFilter) {
          return true;
        }
      }
    }
    return false;
  }
  
  public static boolean hasAppenderRefFilter(final Configuration config) {
    for (LoggerConfig loggerConfig : config.getLoggers().values()) {
      boolean isRoot = loggerConfig.getName().equals("");
      boolean isGemFire = loggerConfig.getName().startsWith(LogService.BASE_LOGGER_NAME);
      boolean hasFilter = loggerConfig.hasFilter();
      boolean isGemFireVerboseFilter = hasFilter && LogService.GEMFIRE_VERBOSE_FILTER.equals(loggerConfig.getFilter().toString());
      
      if (isRoot || isGemFire) {
        // check for AppenderRef Filter
        for (AppenderRef appenderRef : loggerConfig.getAppenderRefs()) {
          if (appenderRef.getFilter() != null) {
            return true;
          }
        }
      }
    }
    return false;
  }
}
