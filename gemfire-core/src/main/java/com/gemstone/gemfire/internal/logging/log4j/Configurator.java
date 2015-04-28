package com.gemstone.gemfire.internal.logging.log4j;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

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
  
  public static void setLevel(String name, Level level) {
    LoggerContext context = (LoggerContext)LogManager.getContext(false);
    LoggerConfig logConfig = getLoggerConfig(name);

    logConfig.setLevel(level);
    context.updateLoggers();
    
    if (level.isLessSpecificThan(Level.DEBUG)) {
      LogService.setFastLoggerDebugAvailableFlag();
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
}
