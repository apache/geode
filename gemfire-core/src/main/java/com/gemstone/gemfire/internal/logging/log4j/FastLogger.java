package com.gemstone.gemfire.internal.logging.log4j;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.message.MessageFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;
import org.apache.logging.log4j.status.StatusLogger;

/**
 * Overrides is enabled checks for log levels below INFO to avoid performance
 * penalties when the log level is INFO or above.
 * 
 * @author Kirk Lund
 * @author David Hoots
 */
public class FastLogger extends ExtendedLoggerWrapper {
  private static final long serialVersionUID = 7084130827962463327L;

  private static volatile boolean debugAvailable = true;
  
  public FastLogger(final Logger logger) {
    this((ExtendedLogger) logger, logger.getName(), logger.getMessageFactory());
  }

  public FastLogger(final ExtendedLogger logger, final String name, final MessageFactory messageFactory) {
    super(logger, name, messageFactory);
  }

  public static void setDebugAvailable(final boolean newValue) {
    StatusLogger.getLogger().debug("Setting debugAvailable to {}", newValue);
    debugAvailable = newValue;
  }
  
  /**
   * Checks whether this Logger is enabled for the {@link Level#DEBUG DEBUG} Level.
   *
   * @return boolean - {@code true} if this Logger is enabled for level DEBUG, {@code false} otherwise.
   */
  @Override
  public boolean isDebugEnabled() {
    return debugAvailable && super.isDebugEnabled();
  }

  /**
   * Checks whether this Logger is enabled for the {@link Level#DEBUG DEBUG} Level.
   *
   * @param marker The marker data specific to this log statement.
   * @return boolean - {@code true} if this Logger is enabled for level DEBUG, {@code false} otherwise.
   */
  @Override
  public boolean isDebugEnabled(final Marker marker) {
    return debugAvailable && super.isDebugEnabled(marker);
  }

  /**
   * Checks whether this Logger is enabled for the {@link Level#TRACE TRACE} level.
   *
   * @return boolean - {@code true} if this Logger is enabled for level TRACE, {@code false} otherwise.
   */
  @Override
  public boolean isTraceEnabled() {
    return debugAvailable && super.isTraceEnabled();
  }

  /**
   * Checks whether this Logger is enabled for the {@link Level#TRACE TRACE} level.
   *
   * @param marker The marker data specific to this log statement.
   * @return boolean - {@code true} if this Logger is enabled for level TRACE, {@code false} otherwise.
   */
  @Override
  public boolean isTraceEnabled(final Marker marker) {
    return debugAvailable && super.isTraceEnabled(marker);
  }
  
  public boolean isDebugAvailable() {
    return debugAvailable;
  }
  
  public Logger getExtendedLogger() {
    return super.logger;
  }
}
