package com.gemstone.gemfire.internal.logging;


import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.Banner;
import com.gemstone.gemfire.internal.ConfigSource;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.internal.logging.log4j.LogMarker;
import com.gemstone.gemfire.internal.logging.log4j.LogWriterLogger;

/**
 * Creates LogWriterLogger instances.
 * 
 * @author Kirk Lund
 */
public class LogWriterFactory {

  // LOG: RemoteGfManagerAgent and CacheCreation use this when there's no InternalDistributedSystem
  public static InternalLogWriter toSecurityLogWriter(final InternalLogWriter logWriter) {
    return new SecurityLogWriter(logWriter.getLogWriterLevel(), logWriter);
  }

  /**
   * Creates the log writer for a distributed system based on the system's
   * parsed configuration. The initial banner and messages are also entered into
   * the log by this method.
   * @param isLoner
   *                Whether the distributed system is a loner or not
   * @param isSecure
   *                Whether a logger for security related messages has to be
   *                created
   * @param config
   *                The DistributionConfig for the target distributed system
   * @param logConfig if true log the configuration
   */
  public static InternalLogWriter createLogWriterLogger(final boolean isLoner,
                                                        final boolean isSecure,
                                                        final LogConfig config,
                                                        final boolean logConfig) {

    // if isSecurity then use "com.gemstone.gemfire.security" else use "com.gemstone.gemfire"
    String name = null;
    if (isSecure){
      name = LogService.SECURITY_LOGGER_NAME;
    } else {
      name = LogService.MAIN_LOGGER_NAME;
    }
    
    // create the LogWriterLogger
    final LogWriterLogger logger = LogService.createLogWriterLogger(name, config.getName(), isSecure);
    
    if (isSecure) {
      logger.setLogWriterLevel(((DistributionConfig) config).getSecurityLogLevel());
    } else {
      boolean defaultSource = false;
      if (config instanceof DistributionConfig) {
        ConfigSource source = ((DistributionConfig) config).getConfigSource(DistributionConfig.LOG_LEVEL_NAME);
        if (source == null) {
          defaultSource = true;
        }
      }
      if (!defaultSource) {
        // LOG: fix bug #51709 by not setting if log-level was not specified
        // LOG: let log4j2.xml specify log level which defaults to INFO
        logger.setLogWriterLevel(config.getLogLevel()); 
      }
    }
    
    // log the banner
    if (InternalDistributedSystem.getReconnectAttemptCounter() == 0 // avoid filling up logs during auto-reconnect
        && !isSecure && (!isLoner /* do this on a loner to fix bug 35602 */
        || !Boolean.getBoolean(InternalLocator.INHIBIT_DM_BANNER))) {
      // LOG:CONFIG:
      logger.info(LogMarker.CONFIG, Banner.getString(null));
    }

    // log the config
    if (logConfig) {
      if (isLoner) {
        // LOG:CONFIG:
        logger.info(LogMarker.CONFIG, LocalizedMessage.create(LocalizedStrings.InternalDistributedSystem_RUNNING_IN_LOCAL_MODE_SINCE_MCASTPORT_WAS_0_AND_LOCATORS_WAS_EMPTY));
      } else {
        // LOG:CONFIG: changed from config to info
        logger.info(LogMarker.CONFIG, LocalizedMessage.create(LocalizedStrings.InternalDistributedSystem_STARTUP_CONFIGURATIONN_0, config.toLoggerString()));
      }
    }

    return logger;
  }
}
