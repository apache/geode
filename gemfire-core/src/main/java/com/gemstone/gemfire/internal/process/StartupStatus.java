package com.gemstone.gemfire.internal.process;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.org.jgroups.util.StringId;

/**
 * Extracted from LogWriterImpl and changed to static.
 * 
 * @author Kirk Lund
 */
public class StartupStatus {
  private static final Logger logger = LogService.getLogger();

  /** protected by static synchronized */
  private static StartupStatusListener listener;
  
  /**
   * Writes both a message and exception to this writer.
   * If a startup listener is registered,
   * the message will be written to the listener as well
   * to be reported to a user.
   * @since 7.0
   */
  public static synchronized void startup(StringId msgID, Object[] params) {
    String message = msgID.toLocalizedString(params);
    
    if (listener != null) {
      listener.setStatus(message);
    }
    
    logger.info(message);
  }

  public static synchronized void setListener(StartupStatusListener listener) {
    StartupStatus.listener = listener;
  }
  
  public static synchronized StartupStatusListener getStartupListener() {
    return StartupStatus.listener;
  }
  
  public static synchronized void clearListener() {
    StartupStatus.listener = null;
  }
}
