/*
 * =========================================================================
 *  Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */

package com.vmware.gemfire.tools.pulse.internal.log;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import com.vmware.gemfire.tools.pulse.internal.data.PulseConstants;

/**
 * Class PulseLogger
 * 
 * PulseLogger is the custom logger class for Pulse Web Application. It logs
 * messages to the file in custom format.
 * 
 * @author Sachin K
 * @since version 7.0.Beta
 */
public class PulseLogger {

  // Pulse Application Log File
  private static final String LOG_FILE_NAME = PulseConstants.PULSE_LOG_FILE_LOCATION
      + "/" + PulseConstants.PULSE_LOG_FILE;

  // Date pattern to be used in log messages
  public static final String LOG_MESSAGE_DATE_PATTERN = "dd/MM/yyyy hh:mm:ss.SSS";

  // The log file size is set to 1MB.
  public static final int FILE_SIZE = 1024 * 1024;

  // The log file count set to 1 files.
  public static final int FILE_COUNT = 5;

  // Append logs is set to true.
  public static final boolean FLAG_APPEND = true;

  // Log File handle Object
  private static FileHandler fileHandler;

  // Message Formatter Object
  private static MessageFormatter messageformatter;

  // Logger Object
  private static Logger logger;

  public static Logger getLogger(String name) {
    // Create Logger
    logger = Logger.getLogger(name);

    // Set minimum log level to inform
    logger.setLevel(Level.INFO);

    // Get file handler to log messages into log file.
    try {
      // fileHandler = new FileHandler(LOG_FILE_NAME, FILE_SIZE, FILE_COUNT,
      // FLAG_APPEND);
      fileHandler = new FileHandler(LOG_FILE_NAME, FLAG_APPEND);

      // Log Message Formatter
      messageformatter = new MessageFormatter();

      fileHandler.setFormatter(messageformatter);

      // Add File Handler to logger object
      logger.addHandler(fileHandler);

    } catch (SecurityException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }

    return logger;
  }

  /**
   * Class MessageFormatter
   * 
   * MessageFormatter is the custom formatter class for formatting the log
   * messages.
   * 
   * @author Sachin K
   * @since version 7.0.Beta 2012-09-23
   */
  private static class MessageFormatter extends Formatter {

    public MessageFormatter() {
      super();
    }

    @Override
    public String format(LogRecord record) {
      // Set DateFormat
      DateFormat df = new SimpleDateFormat(PulseLogger.LOG_MESSAGE_DATE_PATTERN);
      StringBuilder builder = new StringBuilder(1000);

      // Format Log Message
      builder.append(df.format(new Date(record.getMillis()))).append(" - ");
      builder.append("[ " + PulseConstants.APP_NAME + " ] - ");
      builder.append("[").append(record.getSourceClassName()).append(".");
      builder.append(record.getSourceMethodName()).append("] - ");
      builder.append("[").append(record.getLevel()).append("] - ");
      builder.append(formatMessage(record));
      builder.append(System.getProperty("line.separator"));

      return builder.toString();
    }

    public String getHead(Handler h) {
      return super.getHead(h);
    }

    public String getTail(Handler h) {
      return super.getTail(h);
    }
  } // End of Class MessageFormatter
}