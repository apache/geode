/*
 *
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
 *
 */

package org.apache.geode.tools.pulse.internal.log;

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

import org.apache.geode.tools.pulse.internal.data.PulseConstants;

/**
 * Class PulseLogger
 * 
 * PulseLogger is the custom logger class for Pulse Web Application. It logs
 * messages to the file in custom format.
 * 
 * @since GemFire version 7.0.Beta
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
   * @since GemFire version 7.0.Beta 2012-09-23
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