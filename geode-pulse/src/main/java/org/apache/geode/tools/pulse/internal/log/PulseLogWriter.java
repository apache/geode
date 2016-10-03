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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.geode.tools.pulse.internal.data.PulseConfig;
import org.apache.geode.tools.pulse.internal.data.Repository;

/**
 * Class PulseLogWriter
 * 
 * PulseLogWriter is the implementation of LogWriter.
 * 
 * @since GemFire 7.0.1
 * 
 */
public class PulseLogWriter implements LogWriter {

  // Log File handle Object
  private FileHandler fileHandler;

  // Message Formatter Object
  private static MessageFormatter messageformatter;

  // pulse log writer
  private static PulseLogWriter pulseLogger = null;

  // Logger Object
  private Logger logger;

  private PulseLogWriter() {
    PulseConfig pulseConfig = Repository.get().getPulseConfig();
    // Create Logger
    logger = Logger.getLogger(this.getClass().getName());

    // Set minimum log level to level passed
    logger.setLevel(pulseConfig.getLogLevel());

    try {
      // Get file handler to log messages into log file.
      if (fileHandler == null) {
        fileHandler = new FileHandler(
            pulseConfig.getLogFileFullName(),
            pulseConfig.getLogFileSize(),
            pulseConfig.getLogFileCount(),
            pulseConfig.getLogAppend());

        // Log Message Formatter
        messageformatter = new MessageFormatter();
        fileHandler.setFormatter(messageformatter);
      }

      // Add File Handler to logger object
      logger.addHandler(fileHandler);
    } catch (SecurityException e) {
      logger.setUseParentHandlers(true);
      e.printStackTrace();
    } catch (IOException e) {
      logger.setUseParentHandlers(true);
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * @param jsonErr
   * @param errorData
   */
  public void logJSONError(Exception jsonErr, Object errorData) {

    // print details of thrown exception and data that couldn't be converted to
    // json
    if (this.fineEnabled()) {

      // write errors
      StringWriter swBuffer = new StringWriter();
      PrintWriter prtWriter = new PrintWriter(swBuffer);
      jsonErr.printStackTrace(prtWriter);
      this.fine("JSON Error Details : " + swBuffer.toString() + "\n");

      this.fine("Erroneous Data : "
          + ((errorData != null) ? errorData.toString()
              : "Not Available for output") + "\n");
    }
  }

  public static synchronized PulseLogWriter getLogger() {
    if (null == pulseLogger) {
      pulseLogger = new PulseLogWriter();
    }
    return pulseLogger;
  }

  @Override
  public boolean severeEnabled() {
    return logger.isLoggable(Level.SEVERE);
  }

  @Override
  public void severe(String msg, Throwable ex) {
    logger.logp(Level.SEVERE, "", "", msg, ex);
  }

  @Override
  public void severe(String msg) {
    logger.severe(msg);
  }

  @Override
  public void severe(Throwable ex) {
    logger.logp(Level.SEVERE, "", "", "", ex);
  }

  /*
  @Override
  public boolean errorEnabled() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void error(String msg, Throwable ex) {
    // TODO Auto-generated method stub

  }

  @Override
  public void error(String msg) {
    // TODO Auto-generated method stub

  }

  @Override
  public void error(Throwable ex) {
    // TODO Auto-generated method stub

  }
  */

  @Override
  public boolean warningEnabled() {
    return logger.isLoggable(Level.WARNING);
  }

  @Override
  public void warning(String msg, Throwable ex) {
    logger.logp(Level.WARNING, "", "", msg, ex);
  }

  @Override
  public void warning(String msg) {
    logger.warning(msg);
  }

  @Override
  public void warning(Throwable ex) {
    logger.logp(Level.WARNING, "", "", "", ex);
  }

  @Override
  public boolean infoEnabled() {
    return logger.isLoggable(Level.INFO);
  }

  @Override
  public void info(String msg, Throwable ex) {
    logger.logp(Level.INFO, "", "", msg, ex);
  }

  @Override
  public void info(String msg) {
    logger.info(msg);
  }

  @Override
  public void info(Throwable ex) {
    logger.logp(Level.WARNING, "", "", "", ex);
  }

  @Override
  public boolean configEnabled() {
    return logger.isLoggable(Level.CONFIG);
  }

  @Override
  public void config(String msg, Throwable ex) {
    logger.logp(Level.CONFIG, "", "", msg, ex);
  }

  @Override
  public void config(String msg) {
    logger.config(msg);
  }

  @Override
  public void config(Throwable ex) {
    logger.logp(Level.CONFIG, "", "", "", ex);
  }

  @Override
  public boolean fineEnabled() {
    return logger.isLoggable(Level.FINE);
  }

  @Override
  public void fine(String msg, Throwable ex) {
    logger.logp(Level.FINE, "", "", msg, ex);
  }

  @Override
  public void fine(String msg) {
    logger.fine(msg);
  }

  @Override
  public void fine(Throwable ex) {
    logger.logp(Level.FINE, "", "", "", ex);
  }

  @Override
  public boolean finerEnabled() {
    return logger.isLoggable(Level.FINER);
  }

  @Override
  public void finer(String msg, Throwable ex) {
    logger.logp(Level.FINER, "", "", msg, ex);
  }

  @Override
  public void finer(String msg) {
    logger.finer(msg);
  }

  @Override
  public void finer(Throwable ex) {
    logger.logp(Level.FINER, "", "", "", ex);
  }

  @Override
  public void entering(String sourceClass, String sourceMethod) {
    logger.entering(sourceClass, sourceMethod);
  }

  @Override
  public void exiting(String sourceClass, String sourceMethod) {
    logger.exiting(sourceClass, sourceMethod);
  }

  @Override
  public void throwing(String sourceClass, String sourceMethod, Throwable thrown) {
    logger.throwing(sourceClass, sourceMethod, thrown);
  }

  @Override
  public boolean finestEnabled() {
    return logger.isLoggable(Level.FINEST);
  }

  @Override
  public void finest(String msg, Throwable ex) {
    logger.logp(Level.FINEST, "", "", msg, ex);
  }

  @Override
  public void finest(String msg) {
    logger.finest(msg);
  }

  @Override
  public void finest(Throwable ex) {
    logger.logp(Level.FINEST, "", "", "", ex);
  }

}
