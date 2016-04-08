/*
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
 */

package hydra.log;

import hydra.HydraRuntimeException;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.LogWriterImpl;
import com.gemstone.gemfire.i18n.StringId;

import java.io.*;

/**
 *
 *  A {@link com.gemstone.gemfire.LogWriter} that writes to a file logger,
 *  depending on whether it is turned on via LogPrms#file_logging.
 *  <p>
 *  The log level query methods answer true if a file logger is active and
 *  answer true.  See LogPrms#file_logLevel.
 */

public class AnyLogWriter implements InternalLogWriter {

  private boolean FILE_LOGGING;
//  private String filePrefix;
  private InternalLogWriter file;

  /**
   *  Create a log writer to stdout.
   *  @param levelName the log level.
   */
  public AnyLogWriter( String levelName ) {
    this.FILE_LOGGING = true;
    int level = LogWriterImpl.levelNameToCode( levelName );
    this.file = new LocalLogWriter( level, System.out );
  }

  /**
   *  Create a log writer to a file of unlimited size.
   *  @param filePrefix the prefix for the filename of the log.
   *  @param levelName the log level.
   */
  public AnyLogWriter( String filePrefix, String levelName, boolean append ) {
    this.FILE_LOGGING = true;
    FileOutputStream fos;
    String fn = filePrefix + ".log";
    try {
      fos = new FileOutputStream( fn, append );
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Unable to open " + fn, e );
    }
    PrintStream ps = new PrintStream( fos, true ); // autoflush
    System.setOut( ps ); System.setErr( ps );
    int level = LogWriterImpl.levelNameToCode( levelName );
    this.file = new LocalLogWriter( level, ps );
  }

  /**
   *  Create a log writer to a file of unlimited size in the specified directory.
   *  @param filePrefix the prefix for the filename of the log.
   *  @param levelName the log level.
   *  @param dir the directory in which to create the file.
   */
  public AnyLogWriter( String filePrefix, String levelName, String dir, boolean append ) {
    this.FILE_LOGGING = true;
    FileOutputStream fos;
    String fn = dir + File.separator + filePrefix + ".log";
    try {
      fos = new FileOutputStream( fn, append );
    } catch( IOException e ) {
      throw new HydraRuntimeException( "Unable to open " + fn, e );
    }
    PrintStream ps = new PrintStream( fos, true ); // autoflush
    System.setOut( ps ); System.setErr( ps );
    int level = LogWriterImpl.levelNameToCode( levelName );
    this.file = new LocalLogWriter( level, ps );
  }

  /**
   *  Create a log writer to a file.  May be circular.
   *  @param filePrefix the prefix for names of files created by this logwriter.
   *  @param fileLogging turn on logging to the file.
   *  @param fileLogLevelName name of the file log level.
   *  @param fileMaxKBPerVM the maximum size of the file log per VM, in kilobytes .
   */
  public AnyLogWriter( String filePrefix, boolean fileLogging,
                       String fileLogLevelName, int fileMaxKBPerVM ) {

//    this.filePrefix = filePrefix;
    if ( fileLogging ) {
      this.FILE_LOGGING = fileLogging;
      if ( fileMaxKBPerVM < 0 )
        throw new IllegalArgumentException( "Illegal (negative) file log length: " + fileMaxKBPerVM );
      int maxBytes = fileMaxKBPerVM * 1024;
      CircularOutputStream cos;
      String fn = filePrefix + ".log";
      try {
        cos = new CircularOutputStream( fn, maxBytes );
      } catch( IOException e ) {
        throw new HydraRuntimeException( "Unable to create " + fn, e );
      }
      // create a local log writer using the circular file
      int level = LogWriterImpl.levelNameToCode( fileLogLevelName );
      this.file = new LocalLogWriter( level, new PrintStream( cos ) );
    }
  }

  /**
   *  Gets the writer's level.  Returns the level obtained from active logger.
   */
  public int getLevel() {
    if ( FILE_LOGGING )
      return ((LocalLogWriter)file).getLogWriterLevel();
    else
      return LogWriterImpl.NONE_LEVEL;
  }
  /**
   *  Sets the writer's level.  Applies to any active logger.
   *  @throws IllegalArgumentException if level is not in legal range
   */
  public void setLevel(int newLevel) {
    if ( FILE_LOGGING )
      ((LocalLogWriter)file).setLevel( newLevel );
  }

  public void setLogWriterLevel(int newLevel) {
    setLevel(newLevel);
  }
  
////////////////////////////////////////////////////////////////////////////////
////                           LOGWRITER INTERFACE                         /////
////////////////////////////////////////////////////////////////////////////////

  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#severeEnabled}.
   *  Answers true if the file logger answers true.
   */
  public boolean severeEnabled() {
    if ( FILE_LOGGING )
      return file.severeEnabled();
    else
      return false;
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#severe(String,Throwable)}.
   */
  public void severe(String msg, Throwable ex) {
    if ( FILE_LOGGING ) file.severe(msg,ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#severe(String)}.
   */
  public void severe(String msg) {
    if ( FILE_LOGGING ) file.severe(msg);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#severe(Throwable)}.
   */
  public void severe(Throwable ex) {
    if ( FILE_LOGGING ) file.severe(ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#errorEnabled}.
   *  Answers true if the file logger answers true.
   */
  public boolean errorEnabled() {
    if ( FILE_LOGGING )
      return file.errorEnabled();
    else
      return false;
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#error(String,Throwable)}.
   */
  public void error(String msg, Throwable ex) {
    if ( FILE_LOGGING ) file.error(msg, ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#error(String)}.
   */
  public void error(String msg) {
    if ( FILE_LOGGING ) file.error(msg);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#error(Throwable)}.
   */
  public void error(Throwable ex) {
    if ( FILE_LOGGING ) file.error(ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#warningEnabled}.
   *  Answers true if the file logger answers true.
   */
  public boolean warningEnabled() {
    if ( FILE_LOGGING )
      return file.warningEnabled();
    else
      return false;
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#warning(String,Throwable)}.
   */
  public void warning(String msg, Throwable ex) {
    if ( FILE_LOGGING ) file.warning(msg,ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#warning(String)}.
   */
  public void warning(String msg) {
    if ( FILE_LOGGING ) file.warning(msg);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#warning(Throwable)}.
   */
  public void warning(Throwable ex) {
    if ( FILE_LOGGING ) file.warning(ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#infoEnabled}.
   *  Answers true if the file logger answers true.
   */
  public boolean infoEnabled() {
    if ( FILE_LOGGING )
      return file.infoEnabled();
    else
      return false;
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#info(String,Throwable)}.
   */
  public void info(String msg, Throwable ex) {
    if ( FILE_LOGGING ) file.info(msg,ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#info(String)}.
   */
  public void info(String msg) {
    if ( FILE_LOGGING ) file.info(msg);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#info(Throwable)}.
   */
  public void info(Throwable ex) {
    if ( FILE_LOGGING ) file.info(ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#configEnabled}.
   *  Answers true if the file logger answers true.
   */
  public boolean configEnabled() {
    if ( FILE_LOGGING )
      return file.configEnabled();
    else
      return false;
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#config(String,Throwable)}.
   */
  public void config(String msg, Throwable ex) {
    if ( FILE_LOGGING ) file.config(msg,ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#config(String)}.
   */
  public void config(String msg) {
    if ( FILE_LOGGING ) file.config(msg);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#config(Throwable)}.
   */
  public void config(Throwable ex) {
    if ( FILE_LOGGING ) file.config(ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#fineEnabled}.
   *  Answers true if the file logger answers true.
   */
  public boolean fineEnabled() {
    if ( FILE_LOGGING )
      return file.fineEnabled();
    else
      return false;
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#fine(String,Throwable)}.
   */
  public void fine(String msg, Throwable ex) {
    if ( FILE_LOGGING ) file.fine(msg,ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#fine(String)}.
   */
  public void fine(String msg) {
    if ( FILE_LOGGING ) file.fine(msg);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#fine(Throwable)}.
   */
  public void fine(Throwable ex) {
    if ( FILE_LOGGING ) file.fine(ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#finerEnabled}.
   *  Answers true if the file logger answers true.
   */
  public boolean finerEnabled() {
    if ( FILE_LOGGING )
      return file.finerEnabled();
    else
      return false;
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#finer(String,Throwable)}.
   */
  public void finer(String msg, Throwable ex) {
    if ( FILE_LOGGING ) file.finer(msg,ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#finer(String)}.
   */
  public void finer(String msg) {
    if ( FILE_LOGGING ) file.finer(msg);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#finer(Throwable)}.
   */
  public void finer(Throwable ex) {
    if ( FILE_LOGGING ) file.finer(ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#finestEnabled}.
   *  Answers true if the file logger answers true.
   */
  public boolean finestEnabled() {
    if ( FILE_LOGGING )
      return file.finestEnabled();
    else
      return false;
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#finest(String,Throwable)}.
   */
  public void finest(String msg, Throwable ex) {
    if ( FILE_LOGGING ) file.finest(msg,ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#finest(String)}.
   */
  public void finest(String msg) {
    if ( FILE_LOGGING ) file.finest(msg);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#finest(Throwable)}.
   */
  public void finest(Throwable ex) {
    if ( FILE_LOGGING ) file.finest(ex);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#entering(String,String)}.
   */
  public void entering(String sourceClass, String sourceMethod) {
    if ( FILE_LOGGING ) file.entering(sourceClass,sourceMethod);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#exiting(String,String)}.
   */
  public void exiting(String sourceClass, String sourceMethod) {
    if ( FILE_LOGGING ) file.exiting(sourceClass,sourceMethod);
  }
  /**
   *  Implements {@link com.gemstone.gemfire.LogWriter#throwing(String,String,Throwable)}.
   */
  public void throwing(String sourceClass, String sourceMethod, Throwable thrown) {
    if ( FILE_LOGGING ) file.throwing(sourceClass,sourceMethod,thrown);
  }
  public java.util.logging.Handler getHandler() {
    return null;
  }

  public void config(StringId msgId, Object param, Throwable ex) {
    config(msgId.toLocalizedString(param), ex);  
  }

  public void config(StringId msgId, Object param) {
    config(msgId.toLocalizedString(param));
  }

  public void config(StringId msgId, Object[] params, Throwable ex) {
    config(msgId.toLocalizedString(params), ex);
  }

  public void config(StringId msgId, Object[] params) {
    config(msgId.toLocalizedString(params));
  }

  public void config(StringId msgId, Throwable ex) {
    config(msgId.toLocalizedString(), ex);
  }

  public void config(StringId msgId) {
    config(msgId.toLocalizedString());
  }

  public void error(StringId msgId, Object param, Throwable ex) {
    error(msgId.toLocalizedString(param), ex);
  }

  public void error(StringId msgId, Object param) {
    error(msgId.toLocalizedString(param));
  }

  public void error(StringId msgId, Object[] params, Throwable ex) {
    error(msgId.toLocalizedString(params), ex);
  }

  public void error(StringId msgId, Object[] params) {
    error(msgId.toLocalizedString(params));
  }

  public void error(StringId msgId, Throwable ex) {
    error(msgId.toLocalizedString(), ex);
  }

  public void error(StringId msgId) {
    error(msgId.toLocalizedString());
  }

  public void info(StringId msgId, Object param, Throwable ex) {
    info(msgId.toLocalizedString(param), ex);    
  }

  public void info(StringId msgId, Object param) {
    info(msgId.toLocalizedString(param));
  }

  public void info(StringId msgId, Object[] params, Throwable ex) {
    info(msgId.toLocalizedString(params), ex); 
  }

  public void info(StringId msgId, Object[] params) {
    info(msgId.toLocalizedString(params));    
  }

  public void info(StringId msgId, Throwable ex) {
    info(msgId.toLocalizedString(), ex);  
  }
  
  public void info(StringId msgId) {
    info(msgId.toLocalizedString()); 
  }

  public void severe(StringId msgId, Object param, Throwable ex) {
    severe(msgId.toLocalizedString(param), ex);
  }

  public void severe(StringId msgId, Object param) {
    severe(msgId.toLocalizedString(param));
  }

  public void severe(StringId msgId, Object[] params, Throwable ex) {
    severe(msgId.toLocalizedString(params), ex); 
  }

  public void severe(StringId msgId, Object[] params) {
    severe(msgId.toLocalizedString(params));
  }

  public void severe(StringId msgId, Throwable ex) {
    severe(msgId.toLocalizedString(), ex);    
  }
  
  public void severe(StringId msgId) {
    severe(msgId.toLocalizedString());
  }

  public void warning(StringId msgId, Object param, Throwable ex) {
    warning(msgId.toLocalizedString(param), ex);
  }

  public void warning(StringId msgId, Object param) {
    warning(msgId.toLocalizedString(param));
  }

  public void warning(StringId msgId, Object[] params, Throwable ex) {
    warning(msgId.toLocalizedString(params), ex);
  }

  public void warning(StringId msgId, Object[] params) {
    warning(msgId.toLocalizedString(params));
  }

  public void warning(StringId msgId, Throwable ex) {
    warning(msgId.toLocalizedString(), ex);
  }
    
  public void warning(StringId msgId) {
    warning(msgId.toLocalizedString()); 
  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.LogWriterI18n#convertToLogWriter()
   */ 
  public LogWriter convertToLogWriter() {
    return this;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.LogWriter#convertToLogWriterI18n()
   */
  public LogWriterI18n convertToLogWriterI18n() {
    return this;
  }

  @Override
  public int getLogWriterLevel() {
    return file.getLogWriterLevel();
  }
  
  @Override
  public boolean isSecure() {
    return false;
  }
  
  @Override
  public String getConnectionName() {
    return null;
  }

  @Override
  public void put(int msgLevel, String msg, Throwable exception) {
    file.put(msgLevel, msg, exception);
  }

  @Override
  public void put(int msgLevel, StringId msgId, Object[] params,Throwable exception) {
    file.put(msgLevel, msgId, params, exception);
  }
}
