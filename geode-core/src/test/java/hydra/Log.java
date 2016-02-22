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

package hydra;

import com.gemstone.gemfire.LogWriter;

import hydra.log.AnyLogWriter;

import java.util.Hashtable;

/**
*
* Manages a singleton instance of {@link com.gemstone.gemfire.LogWriter}
* to do all the logging for a VM.  The instance is created using
* <code>createLogWriter</code> and accessed using <code>getLogWriter</code>.
*
*/
public class Log { 

  // the singleton instance
  private static AnyLogWriter logWriter;

  // the name of the singleton instance
  private static String logName;

  // cache for log writers
  private static Hashtable cache;

  /**
  * Creates a singleton log writer that logs to stdout.
  * @param name the name of the singleton log writer.
  * @param levelName only messages at this level or higher will be logged.
  * @return the singleton log writer.
  * @throws HydraRuntimeException if log writer has already been created.
  * @throws IllegalArgumentException if level is illegal.
  */
  public synchronized static LogWriter createLogWriter( String name, String levelName ) {
    if ( logWriter == null ) {
      logWriter = new AnyLogWriter( levelName );
    } else {
      throw new HydraRuntimeException( "Log writer has already been created" );
    }
    logName = name;
    return logWriter;
  }
  /**
  * Creates a singleton log writer that logs to a file.
  * @param name the name of the singleton log writer.
  * @param filePrefix the prefix for the name of the log file.
  * @param levelName only messages at this level or higher will be logged.
  * @param append whether to append to an existing log file.
  * @return the singleton log writer.
  * @throws HydraRuntimeException if log writer has already been created.
  * @throws IllegalArgumentException if level is illegal.
  */
  public synchronized static LogWriter createLogWriter( String name, String filePrefix, String levelName, boolean append ) {
    if ( logWriter == null ) {
      logWriter = new AnyLogWriter( filePrefix, levelName, append );
    } else {
      throw new HydraRuntimeException( "Log writer has already been created" );
    }
    logName = name;
    return logWriter;
  }
  /**
  * Creates a singleton log writer that logs to a file in a specified directory.
  * @param name the name of the singleton log writer.
  * @param filePrefix the prefix for the name of the log file.
  * @param levelName only messages at this level or higher will be logged.
  * @param dir the directory in which to create the log file.
  * @param append whether to append to an existing log file.
  * @return the singleton log writer.
  * @throws HydraRuntimeException if log writer has already been created.
  * @throws IllegalArgumentException if level is illegal.
  */
  public synchronized static LogWriter createLogWriter( String name, String filePrefix, String levelName, String dir, boolean append ) {
    if ( logWriter == null ) {
      logWriter = new AnyLogWriter( filePrefix, levelName, dir, append );
    } else {
      throw new HydraRuntimeException( "Log writer has already been created" );
    }
    logName = name;
    return logWriter;
  }
  /**
  * Creates a singleton log writer that logs to a file.
  * @param name the name of the singleton log writer.
  * @param filePrefix the prefix for files created by this log writer.
  *
  * @return the singleton log writer.
  * @throws HydraRuntimeException if file can't be created or if log writer has
  *         already been created.
  * @throws IllegalArgumentException if level is illegal.
  */
  public synchronized static LogWriter createLogWriter( String name,
                                                        String filePrefix,
                                                        boolean fileLogging,
                                                        String fileLogLevelName,
                                                        int fileMaxKBPerVM ) {
    if ( logWriter == null ) {
      logWriter = new AnyLogWriter( filePrefix, fileLogging, fileLogLevelName,
                                    fileMaxKBPerVM );
    } else {
      throw new HydraRuntimeException( "Log writer has already been created" );
    }
    logName = name;
    return logWriter;
  }
  /**
  * Closes the singleton log writer.  After this method executes, there is no
  * singleton log writer.
  * @throws HydraRuntimeException if the singleton log writer does not exist.
  */
  public static void closeLogWriter() {
    if ( logWriter == null ) {
      throw new HydraRuntimeException( "Log writer does not exist" );
    } else {
      logName = null;
      logWriter = null;
    }
  }
  /**
  * Caches the singleton log writer so another log writer can be created.
  * After this method executes, there is no singleton log writer.
  * @throws HydraRuntimeException if the singleton log writer does not exist or
  *                               has already been cached.
  */
  public static void cacheLogWriter() {
    if ( logWriter == null ) {
      throw new HydraRuntimeException( "Log writer has not been created" );
    } else {
      if ( cache == null )
        cache = new Hashtable();
      if ( cache.get( logName ) != null )
        throw new HydraRuntimeException( "Log writer " + logName + " has already been cached" );
      cache.put( logName, logWriter );
      logName = null;
      logWriter = null;
    }
  }
  /**
  * Uncaches the log writer with the specified name, blowing away the existing one
  * (unless it was previously cached).  After this method executes, the named log
  * writer is the singleton log writer.
  * @param name the name of the log writer to uncache.
  * @return the uncached (now active) log writer.
  * @throws HydraRuntimeException if the named log writer does not exist or there
  *                               is already a singleton log writer.
  */
  public static LogWriter uncacheLogWriter( String name ) {
    if ( cache == null )
      throw new HydraRuntimeException( "Log writer " + name + " has not been cached" );
    if ( logWriter != null )
      throw new HydraRuntimeException( "Log writer " + name + " is still active" );
    AnyLogWriter lw = (AnyLogWriter) cache.get( name );
    if ( lw == null )
      throw new HydraRuntimeException( "Log writer " + name + " has not been cached" );
    logName = name;
    logWriter = lw;
    return logWriter;
  }
  /**
  * Fetches the singleton log writer.
  * @throws HydraRuntimeException if log writer has not been created.
  */
  public static LogWriter getLogWriter() {
    if ( logWriter == null )
      throw new HydraRuntimeException( "Attempt to getLogWriter() before createLogWriter()" );
    return logWriter;
  }
  /**
  *
  * Fetches the current log level of the singleton log writer.
  *
  */
  public static String getLogWriterLevel() {
    return LogVersionHelper.levelToString(logWriter.getLevel());
  }
  /**
  *
  * Resets the log level of the singleton log writer.
  *
  */
  public static void setLogWriterLevel( String levelName ) {
    logWriter.setLevel(LogVersionHelper.levelNameToCode(levelName));
  }
  /**
  * Small Log test program
  */
  public static void main(String[] args) {
     Thread.currentThread().setName( "chester" );

     Log.createLogWriter( "test", "finer" );

     Log.getLogWriter().fine( "fine" );
     Log.getLogWriter().finer( "finer" );
     Log.getLogWriter().finest( "finest" );

     Log.setLogWriterLevel( "all" );
     Log.getLogWriter().fine( "fine" );
     Log.getLogWriter().finer( "finer" );
     Log.getLogWriter().finest( "finest" );
  }
}
