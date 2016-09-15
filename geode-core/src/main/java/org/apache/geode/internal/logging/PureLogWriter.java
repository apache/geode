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
package org.apache.geode.internal.logging;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.i18n.StringId;

/**
  * Implementation of {@link org.apache.geode.i18n.LogWriterI18n}
  * that will write to a local stream and only use pure java features.
  */
public class PureLogWriter extends LogWriterImpl {
  
  /** The "name" of the connection associated with this log writer */
  private final String connectionName;
  
  /** If the log stream has been closed */
  private volatile boolean closed;

    // Constructors
    /**
     * Creates a writer that logs to <code>System.out</code>.
     * @param level only messages greater than or equal to this value will be logged.
     * @throws IllegalArgumentException if level is not in legal range
     */
    public PureLogWriter(int level) {
	this(level, System.out);
    }
    
    /**
     * Creates a writer that logs to <code>logWriter</code>.
     * @param level only messages greater than or equal to this value will be logged.
     * @param logWriter is the stream that message will be printed to.
     * @throws IllegalArgumentException if level is not in legal range
     */
    public PureLogWriter(int level, PrintStream logWriter) {
	this(level, new PrintWriter(logWriter, true), null);
    }
    
    /**
     * Creates a writer that logs to <code>logWriter</code>.
     * @param level only messages greater than or equal to this value will be logged.
     * @param logWriter is the stream that message will be printed to.
     * @throws IllegalArgumentException if level is not in legal range
     */
    public PureLogWriter(int level, PrintStream logWriter, 
                         String connectionName) {
	this(level, new PrintWriter(logWriter, true), connectionName);
    }

    /**
     * Creates a writer that logs to <code>logWriter</code>.
     * @param level only messages greater than or equal to this value will be logged.
     * @param logWriter is the stream that message will be printed to.
     * @param connectionName
     *        The name of the connection associated with this log writer
     * @throws IllegalArgumentException if level is not in legal range
     */
    public PureLogWriter(int level, PrintWriter logWriter,
                         String connectionName) {
	super();
	this.setLevel(level);
	this.logWriter = logWriter;
        this.connectionName = connectionName;
    }

    // Special Instance Methods on this class only
    /**
     * Gets the writer's level.
     */
    @Override
    public int getLogWriterLevel() {
	return this.level;
    }
    /**
     * Sets the writer's level.
     * @throws IllegalArgumentException if level is not in legal range
     */
    public void setLevel(int newLevel) {
//	if (newLevel < ALL_LEVEL || newLevel > NONE_LEVEL) {
//	    throw new IllegalArgumentException(LocalizedStrings.PureLogWriter_WRITER_LEVEL_0_WAS_NOT_IN_THE_RANGE_1_2.toLocalizedString(new Object[] {Integer.valueOf(newLevel), Integer.valueOf(ALL_LEVEL), Integer.valueOf(NONE_LEVEL)}));
//	}
	this.level = newLevel;
    }

    public void setLogWriterLevel(int newLevel) {
      setLevel(newLevel);
    }
    
    // internal implementation methods
    protected String getThreadName() {
      return Thread.currentThread().getName();
    }
    protected long getThreadId() {
      // fix for bug 37861
      return Thread.currentThread().getId();
    }

    /**
     * Logs a message and an exception to the specified log destination.
     * 
     * @param msgLevel
     *                a string representation of the level
     * @param msg
     *                the actual message to log
     * @param ex
     *                the actual Exception to log
     */
    @Override
    public void put(int msgLevel, String msg, Throwable ex) {
      String exceptionText = null;
      if (ex != null) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        ex.printStackTrace(pw);
        pw.close();
        try {
          sw.close();
        }
        catch (IOException ignore) {
        }
        exceptionText = sw.toString();
      }
      put(msgLevel, new Date(), this.connectionName, getThreadName(),
          getThreadId(), msg, exceptionText);
    }

    /**
    * Logs a message and an exception to the specified log destination.
    * @param msgLevel a string representation of the level
    * @param msgId the actual message to log
    * @param ex the actual Exception to log
    */
    @Override
    public void put(int msgLevel, StringId msgId, Object[] params, Throwable ex) {
      String msg = msgId.toLocalizedString(params);
      put(msgLevel, msg, ex);
    } 

    protected String formatLogLine(int msgLevel, Date msgDate, @SuppressWarnings("hiding") String connectionName,
      String threadName, long tid, String msg, String exceptionText) {
        java.io.StringWriter sw = new java.io.StringWriter();
	PrintWriter pw = new PrintWriter(sw);

	printHeader(pw, msgLevel, msgDate, connectionName, threadName, tid);

	if (msg != null) {
	    try {
		formatText(pw, msg, 40);
	    } catch (RuntimeException e) {
		pw.println(msg);
		pw.println(LocalizedStrings.PureLogWriter_IGNORING_EXCEPTION.toLocalizedString());
		e.printStackTrace(pw);
	    }
	} else {
	    pw.println();
	}
	if (exceptionText != null) {
	    pw.print(exceptionText);
	}
	pw.close();
	try {
	    sw.close();
	} catch (java.io.IOException ignore) {}

        return sw.toString();
    }

    protected void printHeader(PrintWriter pw, int msgLevel, Date msgDate,
        String connectionName, String threadName, long tid) {
      pw.println();
      pw.print('[');
      pw.print(levelToString(msgLevel));
      pw.print(' ');
      pw.print(this.formatDate(msgDate));
      if (connectionName != null) {
        pw.print(' ');
        pw.print(connectionName);
      }
      if (threadName != null) {
        pw.print(" <");
        pw.print(threadName);
        pw.print(">");
      }
      pw.print(" tid=0x");
      pw.print(Long.toHexString(tid));
      pw.print("] ");
    }

    public String put(int msgLevel, Date msgDate, String connectionName,
        String threadName, long tid, String msg, String exceptionText) {
        String result = formatLogLine(msgLevel, msgDate, connectionName
                                    , threadName, tid, msg, exceptionText);
        writeFormattedMessage(result);
        return result;
    }

    public void writeFormattedMessage(String s) {
      synchronized (this) {
        this.bytesLogged += s.length();
        this.logWriter.print(s);
        this.logWriter.flush();
      }
    }

    /**
     * Returns the number of bytes written to the current log file.
     */
    public long getBytesLogged() {
      return this.bytesLogged;
    }
    /**
     * Sets the target that this logger will sends its output to.
     * @return the previous target.
     */
    public PrintWriter setTarget(PrintWriter logWriter) {
      return setTarget(logWriter, 0L);
    }
    
    public PrintWriter setTarget(PrintWriter logWriter, long targetLength) {
      synchronized(this) {
        PrintWriter result = this.logWriter;
        this.bytesLogged = targetLength;
        this.logWriter = logWriter;
        return result;
      }
    }
    
    public final void close() {
      this.closed = true;
      try { 
        if(this.logWriter!=null) {
         this.logWriter.close(); 
        }
      } catch(Exception e) {
       // ignore , we are closing.
       }
    }
    
    public final boolean isClosed() {
      return this.closed;
    }

  /**
   * Returns the name of the connection on whose behalf this log
   * writer logs.
   */
  public String getConnectionName() {
    return this.connectionName;
  }

  /** Get the underlying PrintWriter. */
  public PrintWriter getPrintWriter() {
    return this.logWriter;
  }

    // instance variables
    protected volatile int level;
    private long bytesLogged = 0;
    private PrintWriter logWriter;
}
