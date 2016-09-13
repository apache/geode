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

import java.util.logging.ErrorManager;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.apache.geode.GemFireException;
import org.apache.geode.LogWriter;
import org.apache.geode.internal.logging.log4j.LogWriterLogger;

/**
 * Implementation of the standard JDK handler that publishes a log record
 * to a LogWriterImpl.
 * Note this handler ignores any installed handler.
 */
public final class GemFireHandler extends Handler {

  /**
   * Use the log writer to use some of its formatting code.
   */
  private LogWriter logWriter;

  public GemFireHandler(LogWriter logWriter) {
    this.logWriter = logWriter;
    this.setFormatter(new GemFireFormatter(logWriter));
  }

  @Override
  public void close() {
    // clear the reference to GFE LogWriter
    this.logWriter = null;
  }

  @Override
  public void flush() {
    // nothing needed
  }

  private String getMessage(LogRecord record) {
    final StringBuilder b = new StringBuilder();
    b .append('(')
      .append("tid=" + record.getThreadID())
      .append(" msgId=" + record.getSequenceNumber())
      .append(") ");
    if (record.getMessage() != null) {
      b.append(getFormatter().formatMessage(record));
    }
    return b.toString();
  }

  @Override
  public void publish(LogRecord record) {
    if (isLoggable(record)) {
      try {
        if (this.logWriter instanceof LogWriterLogger) {
          ((LogWriterLogger) this.logWriter).log(record.getLevel().intValue(), getMessage(record), record.getThrown());
        } else {
          ((LogWriterImpl) this.logWriter).put(record.getLevel().intValue(), getMessage(record), record.getThrown());
        }
      } catch (GemFireException ex) {
        reportError(null, ex, ErrorManager.WRITE_FAILURE);
      }
    }
  }
}
