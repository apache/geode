/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.logging;

import java.util.logging.ErrorManager;
import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.apache.geode.GemFireException;
import org.apache.geode.LogWriter;
import org.apache.geode.logging.internal.log4j.LogWriterLogger;

/**
 * Implementation of the standard JDK handler that publishes a log record to a LogWriterImpl. Note
 * this handler ignores any installed handler.
 */
public class GemFireHandler extends Handler {

  /**
   * Use the log writer to use some of its formatting code.
   */
  private LogWriter logWriter;

  public GemFireHandler(LogWriter logWriter) {
    this.logWriter = logWriter;
    setFormatter(new GemFireFormatter());
  }

  @Override
  public void close() {
    // clear the reference to GFE LogWriter
    logWriter = null;
  }

  @Override
  public void flush() {
    // nothing needed
  }

  private String getMessage(final LogRecord record) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append('(').append("tid=").append(record.getThreadID())
        .append(" msgId=").append(record.getSequenceNumber()).append(") ");
    if (record.getMessage() != null) {
      stringBuilder.append(getFormatter().formatMessage(record));
    }
    return stringBuilder.toString();
  }

  @Override
  public void publish(final LogRecord record) {
    if (isLoggable(record)) {
      try {
        if (logWriter instanceof LogWriterLogger) {
          ((LogWriterLogger) logWriter).log(record.getLevel().intValue(), getMessage(record),
              record.getThrown());
        } else {
          ((LogWriterImpl) logWriter).put(record.getLevel().intValue(), getMessage(record),
              record.getThrown());
        }
      } catch (GemFireException ex) {
        reportError(null, ex, ErrorManager.WRITE_FAILURE);
      }
    }
  }
}
