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
package org.apache.persistence.logging;

import java.io.OutputStream;
import java.io.PrintWriter;

/**
 * A <code>StreamHandler</code> exports log records to an <code>OutputStream</code>.
 */
public class StreamHandler extends Handler {

  /** The destination PrintWriter */
  private final PrintWriter pw;

  /**
   * Creates a new <code>StreamHandler</code> that exports log records to an
   * <code>OutputStream</code> in a given format.
   */
  public StreamHandler(OutputStream stream, Formatter formatter) {
    super();
    pw = new PrintWriter(stream, true);
    setFormatter(formatter);
  }

  @Override
  public void close() {
    pw.close();
  }

  @Override
  public void flush() {
    pw.flush();
  }

  @Override
  public boolean isLoggable(LogRecord record) {
    if (pw == null) {
      return (false);
    } else {
      return (super.isLoggable(record));
    }
  }

  @Override
  public void publish(LogRecord record) {
    Formatter formatter = getFormatter();
    pw.print(formatter.format(record));
  }

}
