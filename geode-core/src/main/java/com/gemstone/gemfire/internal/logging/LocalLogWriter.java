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
package com.gemstone.gemfire.internal.logging;

import java.io.*;

/**
  * Implementation of {@link com.gemstone.gemfire.i18n.LogWriterI18n} that will write
  * to a local stream.
  * <P>
  * Note this class is no longer needed. It can be replaced by PureLogWriter.
  */
public class LocalLogWriter extends PureLogWriter {
    // Constructors
    /**
     * Creates a writer that logs to <code>System.out</code>.
     * @param level only messages greater than or equal to this value will be logged.
     * @throws IllegalArgumentException if level is not in legal range
     */
    public LocalLogWriter(int level) {
      super(level);
    }

    /**
     * Creates a writer that logs to <code>logWriter</code>.
     * @param level only messages greater than or equal to this value will be logged.
     * @param logWriter is the stream that message will be printed to.
     * @throws IllegalArgumentException if level is not in legal range
     */
    public LocalLogWriter(int level, PrintStream logWriter) {
      super(level, logWriter);
    }

    /**
     * Creates a writer that logs to <code>logWriter</code>.
     *
     * @param level
     *        only messages greater than or equal to this value will
     *        be logged.
     * @param logWriter
     *        is the stream that message will be printed to.
     * @param connectionName
     *        Name of connection associated with this log writer
     *
     * @throws IllegalArgumentException if level is not in legal range
     */
    public LocalLogWriter(int level, PrintStream logWriter,
                          String connectionName) {
      super(level, logWriter, connectionName);
    }

    /**
     * Creates a writer that logs to <code>logWriter</code>.
     * @param level only messages greater than or equal to this value will be logged.
     * @param logWriter is the stream that message will be printed to.
     * @throws IllegalArgumentException if level is not in legal range
     */
    public LocalLogWriter(int level, PrintWriter logWriter) {
	this(level, logWriter, null);
    }

    /**
     * Creates a writer that logs to <code>logWriter</code>.
     *
     * @param level
     *        only messages greater than or equal to this value will
     *        be logged.
     * @param logWriter
     *        is the stream that message will be printed to.
     * @param connectionName
     *        Name of connection associated with this log writer
     *
     * @throws IllegalArgumentException if level is not in legal range
     *
     * @since GemFire 3.5
     */
    public LocalLogWriter(int level, PrintWriter logWriter,
                          String connectionName) {
	super(level, logWriter, connectionName);
    }
}
