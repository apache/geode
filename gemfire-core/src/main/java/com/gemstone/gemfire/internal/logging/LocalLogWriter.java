/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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
     * @since 3.5
     */
    public LocalLogWriter(int level, PrintWriter logWriter,
                          String connectionName) {
	super(level, logWriter, connectionName);
    }
}
