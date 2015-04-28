/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.logging;

import java.io.PrintStream;

import com.gemstone.org.jgroups.util.StringId;


/**
 * Implementation of {@link com.gemstone.gemfire.LogWriter} that will write
 * security related logs to a local stream.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class SecurityLocalLogWriter extends PureLogWriter {

  /**
   * Creates a writer that logs to given <code>{@link PrintStream}</code>.
   * 
   * @param level
   *                only messages greater than or equal to this value will be
   *                logged.
   * @param logWriter
   *                is the stream that message will be printed to.
   * 
   * @throws IllegalArgumentException
   *                 if level is not in legal range
   */
  public SecurityLocalLogWriter(int level, PrintStream logWriter) {
    super(level, logWriter);
  }

  /**
   * Creates a writer that logs to given <code>{@link PrintStream}</code> and
   * having the given <code>connectionName</code>.
   * 
   * @param level
   *                only messages greater than or equal to this value will be
   *                logged.
   * @param logWriter
   *                is the stream that message will be printed to.
   * @param connectionName
   *                Name of connection associated with this log writer
   * 
   * @throws IllegalArgumentException
   *                 if level is not in legal range
   */
  public SecurityLocalLogWriter(int level, PrintStream logWriter,
      String connectionName) {
    super(level, logWriter, connectionName);
  }

  @Override
  public boolean isSecure() {
    return true;
  }
  
  @Override
  public void put(int msgLevel, StringId msgId, Object[] params, Throwable exception) {
    put(msgLevel, msgId.toLocalizedString(params), exception);
  }
  
  /**
   * Adds the {@link SecurityLogWriter#SECURITY_PREFIX} prefix to the log-level
   * to distinguish security related log-lines.
   */
  @Override
  public void put(int msgLevel, String msg, Throwable exception) {
    super.put(msgLevel, new StringBuilder(SecurityLogWriter.SECURITY_PREFIX).append(levelToString(msgLevel)).append(" ").append(msg).toString(), exception);
  }
}
