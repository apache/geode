/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.logging;

import java.io.PrintStream;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.org.jgroups.util.StringId;

/**
 * A log writer for security related logs. This will prefix all messages with
 * "security-" in the level part of log-line for easy recognition and filtering
 * if required. Intended usage is in all places where security related logging
 * (authentication, authorization success and failure of clients and peers) as
 * well as for security callbacks.
 * 
 * This class extends the {@link ManagerLogWriter} to add the security prefix
 * feature mentioned above.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public final class SecurityManagerLogWriter extends ManagerLogWriter {

  public SecurityManagerLogWriter(int level, PrintStream stream) {

    super(level, stream);
  }

  public SecurityManagerLogWriter(int level, PrintStream stream,
      String connectionName) {

    super(level, stream, connectionName);
  }


  @Override
  public void setConfig(LogConfig config) {
    
    if (config instanceof DistributionConfig) {
      config = new SecurityLogConfig((DistributionConfig)config);
    }
    super.setConfig(config);
  }

  @Override
  public boolean isSecure() {
    return true;
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
