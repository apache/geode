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

import java.io.PrintStream;

import org.apache.geode.i18n.StringId;


/**
 * Implementation of {@link org.apache.geode.LogWriter} that will write
 * security related logs to a local stream.
 * 
 * @since GemFire 5.5
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
