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

import org.apache.geode.distributed.internal.DistributionConfig;

import java.io.PrintStream;

/**
 * A log writer for security related logs. This will prefix all messages with "security-" in the
 * level part of log-line for easy recognition and filtering if required. Intended usage is in all
 * places where security related logging (authentication, authorization success and failure of
 * clients and peers) as well as for security callbacks.
 * 
 * This class extends the {@link ManagerLogWriter} to add the security prefix feature mentioned
 * above.
 * 
 * @since GemFire 5.5
 */
public final class SecurityManagerLogWriter extends ManagerLogWriter {

  public SecurityManagerLogWriter(int level, PrintStream stream) {

    super(level, stream);
  }

  public SecurityManagerLogWriter(int level, PrintStream stream, String connectionName) {

    super(level, stream, connectionName);
  }


  @Override
  public void setConfig(LogConfig config) {

    if (config instanceof DistributionConfig) {
      config = new SecurityLogConfig((DistributionConfig) config);
    }
    super.setConfig(config);
  }

  @Override
  public boolean isSecure() {
    return true;
  }

  /**
   * Adds the {@link SecurityLogWriter#SECURITY_PREFIX} prefix to the log-level to distinguish
   * security related log-lines.
   */
  @Override
  public void put(int msgLevel, String msg, Throwable exception) {
    super.put(msgLevel, new StringBuilder(SecurityLogWriter.SECURITY_PREFIX)
        .append(levelToString(msgLevel)).append(" ").append(msg).toString(), exception);
  }
}
