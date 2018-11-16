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
import org.apache.geode.i18n.StringId;

/**
 * A log writer for security related logs. This will prefix all messages with "security-" in the
 * level part of log-line for easy recognition and filtering if required. Intended usage is in all
 * places where security related logging (authentication, authorization success and failure of
 * clients and peers) as well as for security callbacks.
 *
 * <p>
 * This class wraps an existing {@link InternalLogWriter} instance to add the security prefix
 * feature mentioned above.
 *
 * @since GemFire 5.5
 */
public class SecurityLogWriter extends LogWriterImpl implements InternalLogWriter {

  public static final String SECURITY_PREFIX = DistributionConfig.SECURITY_PREFIX_NAME;

  private final int logLevel;
  private final InternalLogWriter realLogWriter;

  public SecurityLogWriter(int level, InternalLogWriter logWriter) {
    logLevel = level;
    realLogWriter = logWriter;
  }

  @Override
  public int getLogWriterLevel() {
    return logLevel;
  }

  @Override
  public void setLogWriterLevel(int logWriterLevel) {
    throw new UnsupportedOperationException("Unable to change log level after creation");
  }

  @Override
  public boolean isSecure() {
    return true;
  }

  @Override
  public String getConnectionName() {
    return null;
  }

  @Override
  public void put(int messageLevel, StringId messageId, Object[] parameters, Throwable throwable) {
    put(messageLevel, messageId.toLocalizedString(parameters), throwable);
  }

  /**
   * Adds the {@link SecurityLogWriter#SECURITY_PREFIX} prefix to the log-level to distinguish
   * security related log-lines.
   */
  @Override
  public void put(int messageLevel, String message, Throwable throwable) {
    realLogWriter.put(messageLevel, new StringBuilder(SECURITY_PREFIX)
        .append(levelToString(messageLevel)).append(" ").append(message).toString(), throwable);
  }
}
