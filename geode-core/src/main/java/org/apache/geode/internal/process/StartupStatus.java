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
package org.apache.geode.internal.process;

import static org.apache.commons.lang.Validate.notNull;

import org.apache.logging.log4j.Logger;

import org.apache.geode.internal.logging.LogService;

/**
 * Extracted from LogWriterImpl and changed to static.
 */
public class StartupStatus {
  private static final Logger logger = LogService.getLogger();

  /** protected by static synchronized */
  private static StartupStatusListener listener;

  private StartupStatus() {
    // do nothing
  }

  /**
   * Writes both a message and exception to this writer. If a startup listener is registered, the
   * message will be written to the listener as well to be reported to a user.
   *
   * @since GemFire 7.0
   */
  public static synchronized void startup(final String msg, final Object... params) {
    notNull(msg, "Invalid msgId '" + msg + "' specified");
    notNull(params, "Invalid params '" + params + "' specified");

    String message = (params == null) ? msg : String.format(msg, params);

    if (listener != null) {
      listener.setStatus(message);
    }

    logger.info(message);
  }

  public static synchronized void setListener(final StartupStatusListener listener) {
    StartupStatus.listener = listener;
  }

  public static synchronized StartupStatusListener getStartupListener() {
    return StartupStatus.listener;
  }

  public static synchronized void clearListener() {
    StartupStatus.listener = null;
  }
}
