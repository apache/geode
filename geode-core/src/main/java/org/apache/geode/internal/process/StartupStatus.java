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

import static org.apache.commons.lang3.Validate.notNull;

import java.util.function.Consumer;

import org.apache.logging.log4j.Logger;

import org.apache.geode.logging.internal.log4j.api.LogService;

public class StartupStatus {
  private static final Logger LOGGER = LogService.getLogger();

  private final Consumer<String> logger;

  public StartupStatus() {
    this(LOGGER::info);
  }

  private StartupStatus(Consumer<String> logger) {
    this.logger = logger;
  }

  /**
   * Writes both a message and exception to this writer. If a startup listener is registered, the
   * message will be written to the listener as well to be reported to a user.
   */
  public void startup(String message, Object... params) {
    notNull(message, "Invalid message '" + message + "' specified");
    notNull(params, "Invalid params specified");

    String formattedMessage = String.format(message, params);

    StartupStatusListener listener = StartupStatusListenerRegistry.getStartupListener();
    if (listener != null) {
      listener.setStatus(formattedMessage);
    }

    logger.accept(formattedMessage);
  }
}
