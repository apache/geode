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
package org.apache.geode.logging.internal.log4j.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.StackLocator;

import org.apache.geode.logging.internal.log4j.api.message.GemFireParameterizedMessageFactory;

/**
 * Provides Log4J2 Loggers with customized optimizations for Geode:
 */
public class LogService {

  private LogService() {
    // do not instantiate
  }

  /**
   * Returns a Logger with the name of the calling class.
   *
   * @return The Logger for the calling class.
   */
  public static Logger getLogger() {
    String name = StackLocator.getInstance().getCallerClass(2).getName();
    return getLogger(name);
  }

  public static Logger getLogger(String name) {
    return new FastLogger(LogManager.getLogger(name, GemFireParameterizedMessageFactory.INSTANCE));
  }

  public static Logger getRootLogger() {
    return LogManager.getRootLogger();
  }
}
