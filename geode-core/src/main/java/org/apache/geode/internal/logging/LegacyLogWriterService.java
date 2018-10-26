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

import java.io.File;

import org.apache.logging.log4j.core.appender.AbstractAppender;

/**
 * TODO:KIRK: delete LegacyLogWriterService after replacing calls
 */
public class LegacyLogWriterService {

  public static File getChildLogFile() {
    return null;
  }

  public static void stop() {

  }

  public static void destroy() {

  }

  public static void getOrCreate(boolean isLoner, LogConfig logConfig, boolean logTheConfig) {

  }

  public static void startupComplete() {

  }

  public static void configChanged() {

  }

  public static File getLogDir() {
    return null;
  }

  public static int getMainLogId() {
    return 0;
  }

  public static boolean useChildLogging() {
    return false;
  }

  public static boolean hasLogWriterAppender() {
    return false;
  }

  public static void getOrCreateSecurity(boolean isLoner, LogConfig logConfig,
      boolean logTheConfig) {

  }

  public static void startupCompleteSecurity() {

  }

  public static void stopSecurity() {

  }

  public static void destroySecurity() {

  }

  public static void getOrCreate(boolean appendToFile, boolean isLoner, LogConfig logConfig,
      boolean startDistributedSystem) {

  }

  public static void getOrCreateSecurity(boolean appendToFile, boolean isLoner, LogConfig logConfig,
      boolean startDistributedSystem) {

  }

  public static AbstractAppender getAppender() {
    return null;
  }

  public static void configureLoggers(boolean hasLogFile, boolean hasSecurityLogFile) {

  }

  public static void setInternalDistributedSystemInternalLogWriterLogWriterLevel(int value) {

  }
}
