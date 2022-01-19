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
package org.apache.geode.management.internal.cli.remote;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.geode.management.internal.cli.GfshParser;
import org.apache.geode.management.internal.cli.shell.Gfsh;

/**
 *
 * @since GemFire 7.0
 */
public class CommandExecutionContext {
  // ThreadLocal variables that can be uses by commands
  private static final ThreadLocal<Map<String, String>> ENV = new ThreadLocal<>();
  private static final ThreadLocal<Boolean> FROM_SHELL = new ThreadLocal<>();
  private static final ThreadLocal<List<String>> SHELL_FILEPATH = new ThreadLocal<>();

  public static String getShellEnvProperty(String propertyName, String defaultValue) {
    String propertyValue = null;
    Map<String, String> gfshEnv = ENV.get();
    if (gfshEnv != null) {
      propertyValue = gfshEnv.get(propertyName);
    }
    return propertyValue != null ? propertyValue : defaultValue;
  }

  public static int getShellFetchSize() {
    int fetchSize = Gfsh.DEFAULT_APP_FETCH_SIZE;
    String fetchSizeStr = getShellEnvProperty(Gfsh.ENV_APP_FETCH_SIZE, null);
    if (fetchSizeStr != null) {
      fetchSize = Integer.parseInt(fetchSizeStr);
    }
    return fetchSize;
  }

  public static String getShellLineSeparator() {
    return getShellEnvProperty(Gfsh.ENV_SYS_OS_LINE_SEPARATOR, GfshParser.LINE_SEPARATOR);
  }

  public static Map<String, String> getShellEnv() {
    Map<String, String> envMap = ENV.get();
    if (envMap != null) {
      return Collections.unmodifiableMap(envMap);
    } else {
      return Collections.emptyMap();
    }
  }

  public static void setShellEnv(Map<String, String> env) {
    ENV.set(env);
  }

  public static List<String> getFilePathFromShell() {
    return SHELL_FILEPATH.get();
  }

  public static void setFilePathToShell(List<String> data) {
    SHELL_FILEPATH.set(data);
  }

  public static boolean isShellRequest() {
    return FROM_SHELL.get() != null && FROM_SHELL.get();
  }

  public static void setShellRequest() {
    FROM_SHELL.set(true);
  }

  public static void clear() {
    Map<String, String> map = ENV.get();
    if (map != null) {
      map.clear();
    }
    ENV.set(null);

    FROM_SHELL.set(false);
    SHELL_FILEPATH.set(null);
  }
}
