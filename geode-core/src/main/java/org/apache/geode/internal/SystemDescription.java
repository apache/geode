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

package org.apache.geode.internal;

import static org.apache.geode.internal.lang.SystemUtils.getOsArchitecture;
import static org.apache.geode.internal.lang.SystemUtils.getOsName;
import static org.apache.geode.internal.lang.SystemUtils.getOsVersion;

import java.net.UnknownHostException;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.internal.inet.LocalHostUtil;

/**
 * Provides information about the system this JVM is running on.
 */
public class SystemDescription {

  public static final String RUNNING_ON = "Running on";

  /**
   * Get the contents of the "Running on" banner text.
   *
   * @return "Running on" info.
   */
  public static @NotNull String getRunningOnInfo() {
    String line = getLocalHost() + ", " + Runtime.getRuntime().availableProcessors() + " cpu(s), "
        + getOsArchitecture() + ' ' + getOsName() + ' ' + getOsVersion() + ' ';
    return String.format(RUNNING_ON + ": %s", line);
  }

  private static @NotNull String getLocalHost() {
    try {
      return LocalHostUtil.getLocalHostString();
    } catch (UnknownHostException e) {
      return e.getMessage();
    }
  }

}
