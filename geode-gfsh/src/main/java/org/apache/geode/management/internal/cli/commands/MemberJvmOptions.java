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
 *
 */

package org.apache.geode.management.internal.cli.commands;

import static org.apache.commons.lang3.JavaVersion.JAVA_11;
import static org.apache.commons.lang3.JavaVersion.JAVA_13;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtLeast;
import static org.apache.commons.lang3.SystemUtils.isJavaVersionAtMost;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MemberJvmOptions {
  static final int CMS_INITIAL_OCCUPANCY_FRACTION = 60;
  private static final List<String> JAVA_11_OPTIONS = Arrays.asList(
      "--add-exports=java.management/com.sun.jmx.remote.security=ALL-UNNAMED");

  public static List<String> getMemberJvmOptions() {
    if (isJavaVersionAtLeast(JAVA_11)) {
      return JAVA_11_OPTIONS;
    }
    return Collections.emptyList();
  }

  public static List<String> getGcJvmOptions(List<String> commandLine) {
    if (isJavaVersionAtMost(JAVA_13)) {
      List<String> cmsOptions = new ArrayList<>();
      String collectorKey = "-XX:+UseConcMarkSweepGC";
      if (!commandLine.contains(collectorKey)) {
        cmsOptions.add(collectorKey);
      }
      String occupancyFractionKey = "-XX:CMSInitiatingOccupancyFraction=";
      if (commandLine.stream().noneMatch(s -> s.contains(occupancyFractionKey))) {
        cmsOptions.add(occupancyFractionKey + CMS_INITIAL_OCCUPANCY_FRACTION);
      }
      return cmsOptions;
    } else {
      // TODO: configure ZGC?
      return Collections.emptyList();
    }
  }
}
