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

import org.apache.geode.distributed.internal.deadlock.UnsafeThreadLocal;
import org.apache.geode.internal.offheap.AddressableMemoryManager;
import org.apache.geode.internal.stats50.VMStats50;
import org.apache.geode.unsafe.internal.com.sun.jmx.remote.security.MBeanServerAccessController;
import org.apache.geode.unsafe.internal.sun.nio.ch.DirectBuffer;

public class MemberJvmOptions {
  static final int CMS_INITIAL_OCCUPANCY_FRACTION = 60;
  /**
   * export needed by {@link DirectBuffer}
   */
  private static final String SUN_NIO_CH_EXPORT =
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED";
  /**
   * export needed by {@link MBeanServerAccessController}
   */
  private static final String COM_SUN_JMX_REMOTE_SECURITY_EXPORT =
      "--add-exports=java.management/com.sun.jmx.remote.security=ALL-UNNAMED";
  /**
   * open needed by {@link UnsafeThreadLocal}
   */
  private static final String JAVA_LANG_OPEN = "--add-opens=java.base/java.lang=ALL-UNNAMED";

  /**
   * open needed by {@link VMStats50}
   */
  private static final String SUN_MANAGEMENT_OPEN =
      "--add-opens=java.management/sun.management=ALL-UNNAMED";
  /**
   * open needed by {@link AddressableMemoryManager}
   */
  private static final String JAVA_NIO_OPEN = "--add-opens=java.base/java.nio=ALL-UNNAMED";
  /**
   * open needed by {@link VMStats50}
   */
  private static final String COM_SUN_MANAGEMENT_INTERNAL_OPEN =
      "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED";

  static final List<String> JAVA_11_OPTIONS = Arrays.asList(
      COM_SUN_JMX_REMOTE_SECURITY_EXPORT,
      SUN_NIO_CH_EXPORT,
      COM_SUN_MANAGEMENT_INTERNAL_OPEN,
      JAVA_LANG_OPEN,
      JAVA_NIO_OPEN,
      SUN_MANAGEMENT_OPEN);

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
