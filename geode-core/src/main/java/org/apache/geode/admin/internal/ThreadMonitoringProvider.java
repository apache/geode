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

package org.apache.geode.admin.internal;


import java.util.Properties;

import org.apache.logging.log4j.Logger;

import org.apache.geode.admin.ThreadMonitoring;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.logging.LogService;

public class ThreadMonitoringProvider {

  private static ThreadMonitoring threadMonitor = null;
  private static final Logger logger = LogService.getLogger();

  static Properties nonDefault = new Properties();
  static DistributionConfigImpl dcI = new DistributionConfigImpl(nonDefault);

  private ThreadMonitoringProvider() {

  }

  public static void init() {
    if (threadMonitor == null) {
      if (isEnabled()) {
        threadMonitor = new ThreadMonitoringImpl();
        logger.info("ThreadsMonitor - New Monitor object and process were created\n");
      } else {
        threadMonitor = new ThreadMonitoringImplDummy();
        logger.info("ThreadsMonitor - Monitoring is disabled and will not be run\n");
      }
    }
  }

  public static ThreadMonitoring getInstance() {
    if (threadMonitor == null)
      init();
    return threadMonitor;
  }

  private static boolean isEnabled() {
    return dcI.getThreadMonitorEnabled();
  }

  public static void deleteInstance() {
    threadMonitor = null;
  }

}
