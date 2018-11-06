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
package org.apache.geode.management.internal.beans;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.Locator;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.management.internal.JmxManagerAdvisor.JmxManagerProfile;
import org.apache.geode.management.internal.ManagementConstants;

public class LocatorMBeanBridge {
  private static final Logger logger = LogService.getLogger();

  private Locator loc;

  private InternalCache cache;

  public LocatorMBeanBridge(Locator loc) {
    this.loc = loc;
    this.cache = GemFireCacheImpl.getInstance();
  }

  public String getBindAddress() {
    InetAddress bindAddress = loc.getBindAddress();
    return bindAddress != null ? bindAddress.getCanonicalHostName() : null;
  }

  public String getHostnameForClients() {
    return loc.getHostnameForClients();
  }

  public String viewLog() {
    return fetchLog(loc.getLogFile(), ManagementConstants.DEFAULT_SHOW_LOG_LINES);
  }

  public int getPort() {
    return loc.getPort();
  }

  public boolean isPeerLocator() {
    return true;
  }

  public boolean isServerLocator() {
    return true;
  }

  public String[] listManagers() {
    if (cache != null) {
      List<JmxManagerProfile> alreadyManaging =
          this.cache.getJmxManagerAdvisor().adviseAlreadyManaging();
      if (!alreadyManaging.isEmpty()) {
        String[] managers = new String[alreadyManaging.size()];
        int j = 0;
        for (JmxManagerProfile profile : alreadyManaging) {
          managers[j] = profile.getDistributedMember().getId();
          j++;
        }
        return managers;
      }
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  public String[] listPotentialManagers() {
    if (cache != null) {
      List<JmxManagerProfile> willingToManage =
          this.cache.getJmxManagerAdvisor().adviseWillingToManage();
      if (!willingToManage.isEmpty()) {
        String[] managers = new String[willingToManage.size()];
        int j = 0;
        for (JmxManagerProfile profile : willingToManage) {
          managers[j] = profile.getDistributedMember().getId();
          j++;
        }
        return managers;
      }
    }
    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * @return log of the locator.
   */
  private String fetchLog(File logFile, int numLines) {
    if (numLines > ManagementConstants.MAX_SHOW_LOG_LINES) {
      numLines = ManagementConstants.MAX_SHOW_LOG_LINES;
    }
    if (numLines == 0 || numLines < 0) {
      numLines = ManagementConstants.DEFAULT_SHOW_LOG_LINES;
    }
    String mainTail = null;
    try {
      mainTail = BeanUtilFuncs.tailSystemLog(logFile, numLines);
      if (mainTail == null) {
        mainTail =
            "No log file was specified in the configuration, messages is being directed to stdout.";
      }

    } catch (IOException e) {
      logger.warn("Error occurred while reading log file: ", e);
      mainTail = "";
    }

    if (mainTail == null) {
      return "No log file configured, log messages will be directed to stdout.";
    } else {
      StringBuffer result = new StringBuffer();
      if (mainTail != null) {
        result.append(mainTail);
      }
      return result.toString();
    }
  }

}
