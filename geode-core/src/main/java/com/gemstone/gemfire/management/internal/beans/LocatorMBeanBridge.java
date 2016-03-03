/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.beans;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.management.internal.JmxManagerAdvisor.JmxManagerProfile;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.ManagementStrings;


/**
 * 
 *
 */
public class LocatorMBeanBridge {
  private static final Logger logger = LogService.getLogger();
  
  private Locator loc;
  
  private InternalDistributedSystem system;
  
  private GemFireCacheImpl cache;
  
  public LocatorMBeanBridge(Locator loc) {
    this.loc = loc;
    this.system = (InternalDistributedSystem) loc.getDistributedSystem();
    this.cache = GemFireCacheImpl.getInstance();
  }
 
  public String getBindAddress() {
    return loc.getBindAddress().getCanonicalHostName();
  }

 
  public String getHostnameForClients() {
     return loc.getHostnameForClients();
  }

 
  public String viewLog() {
    return fetchLog(loc.getLogFile(),ManagementConstants.DEFAULT_SHOW_LOG_LINES);
  }

 
  public int getPort() {
    return loc.getPort();
  }

 
  public boolean isPeerLocator() {
    return loc.isPeerLocator();
  }

 
  public boolean isServerLocator() {
    return loc.isServerLocator();
  }

  public String[] listManagers() {
    if (cache != null) {
      List<JmxManagerProfile> alreadyManaging = this.cache
          .getJmxManagerAdvisor().adviseAlreadyManaging();
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
      List<JmxManagerProfile> willingToManage = this.cache
          .getJmxManagerAdvisor().adviseWillingToManage();
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
   * 
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
      InternalDistributedSystem sys = system;
      mainTail = BeanUtilFuncs.tailSystemLog(logFile, numLines);
      if (mainTail == null) {
        mainTail = ManagementStrings.TailLogResponse_NO_LOG_FILE_WAS_SPECIFIED_IN_THE_CONFIGURATION_MESSAGES_IS_BEING_DIRECTED_TO_STDOUT
            .toLocalizedString();
      }

    } catch (IOException e) {
      logger.warn(LocalizedMessage.create(ManagementStrings.TailLogResponse_ERROR_OCCURRED_WHILE_READING_LOGFILE_LOG__0, e));
      mainTail = "";
    }

    if ( mainTail == null) {
      return LocalizedStrings.SystemMemberImpl_NO_LOG_FILE_CONFIGURED_LOG_MESSAGES_WILL_BE_DIRECTED_TO_STDOUT
          .toLocalizedString();
    } else {
      StringBuffer result = new StringBuffer();
      if (mainTail != null) {
        result.append(mainTail);
      }
      return result.toString();
    }
  }

}
