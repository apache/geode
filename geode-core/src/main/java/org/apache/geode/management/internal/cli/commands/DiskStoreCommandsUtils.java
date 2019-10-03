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
package org.apache.geode.management.internal.cli.commands;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.Configuration;
import org.apache.geode.logging.internal.LogService;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.internal.cli.CliUtil;

class DiskStoreCommandsUtils {
  private static final Logger logger = LogService.getLogger();

  private static final String LOG4J_CONFIGURATION_FILE_PROPERTY = "log4j.configurationFile";

  static void configureLogging(final List<String> commandList) {
    String configFilePropertyValue = System.getProperty(LOG4J_CONFIGURATION_FILE_PROPERTY);
    if (StringUtils.isBlank(configFilePropertyValue)) {
      URL configUrl = LogService.class.getResource(Configuration.CLI_CONFIG);
      configFilePropertyValue = configUrl.toString();
    }
    commandList.add("-D" + LOG4J_CONFIGURATION_FILE_PROPERTY + "=" + configFilePropertyValue);
  }

  static String validatedDirectories(String[] diskDirs) {
    String invalidDirectories = null;
    StringBuilder builder = null;
    File diskDir;
    for (String diskDirPath : diskDirs) {
      diskDir = new File(diskDirPath);
      if (!diskDir.exists()) {
        if (builder == null) {
          builder = new StringBuilder();
        } else if (builder.length() != 0) {
          builder.append(", ");
        }
        builder.append(diskDirPath);
      }
    }
    if (builder != null) {
      invalidDirectories = builder.toString();
    }
    return invalidDirectories;
  }

  static Set<DistributedMember> getNormalMembers(final InternalCache cache) {
    return CliUtil.getAllNormalMembers(cache);
  }


  static boolean diskStoreBeanAndMemberBeanDiskStoreExists(DistributedSystemMXBean dsMXBean,
      String memberName,
      String diskStore) {
    return diskStoreBeanExists(dsMXBean, memberName, diskStore) &&
        memberBeanDiskStoreExists(dsMXBean, memberName, diskStore);
  }

  private static boolean diskStoreBeanExists(DistributedSystemMXBean dsMXBean, String memberName,
      String diskStore) {
    try {
      dsMXBean.fetchDiskStoreObjectName(memberName, diskStore);
      return true;
    } catch (Exception e) {
      if (!e.getMessage().toLowerCase().contains("not found")) {
        logger.warn("Unable to retrieve Disk Store ObjectName for member: {}, diskstore: {}  {}",
            memberName, diskStore, e.getMessage());
      }
    }
    return false;
  }

  private static boolean memberBeanDiskStoreExists(DistributedSystemMXBean dsMXBean,
      String memberName,
      String diskStore) {
    return Stream.of(dsMXBean)
        .filter(Objects::nonNull)
        .map(DistributedSystemMXBean::listMemberDiskstore)
        .filter(Objects::nonNull)
        .map(mds -> mds.get(memberName))
        .filter(Objects::nonNull)
        .flatMap(Stream::of)
        .filter(Objects::nonNull)
        .anyMatch(dsName -> dsName.equals(diskStore));
  }
}
