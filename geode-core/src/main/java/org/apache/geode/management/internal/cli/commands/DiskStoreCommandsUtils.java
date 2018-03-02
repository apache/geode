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

import org.apache.geode.internal.logging.LogService;

class DiskStoreCommandsUtils {
  static void configureLogging(final List<String> commandList) {
    URL configUrl = LogService.class.getResource(LogService.CLI_CONFIG);
    String configFilePropertyValue = configUrl.toString();
    commandList.add("-Dlog4j.configurationFile=" + configFilePropertyValue);
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
}
