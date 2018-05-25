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
package org.apache.geode.management.internal.cli.util;


import java.io.File;

import org.apache.geode.internal.cache.DiskStoreImpl;
import org.apache.geode.management.internal.cli.i18n.CliStrings;

/**
 * @since GemFire 7.0
 */


public class DiskStoreValidater {
  public static void main(String[] args) {
    try {
      if (args.length < 2 || args.length > 2) {
        throw new IllegalArgumentException("Requires only 2  arguments : <DiskStore> <Dirs>");
      }
      validate(args[0], args[1]);
    } catch (Exception ex) {
      ex.printStackTrace();
      System.exit(1);
    }
  }

  static void validate(String diskStoreName, String diskDirs) throws Exception {
    File[] dirs = null;
    String[] dirList = null;
    dirList = diskDirs.split(";");
    if (dirList != null && dirList.length > 0) {
      dirs = new File[dirList.length];
      for (int i = 0; i < dirList.length; ++i) {
        dirs[i] = new File(dirList[i]);
      }
    } else {
      System.out.println(CliStrings.VALIDATE_DISK_STORE__MSG__NO_DIRS);
    }
    DiskStoreImpl.validate(diskStoreName, dirs);
  }
}
