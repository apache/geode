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
package org.apache.geode.internal.cache.backup;

import static org.apache.geode.internal.cache.backup.AbstractBackupWriterConfig.TIMESTAMP;
import static org.apache.geode.internal.cache.backup.AbstractBackupWriterConfig.TYPE;
import static org.apache.geode.internal.cache.backup.FileSystemBackupWriterConfig.BASELINE_DIR;
import static org.apache.geode.internal.cache.backup.FileSystemBackupWriterConfig.TARGET_DIR;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

class BackupConfigFactory {

  private String targetDirPath;
  private String baselineDirPath;

  BackupConfigFactory() {
    // nothing
  }

  BackupConfigFactory withTargetDirPath(String targetDirPath) {
    this.targetDirPath = targetDirPath;
    return this;
  }

  BackupConfigFactory withBaselineDirPath(String baselineDirPath) {
    this.baselineDirPath = baselineDirPath;
    return this;
  }

  Properties createBackupProperties() {
    Properties properties = new Properties();
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
    properties.setProperty(TIMESTAMP, format.format(new Date()));
    properties.setProperty(TYPE, "FileSystem");
    properties.setProperty(TARGET_DIR, targetDirPath);
    if (baselineDirPath != null) {
      properties.setProperty(BASELINE_DIR, baselineDirPath);
    }
    return properties;
  }
}
