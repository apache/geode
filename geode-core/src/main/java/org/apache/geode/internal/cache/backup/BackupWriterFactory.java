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

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

enum BackupWriterFactory {

  FILE_SYSTEM("FileSystem") {
    @Override
    BackupWriter createWriter(Properties properties, String memberId) {
      // Remove chars that are illegal in Windows paths
      memberId = memberId.replaceAll("[:()]", "-");
      FileSystemBackupWriterConfig config = new FileSystemBackupWriterConfig(properties);
      Path targetDir = Paths.get(config.getTargetDirectory())
          .resolve(properties.getProperty(TIMESTAMP)).resolve(memberId);
      String baselineDir = config.getBaselineDirectory();
      FileSystemIncrementalBackupLocation incrementalBackupLocation = null;
      if (baselineDir != null) {
        File baseline = new File(baselineDir).getAbsoluteFile();
        incrementalBackupLocation = new FileSystemIncrementalBackupLocation(baseline, memberId);
      }
      return new FileSystemBackupWriter(targetDir, incrementalBackupLocation);
    }
  };

  private final String type;

  BackupWriterFactory(String type) {
    this.type = type;
  }

  String getType() {
    return type;
  }

  static BackupWriterFactory getFactoryForType(String type) {
    for (BackupWriterFactory factory : BackupWriterFactory.values()) {
      if (factory.type.equals(type)) {
        return factory;
      }
    }
    throw new IllegalArgumentException("No factory exists for type '" + type + "'");
  }

  abstract BackupWriter createWriter(Properties properties, String memberId);
}
