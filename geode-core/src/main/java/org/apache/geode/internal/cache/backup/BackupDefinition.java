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

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.DiskStore;
import org.apache.geode.internal.lang.JavaWorkarounds;

class BackupDefinition {

  private final Map<DiskStore, Set<Path>> oplogFilesByDiskStore = new HashMap<>();
  private final Set<Path> configFiles = new HashSet<>();
  private final Map<Path, Path> userFiles = new HashMap<>();
  private final Map<Path, Path> deployedJars = new HashMap<>();
  private final Map<DiskStore, Path> diskInitFiles = new HashMap<>();

  private RestoreScript restoreScript;

  void addConfigFileToBackup(Path configFile) {
    configFiles.add(configFile);
  }

  void addUserFilesToBackup(Path userFile, Path source) {
    userFiles.put(userFile, source);
  }

  void addDeployedJarToBackup(Path deployedJar, Path source) {
    deployedJars.put(deployedJar, source);
  }

  void addDiskInitFile(DiskStore diskStore, Path diskInitFile) {
    diskInitFiles.put(diskStore, diskInitFile);
  }

  void setRestoreScript(RestoreScript restoreScript) {
    this.restoreScript = restoreScript;
  }

  Map<DiskStore, Collection<Path>> getOplogFilesByDiskStore() {
    return Collections.unmodifiableMap(oplogFilesByDiskStore);
  }

  Set<Path> getConfigFiles() {
    return Collections.unmodifiableSet(configFiles);
  }

  Map<Path, Path> getUserFiles() {
    return Collections.unmodifiableMap(userFiles);
  }

  Map<Path, Path> getDeployedJars() {
    return Collections.unmodifiableMap(deployedJars);
  }

  Map<DiskStore, Path> getDiskInitFiles() {
    return Collections.unmodifiableMap(diskInitFiles);
  }

  RestoreScript getRestoreScript() {
    return restoreScript;
  }

  void addOplogFileToBackup(DiskStore diskStore, Path fileLocation) {
    Set<Path> files =
        JavaWorkarounds.computeIfAbsent(oplogFilesByDiskStore, diskStore, k -> new HashSet<>());
    files.add(fileLocation);
  }
}
