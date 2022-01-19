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
package org.apache.geode.management.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.management.DiskBackupStatus;

public class DiskBackupStatusImpl implements DiskBackupStatus {

  /**
   * Map of DistributedMember, Set<PersistentID>
   */
  private Map<String, String[]> backedUpDiskStores;

  /**
   * List of offline disk stores
   */
  private String[] offlineDiskStores;

  @Override
  public Map<String, String[]> getBackedUpDiskStores() {
    return backedUpDiskStores;
  }

  @Override
  public String[] getOfflineDiskStores() {
    return offlineDiskStores;
  }

  /**
   * Sets the map of member names/IDs and the {@link PersistentID} of the disk stores that were
   * backed up.
   */
  public void setBackedUpDiskStores(Map<String, String[]> backedUpDiskStores) {
    this.backedUpDiskStores = backedUpDiskStores;
  }

  /**
   * Sets the list of directories for the disk stores that were off-line at the time the backup
   * occurred.
   */
  public void setOfflineDiskStores(String[] offLineDiskStores) {
    offlineDiskStores = offLineDiskStores;
  }

  /**
   * Sets the map of member names/IDs and the {@link PersistentID} of the disk stores that were
   * backed up.
   */
  public void generateBackedUpDiskStores(
      Map<DistributedMember, Set<PersistentID>> backedUpDiskStores) {
    Map<String, String[]> diskStores = new HashMap<>();
    backedUpDiskStores.entrySet().forEach(entry -> {
      DistributedMember member = entry.getKey();
      Set<PersistentID> ids = entry.getValue();
      String[] setOfDiskStr = new String[ids.size()];
      entry.getValue().stream().map(PersistentID::getDirectory).collect(Collectors.toList())
          .toArray(setOfDiskStr);
      diskStores.put(member.getId(), setOfDiskStr);
    });
    setBackedUpDiskStores(diskStores);
  }

  /**
   * Sets the list of directories for the disk stores that were off-line at the time the backup
   * occurred.
   */
  public void generateOfflineDiskStores(Set<PersistentID> offLineDiskStores) {
    String[] diskStores = new String[offLineDiskStores.size()];
    offLineDiskStores.stream().map(PersistentID::getDirectory).collect(Collectors.toList())
        .toArray(diskStores);
    setOfflineDiskStores(diskStores);
  }
}
