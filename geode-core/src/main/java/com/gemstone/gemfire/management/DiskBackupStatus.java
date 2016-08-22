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
package com.gemstone.gemfire.management;

import java.util.Map;

import com.gemstone.gemfire.cache.persistence.PersistentID;

/**
 * Composite data type used to distribute the status of disk backup
 * operations.
 * 
 * @since GemFire 7.0
 */
public class DiskBackupStatus {

  /**
   * Map of DistributedMember, Set<PersistentID>
   */
  private Map<String, String[]> backedUpDiskStores;
  
  /**
   * List of offline disk stores
   */
  private String[] offlineDiskStores;

  /**
   * Returns a map of member names/IDs and the {@link PersistentID} of the disk
   * stores that were backed up.
   */
  public Map<String, String[]> getBackedUpDiskStores() {
    return backedUpDiskStores;
  }

  /**
   * Returns a list of directories for the disk stores that were off-line at
   * the time the backup occurred.
   */
  public String[] getOfflineDiskStores() {
    return offlineDiskStores;
  }

  /**
   * Sets the map of member names/IDs and the {@link PersistentID} of the disk
   * stores that were backed up.
   */
  public void setBackedUpDiskStores(Map<String, String[]> backedUpDiskStores) {
    this.backedUpDiskStores = backedUpDiskStores;
  }

  /**
   * Sets the list of directories for the disk stores that were off-line at the
   * time the backup occurred.
   */
  public void setOfflineDiskStores(String[] offlineDiskStores) {
    this.offlineDiskStores = offlineDiskStores;
  }
}
