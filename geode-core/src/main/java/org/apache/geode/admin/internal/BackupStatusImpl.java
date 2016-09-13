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
package com.gemstone.gemfire.admin.internal;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.admin.BackupStatus;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * Holds the result of a backup operation.
 * 
 *
 */
public class BackupStatusImpl implements BackupStatus, Serializable {
  private static final long serialVersionUID = 3704162840296921840L;
  
  private Map<DistributedMember, Set<PersistentID>> backedUpDiskStores;
  private Set<PersistentID> offlineDiskStores;
  
  public BackupStatusImpl(
      Map<DistributedMember, Set<PersistentID>> backedUpDiskStores,
      Set<PersistentID> offlineDiskStores) {
    super();
    this.backedUpDiskStores = backedUpDiskStores;
    this.offlineDiskStores = offlineDiskStores;
  }

  public Map<DistributedMember, Set<PersistentID>> getBackedUpDiskStores() {
    return backedUpDiskStores;
  }

  public Set<PersistentID> getOfflineDiskStores() {
    return offlineDiskStores;
  }

  @Override
  public String toString() {
    return "BackupStatus[backedUpDiskStores=" + backedUpDiskStores + ", offlineDiskStores=" + offlineDiskStores + "]"; 
  }
  
  

}
