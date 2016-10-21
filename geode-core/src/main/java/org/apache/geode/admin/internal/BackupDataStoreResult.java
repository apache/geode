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
package org.apache.geode.admin.internal;

import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;

public class BackupDataStoreResult {

  private Map<DistributedMember, Set<PersistentID>> existingDataStores;

  private Map<DistributedMember, Set<PersistentID>> successfulMembers;

  public BackupDataStoreResult(Map<DistributedMember, Set<PersistentID>> existingDataStores,
      Map<DistributedMember, Set<PersistentID>> successfulMembers) {
    this.existingDataStores = existingDataStores;
    this.successfulMembers = successfulMembers;
  }

  public Map<DistributedMember, Set<PersistentID>> getExistingDataStores() {
    return this.existingDataStores;
  }

  public Map<DistributedMember, Set<PersistentID>> getSuccessfulMembers() {
    return this.successfulMembers;
  }

  public String toString() {
    return new StringBuilder().append(getClass().getSimpleName()).append("[")
        .append("existingDataStores=").append(this.existingDataStores)
        .append("; successfulMembers=").append(this.successfulMembers).append("]").toString();
  }
}
