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

import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;

class BackupDataStoreResult {

  private final Map<DistributedMember, Set<PersistentID>> existingDataStores;
  private final Map<DistributedMember, Set<PersistentID>> successfulMembers;

  BackupDataStoreResult(Map<DistributedMember, Set<PersistentID>> existingDataStores,
      Map<DistributedMember, Set<PersistentID>> successfulMembers) {
    this.existingDataStores = existingDataStores;
    this.successfulMembers = successfulMembers;
  }

  Map<DistributedMember, Set<PersistentID>> getExistingDataStores() {
    return existingDataStores;
  }

  Map<DistributedMember, Set<PersistentID>> getSuccessfulMembers() {
    return successfulMembers;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "["
        + "existingDataStores=" + existingDataStores
        + "; successfulMembers=" + successfulMembers + "]";
  }
}
