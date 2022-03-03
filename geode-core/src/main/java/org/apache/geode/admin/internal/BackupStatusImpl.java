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

import org.apache.geode.admin.BackupStatus;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;

/**
 * @deprecated as of 7.0 use the <code><a href=
 *             "{@docRoot}/org/apache/geode/management/package-summary.html">management</a></code>
 *             package instead
 */
public class BackupStatusImpl implements BackupStatus {
  private static final long serialVersionUID = 3704162840296921841L;

  private final org.apache.geode.management.BackupStatus status;

  public BackupStatusImpl(Map<DistributedMember, Set<PersistentID>> backedUpDiskStores,
      Set<PersistentID> offlineDiskStores) {
    status = new org.apache.geode.management.internal.BackupStatusImpl(backedUpDiskStores,
        offlineDiskStores);
  }

  BackupStatusImpl(org.apache.geode.management.BackupStatus status) {
    this.status = status;
  }

  @Override
  public Map<DistributedMember, Set<PersistentID>> getBackedUpDiskStores() {
    return status.getBackedUpDiskStores();
  }

  @Override
  public Set<PersistentID> getOfflineDiskStores() {
    return status.getOfflineDiskStores();
  }
}
