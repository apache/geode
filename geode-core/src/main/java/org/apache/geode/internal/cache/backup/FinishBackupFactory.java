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

import java.util.HashSet;
import java.util.Set;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;

class FinishBackupFactory {

  BackupReplyProcessor createReplyProcessor(BackupResultCollector resultCollector,
      DistributionManager dm, Set<InternalDistributedMember> recipients) {
    return new BackupReplyProcessor(resultCollector, dm, recipients);
  }

  FinishBackupRequest createRequest(InternalDistributedMember sender,
      Set<InternalDistributedMember> recipients, int processorId) {
    return new FinishBackupRequest(sender, recipients, processorId, this);
  }

  FinishBackup createFinishBackup(InternalCache cache) {
    return new FinishBackup(cache);
  }

  BackupResponse createBackupResponse(InternalDistributedMember sender,
      HashSet<PersistentID> persistentIds) {
    return new BackupResponse(sender, persistentIds);
  }

  FinishBackupStep createFinishBackupStep(DistributionManager dm, InternalDistributedMember member,
      InternalCache cache, Set<InternalDistributedMember> recipients,
      FinishBackupFactory finishBackupFactory) {
    return new FinishBackupStep(dm, member, cache, recipients, finishBackupFactory);
  }
}
