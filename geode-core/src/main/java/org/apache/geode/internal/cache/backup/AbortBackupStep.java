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

import java.util.Collections;
import java.util.Set;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;

class AbortBackupStep extends BackupStep {

  private final InternalDistributedMember member;
  private final InternalCache cache;
  private final Set<InternalDistributedMember> recipients;
  private final AbortBackupFactory abortBackupFactory;

  AbortBackupStep(DistributionManager dm, InternalDistributedMember member,
      InternalCache cache, Set<InternalDistributedMember> recipients,
      AbortBackupFactory abortBackupFactory) {
    super(dm);
    this.member = member;
    this.cache = cache;
    this.recipients = recipients;
    this.abortBackupFactory = abortBackupFactory;
  }

  /**
   * AbortBackupStep overrides addToResults in order to include members with empty persistentIds
   * (such as the sender).
   */
  @Override
  public void addToResults(InternalDistributedMember member, Set<PersistentID> persistentIds) {
    if (persistentIds != null) {
      getResults().put(member, persistentIds);
    }
  }

  @Override
  ReplyProcessor21 createReplyProcessor() {
    return abortBackupFactory.createReplyProcessor(this, getDistributionManager(), recipients);
  }

  @Override
  DistributionMessage createDistributionMessage(ReplyProcessor21 replyProcessor) {
    return abortBackupFactory.createRequest(member, recipients, replyProcessor.getProcessorId());
  }

  @Override
  void processLocally() {
    abortBackupFactory.createAbortBackup(cache).run();
    addToResults(member, Collections.emptySet());
  }
}
