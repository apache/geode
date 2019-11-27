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

import java.io.IOException;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.distributed.internal.ClusterMessage;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;

class FinishBackupStep extends BackupStep {
  private static final Logger logger = LogService.getLogger();

  private final InternalDistributedMember member;
  private final InternalCache cache;
  private final Set<InternalDistributedMember> recipients;
  private final FinishBackupFactory finishBackupFactory;

  FinishBackupStep(DistributionManager dm, InternalDistributedMember member,
      InternalCache cache, Set<InternalDistributedMember> recipients,
      FinishBackupFactory finishBackupFactory) {
    super(dm);
    this.member = member;
    this.cache = cache;
    this.recipients = recipients;
    this.finishBackupFactory = finishBackupFactory;
  }

  @Override
  ReplyProcessor21 createReplyProcessor() {
    return finishBackupFactory.createReplyProcessor(this, getDistributionManager(), recipients);
  }

  @Override
  ClusterMessage createDistributionMessage(ReplyProcessor21 replyProcessor) {
    return finishBackupFactory.createRequest(member, recipients, replyProcessor.getProcessorId());
  }

  @Override
  void processLocally() {
    try {
      addToResults(member, finishBackupFactory.createFinishBackup(cache).run());
    } catch (IOException e) {
      logger.fatal("Failed to FinishBackup in " + member, e);
    }
  }
}
