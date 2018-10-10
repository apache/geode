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
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminFailureResponse;
import org.apache.geode.internal.admin.remote.AdminResponse;
import org.apache.geode.internal.admin.remote.CliLegacyMessage;
import org.apache.geode.internal.logging.LogService;

/**
 * A request send from an admin VM to all of the peers to indicate that that should complete the
 * backup operation.
 */
public class FinishBackupRequest extends CliLegacyMessage {
  private static final Logger logger = LogService.getLogger();

  private final transient FinishBackupFactory finishBackupFactory;

  public FinishBackupRequest() {
    finishBackupFactory = new FinishBackupFactory();
  }

  FinishBackupRequest(InternalDistributedMember sender, Set<InternalDistributedMember> recipients,
      int processorId, FinishBackupFactory finishBackupFactory) {
    setSender(sender);
    setRecipients(recipients);
    msgId = processorId;
    this.finishBackupFactory = finishBackupFactory;
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    HashSet<PersistentID> persistentIds;
    try {
      persistentIds = finishBackupFactory.createFinishBackup(dm.getCache()).run();
    } catch (IOException e) {
      logger.error(String.format("Error processing request %s.", getClass()), e);
      return AdminFailureResponse.create(getSender(), e);
    }
    return finishBackupFactory.createBackupResponse(getSender(), persistentIds);
  }

  @Override
  public int getDSFID() {
    return FINISH_BACKUP_REQUEST;
  }
}
