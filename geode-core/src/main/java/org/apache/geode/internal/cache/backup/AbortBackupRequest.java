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

import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminResponse;
import org.apache.geode.internal.admin.remote.CliLegacyMessage;

/**
 * A request send from an admin VM to all of the peers to indicate that that should complete the
 * backup operation.
 */
public class AbortBackupRequest extends CliLegacyMessage {
  private final transient AbortBackupFactory abortBackupFactory;

  public AbortBackupRequest() {
    abortBackupFactory = new AbortBackupFactory();
  }

  AbortBackupRequest(InternalDistributedMember sender, Set<InternalDistributedMember> recipients,
      int processorId, AbortBackupFactory abortBackupFactory) {
    setSender(sender);
    setRecipients(recipients);
    msgId = processorId;
    this.abortBackupFactory = abortBackupFactory;
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    abortBackupFactory.createAbortBackup(dm.getCache()).run();
    return abortBackupFactory.createBackupResponse(getSender(), new HashSet<>());
  }

  @Override
  public int getDSFID() {
    return ABORT_BACKUP_REQUEST;
  }
}
