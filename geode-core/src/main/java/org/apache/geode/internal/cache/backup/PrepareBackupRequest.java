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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminFailureResponse;
import org.apache.geode.internal.admin.remote.AdminResponse;
import org.apache.geode.internal.admin.remote.CliLegacyMessage;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

/**
 * A request to from an admin VM to all non admin members to start a backup. In the prepare phase of
 * the backup, the members will suspend bucket destroys to make sure buckets aren't missed during
 * the backup.
 */
public class PrepareBackupRequest extends CliLegacyMessage {
  private static final Logger logger = LogService.getLogger();

  private final transient PrepareBackupFactory prepareBackupFactory;
  private File targetDir;
  private File baselineDir;

  public PrepareBackupRequest() {
    this.prepareBackupFactory = new PrepareBackupFactory();
  }

  PrepareBackupRequest(InternalDistributedMember sender, Set<InternalDistributedMember> recipients,
      int msgId, PrepareBackupFactory prepareBackupFactory, File targetDir, File baselineDir) {
    setSender(sender);
    setRecipients(recipients);
    this.msgId = msgId;
    this.prepareBackupFactory = prepareBackupFactory;
    this.targetDir = targetDir;
    this.baselineDir = baselineDir;
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    HashSet<PersistentID> persistentIds;
    try {
      persistentIds = prepareBackupFactory
          .createPrepareBackup(dm.getDistributionManagerId(), dm.getCache(), targetDir, baselineDir)
          .run();
    } catch (IOException | InterruptedException e) {
      logger.error(LocalizedMessage.create(LocalizedStrings.CliLegacyMessage_ERROR, getClass()), e);
      return AdminFailureResponse.create(getSender(), e);
    }
    return prepareBackupFactory.createBackupResponse(getSender(), persistentIds);
  }

  @Override
  public int getDSFID() {
    return PREPARE_BACKUP_REQUEST;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    targetDir = DataSerializer.readFile(in);
    baselineDir = DataSerializer.readFile(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeFile(targetDir, out);
    DataSerializer.writeFile(baselineDir, out);
  }
}
