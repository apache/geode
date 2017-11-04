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

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.admin.remote.AdminFailureResponse;
import org.apache.geode.internal.admin.remote.AdminMultipleReplyProcessor;
import org.apache.geode.internal.admin.remote.AdminResponse;
import org.apache.geode.internal.admin.remote.CliLegacyMessage;
import org.apache.geode.internal.cache.InternalCache;
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

  private final DM dm;
  private final PrepareBackupReplyProcessor replyProcessor;

  public PrepareBackupRequest() {
    super();
    this.dm = null;
    this.replyProcessor = null;
  }

  private PrepareBackupRequest(DM dm, Set<InternalDistributedMember> recipients) {
    this(dm, recipients, new PrepareBackupReplyProcessor(dm, recipients));
  }

  PrepareBackupRequest(DM dm, Set<InternalDistributedMember> recipients,
      PrepareBackupReplyProcessor replyProcessor) {
    this.dm = dm;
    setRecipients(recipients);
    this.replyProcessor = replyProcessor;
    this.msgId = this.replyProcessor.getProcessorId();
  }

  public static Map<DistributedMember, Set<PersistentID>> send(DM dm, Set recipients) {
    PrepareBackupRequest request = new PrepareBackupRequest(dm, recipients);
    return request.send();
  }

  Map<DistributedMember, Set<PersistentID>> send() {
    dm.putOutgoing(this);

    AdminResponse response = createResponse(dm);

    try {
      replyProcessor.waitForReplies();
    } catch (ReplyException e) {
      if (!(e.getCause() instanceof CancelException)) {
        throw e;
      }
    } catch (InterruptedException e) {
      logger.warn(e.getMessage(), e);
    }

    response.setSender(dm.getDistributionManagerId());
    replyProcessor.process(response);
    return replyProcessor.getResults();
  }

  @Override
  protected AdminResponse createResponse(DM dm) {
    HashSet<PersistentID> persistentIds;
    try {
      persistentIds = prepareForBackup(dm);
    } catch (IOException e) {
      logger.error(LocalizedMessage.create(LocalizedStrings.CliLegacyMessage_ERROR, getClass()), e);
      return AdminFailureResponse.create(getSender(), e);
    }
    return new PrepareBackupResponse(getSender(), persistentIds);
  }

  HashSet<PersistentID> prepareForBackup(DM dm) throws IOException {
    InternalCache cache = dm.getCache();
    HashSet<PersistentID> persistentIds;
    if (cache == null) {
      persistentIds = new HashSet<>();
    } else {
      persistentIds = cache.startBackup(getSender()).prepareForBackup();
    }
    return persistentIds;
  }

  @Override
  public int getDSFID() {
    return PREPARE_BACKUP_REQUEST;
  }

  static class PrepareBackupReplyProcessor extends AdminMultipleReplyProcessor {

    private Map<DistributedMember, Set<PersistentID>> results =
        Collections.synchronizedMap(new HashMap<DistributedMember, Set<PersistentID>>());

    PrepareBackupReplyProcessor(DM dm, Collection initMembers) {
      super(dm, initMembers);
    }

    @Override
    protected boolean stopBecauseOfExceptions() {
      return false;
    }

    @Override
    protected void process(DistributionMessage message, boolean warn) {
      if (message instanceof PrepareBackupResponse) {
        HashSet<PersistentID> persistentIds = ((PrepareBackupResponse) message).getPersistentIds();
        if (persistentIds != null && !persistentIds.isEmpty()) {
          results.put(message.getSender(), persistentIds);
        }
      }
      super.process(message, warn);
    }

    Map<DistributedMember, Set<PersistentID>> getResults() {
      return results;
    }
  }
}
