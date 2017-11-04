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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
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
 * A request send from an admin VM to all of the peers to indicate that that should complete the
 * backup operation.
 */
public class FinishBackupRequest extends CliLegacyMessage {
  private static final Logger logger = LogService.getLogger();

  private final DM dm;
  private final FinishBackupReplyProcessor replyProcessor;
  private File targetDir;
  private File baselineDir;
  private boolean abort;

  public FinishBackupRequest() {
    super();
    this.dm = null;
    this.replyProcessor = null;
  }

  private FinishBackupRequest(DM dm, Set<InternalDistributedMember> recipients, File targetDir,
      File baselineDir, boolean abort) {
    this(dm, recipients, new FinishBackupReplyProcessor(dm, recipients), targetDir, baselineDir,
        abort);
  }

  FinishBackupRequest(DM dm, Set<InternalDistributedMember> recipients,
      FinishBackupReplyProcessor replyProcessor, File targetDir, File baselineDir, boolean abort) {
    this.dm = dm;
    this.targetDir = targetDir;
    this.baselineDir = baselineDir;
    this.abort = abort;
    setRecipients(recipients);
    this.replyProcessor = replyProcessor;
    this.msgId = this.replyProcessor.getProcessorId();
  }

  public static Map<DistributedMember, Set<PersistentID>> send(DM dm, Set recipients,
      File targetDir, File baselineDir, boolean abort) {
    FinishBackupRequest request =
        new FinishBackupRequest(dm, recipients, targetDir, baselineDir, abort);
    return request.send();
  }

  Map<DistributedMember, Set<PersistentID>> send() {
    dm.putOutgoing(this);

    // invokes doBackup and releases BackupLock
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

    // adding local member to the results
    response.setSender(dm.getDistributionManagerId());
    replyProcessor.process(response);
    return replyProcessor.getResults();
  }

  @Override
  protected AdminResponse createResponse(DM dm) {
    HashSet<PersistentID> persistentIds;
    try {
      persistentIds = doBackup(dm);
    } catch (IOException e) {
      logger.error(LocalizedMessage.create(LocalizedStrings.CliLegacyMessage_ERROR, getClass()), e);
      return AdminFailureResponse.create(getSender(), e);
    }
    return new FinishBackupResponse(getSender(), persistentIds);
  }

  private HashSet<PersistentID> doBackup(DM dm) throws IOException {
    InternalCache cache = dm.getCache();
    HashSet<PersistentID> persistentIds;
    if (cache == null || cache.getBackupManager() == null) {
      persistentIds = new HashSet<>();
    } else {
      persistentIds = cache.getBackupManager().doBackup(targetDir, baselineDir, abort);
    }
    return persistentIds;
  }

  @Override
  public int getDSFID() {
    return FINISH_BACKUP_REQUEST;
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    targetDir = DataSerializer.readFile(in);
    baselineDir = DataSerializer.readFile(in);
    abort = DataSerializer.readBoolean(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeFile(targetDir, out);
    DataSerializer.writeFile(baselineDir, out);
    DataSerializer.writeBoolean(abort, out);
  }

  static class FinishBackupReplyProcessor extends AdminMultipleReplyProcessor {

    private Map<DistributedMember, Set<PersistentID>> results =
        Collections.synchronizedMap(new HashMap<DistributedMember, Set<PersistentID>>());

    FinishBackupReplyProcessor(DM dm, Collection initMembers) {
      super(dm, initMembers);
    }

    @Override
    protected boolean stopBecauseOfExceptions() {
      return false;
    }

    @Override
    protected int getAckWaitThreshold() {
      // Disable the 15 second warning if the backup is taking a long time
      return 0;
    }

    @Override
    public long getAckSevereAlertThresholdMS() {
      // Don't log severe alerts for backups either
      return Long.MAX_VALUE;
    }

    @Override
    protected void process(DistributionMessage message, boolean warn) {
      if (message instanceof FinishBackupResponse) {
        HashSet<PersistentID> persistentIds = ((FinishBackupResponse) message).getPersistentIds();
        if (persistentIds != null && !persistentIds.isEmpty()) {
          results.put(message.getSender(), persistentIds);
        }
      }
      super.process(message, warn);
    }

    @Override
    protected InternalDistributedMember[] getMembers() {
      return super.getMembers();
    }

    Map<DistributedMember, Set<PersistentID>> getResults() {
      return results;
    }
  }
}
