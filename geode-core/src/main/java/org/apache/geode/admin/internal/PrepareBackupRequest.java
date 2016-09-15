/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.internal.admin.remote.AdminFailureResponse;
import org.apache.geode.internal.admin.remote.AdminMultipleReplyProcessor;
import org.apache.geode.internal.admin.remote.AdminResponse;
import org.apache.geode.internal.admin.remote.CliLegacyMessage;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.persistence.BackupManager;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

/**
 * A request to from an admin VM to all non admin members
 * to start a backup. In the prepare phase of the backup,
 * the members will suspend bucket destroys to make sure
 * buckets aren't missed during the backup.
 * 
 *
 */
public class PrepareBackupRequest  extends CliLegacyMessage {
  private static final Logger logger = LogService.getLogger();
  
  public PrepareBackupRequest() {
    
  }
  
  public static Map<DistributedMember, Set<PersistentID>> send(DM dm, Set recipients) {
    PrepareBackupRequest request = new PrepareBackupRequest();
    request.setRecipients(recipients);

    PrepareBackupReplyProcessor replyProcessor = new PrepareBackupReplyProcessor(dm, recipients);
    request.msgId = replyProcessor.getProcessorId();
    dm.putOutgoing(request);
    try {
      replyProcessor.waitForReplies();
    } catch (ReplyException e) {
      if(!(e.getCause() instanceof CancelException)) {
        throw e;
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    AdminResponse response = request.createResponse((DistributionManager)dm);
    response.setSender(dm.getDistributionManagerId());
    replyProcessor.process(response);
    return replyProcessor.results;
  }
  
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    HashSet<PersistentID> persistentIds;
    if(cache == null) {
      persistentIds = new HashSet<PersistentID>();
    } else {
      try {
        BackupManager manager = cache.startBackup(getSender());
        persistentIds = manager.prepareBackup();
      } catch(IOException e) {
        logger.error(LocalizedMessage.create(LocalizedStrings.CliLegacyMessage_ERROR, this.getClass()), e);
        return AdminFailureResponse.create(dm, getSender(), e);        
      }
    }


    return new PrepareBackupResponse(this.getSender(), persistentIds);
  }

  public int getDSFID() {
    return PREPARE_BACKUP_REQUEST;
  }
  
  private static class PrepareBackupReplyProcessor extends AdminMultipleReplyProcessor {
    Map<DistributedMember, Set<PersistentID>> results = Collections.synchronizedMap(new HashMap<DistributedMember, Set<PersistentID>>());
    public PrepareBackupReplyProcessor(DM dm, Collection initMembers) {
      super(dm, initMembers);
    }
    
    @Override
    protected boolean stopBecauseOfExceptions() {
      return false;
    }

    @Override
    protected void process(DistributionMessage msg, boolean warn) {
      if(msg instanceof PrepareBackupResponse) {
        final HashSet<PersistentID> persistentIds = ((PrepareBackupResponse) msg).getPersistentIds();
        if(persistentIds != null && !persistentIds.isEmpty()) {
          results.put(msg.getSender(), persistentIds);
        }
      }
      super.process(msg, warn);
    }
    
    

  }
}
