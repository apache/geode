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
package com.gemstone.gemfire.admin.internal;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.internal.admin.remote.AdminFailureResponse;
import com.gemstone.gemfire.internal.admin.remote.AdminMultipleReplyProcessor;
import com.gemstone.gemfire.internal.admin.remote.AdminResponse;
import com.gemstone.gemfire.internal.admin.remote.CliLegacyMessage;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.persistence.BackupManager;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

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
