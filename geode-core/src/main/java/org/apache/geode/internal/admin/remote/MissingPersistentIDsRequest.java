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
package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.geode.CancelException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.persistence.PersistentID;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.cache.persistence.PersistentMemberManager;
import org.apache.geode.internal.cache.persistence.PersistentMemberPattern;

/**
 * A request to all members for any persistent members that they are waiting for. TODO prpersist -
 * This extends AdminRequest, but it doesn't work with most of the admin paradigm, which is a
 * request response to a single member. Maybe we need to a new base class.
 */
public class MissingPersistentIDsRequest extends CliLegacyMessage {

  public static Set<PersistentID> send(DM dm) {
    Set recipients = dm.getOtherDistributionManagerIds();

    MissingPersistentIDsRequest request = new MissingPersistentIDsRequest();

    request.setRecipients(recipients);

    MissingPersistentIDProcessor replyProcessor = new MissingPersistentIDProcessor(dm, recipients);
    request.msgId = replyProcessor.getProcessorId();
    dm.putOutgoing(request);
    try {
      replyProcessor.waitForReplies();
    } catch (ReplyException e) {
      if (!(e.getCause() instanceof CancelException)) {
        throw e;
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Set<PersistentID> results = replyProcessor.missing;
    Set<PersistentID> existing = replyProcessor.existing;


    MissingPersistentIDsResponse localResponse =
        (MissingPersistentIDsResponse) request.createResponse((DistributionManager) dm);
    results.addAll(localResponse.getMissingIds());
    existing.addAll(localResponse.getLocalIds());

    results.removeAll(existing);
    return results;
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    Set<PersistentID> missingIds = new HashSet<PersistentID>();
    Set<PersistentID> localPatterns = new HashSet<PersistentID>();
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null && !cache.isClosed()) {
      PersistentMemberManager mm = cache.getPersistentMemberManager();
      Map<String, Set<PersistentMemberID>> waitingRegions = mm.getWaitingRegions();
      for (Map.Entry<String, Set<PersistentMemberID>> entry : waitingRegions.entrySet()) {
        for (PersistentMemberID id : entry.getValue()) {
          missingIds.add(new PersistentMemberPattern(id));
        }
      }
      Set<PersistentMemberID> localIds = mm.getPersistentIDs();
      for (PersistentMemberID id : localIds) {
        localPatterns.add(new PersistentMemberPattern(id));
      }
    }

    return new MissingPersistentIDsResponse(missingIds, localPatterns, this.getSender());
  }



  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    // TODO Auto-generated method stub
    return super.clone();
  }

  public int getDSFID() {
    return MISSING_PERSISTENT_IDS_REQUEST;
  }

  private static class MissingPersistentIDProcessor extends AdminMultipleReplyProcessor {
    Set<PersistentID> missing = Collections.synchronizedSet(new TreeSet<PersistentID>());
    Set<PersistentID> existing = Collections.synchronizedSet(new TreeSet<PersistentID>());

    /**
     * @param dm
     * @param recipients
     */
    public MissingPersistentIDProcessor(DM dm, Set recipients) {
      super(dm, recipients);
    }

    @Override
    protected void process(DistributionMessage msg, boolean warn) {
      if (msg instanceof MissingPersistentIDsResponse) {
        missing.addAll(((MissingPersistentIDsResponse) msg).getMissingIds());
        existing.addAll(((MissingPersistentIDsResponse) msg).getLocalIds());
      }
      super.process(msg, warn);
    }
  }
}
