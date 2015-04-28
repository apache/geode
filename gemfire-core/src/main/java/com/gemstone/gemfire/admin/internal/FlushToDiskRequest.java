/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.admin.internal;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.internal.admin.remote.AdminMultipleReplyProcessor;
import com.gemstone.gemfire.internal.admin.remote.AdminResponse;
import com.gemstone.gemfire.internal.admin.remote.CliLegacyMessage;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 * A request to from an admin VM to all non admin members
 * to start a backup. In the prepare phase of the backup,
 * the members will suspend bucket destroys to make sure
 * buckets aren't missed during the backup.
 * 
 * @author dsmith
 *
 */
public class FlushToDiskRequest  extends CliLegacyMessage {
  
  public FlushToDiskRequest() {
    
  }
  
  public static void send(DM dm, Set recipients) {
    FlushToDiskRequest request = new FlushToDiskRequest();
    request.setRecipients(recipients);

    FlushToDiskProcessor replyProcessor = new FlushToDiskProcessor(dm, recipients);
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
  }
  
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    HashSet<PersistentID> persistentIds;
    if(cache != null) {
      Collection<DiskStoreImpl> diskStores = cache.listDiskStoresIncludingRegionOwned();
      for(DiskStoreImpl store : diskStores) {
        store.flush();
      }
    }
    
    return new FlushToDiskResponse(this.getSender());
  }

  public int getDSFID() {
    return FLUSH_TO_DISK_REQUEST;
  }
  
  private static class FlushToDiskProcessor extends AdminMultipleReplyProcessor {
    public FlushToDiskProcessor(DM dm, Collection initMembers) {
      super(dm, initMembers);
    }
    
    @Override
    protected boolean stopBecauseOfExceptions() {
      return false;
    }
  }
}
