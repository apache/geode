/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.internal.admin.remote.AdminMultipleReplyProcessor;
import com.gemstone.gemfire.internal.admin.remote.AdminRequest;
import com.gemstone.gemfire.internal.admin.remote.AdminResponse;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * An instruction to all members with cache that they should 
 * compact their disk stores.
 * 
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
// NOTE: This is copied from com/gemstone/gemfire/internal/admin/remote/CompactRequest.java
// and modified as per requirements. (original-author Dan Smith)
public class CompactRequest extends AdminRequest {
  private static final Logger logger = LogService.getLogger();
  
  private String diskStoreName;
  private static String notExecutedMembers;
  
  public static Map<DistributedMember, PersistentID> send(DM dm, String diskStoreName, Set<?> recipients) {
    Map<DistributedMember, PersistentID> results = Collections.emptyMap();

    if (recipients != null && !recipients.isEmpty()) {
      CompactRequest request = new CompactRequest();
      request.setRecipients(recipients);

      CompactReplyProcessor replyProcessor = new CompactReplyProcessor(dm, recipients);
      request.msgId = replyProcessor.getProcessorId();
      request.diskStoreName = diskStoreName;
      request.setSender(dm.getDistributionManagerId());
      Set<?> putOutgoing = dm.putOutgoing(request);
      if (putOutgoing != null && !putOutgoing.isEmpty()) {
        notExecutedMembers = putOutgoing.toString();
      }

      try {
        replyProcessor.waitForReplies();
      } catch (ReplyException e) {
        if(!(e.getCause() instanceof CancelException)) {
          throw e;
        }
      } catch (InterruptedException e) {
        logger.debug(e.getMessage(), e);
      }

      results = replyProcessor.results;
    }

    return results;
  }

  @Override
  protected void process(DistributionManager dm) {
    super.process(dm);
  }

  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    PersistentID compactedDiskStore = compactDiskStore(this.diskStoreName);

    return new CompactResponse(this.getSender(), compactedDiskStore);
  }
  
  public static PersistentID compactDiskStore(String diskStoreName) {
    PersistentID persistentID = null;
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if(cache != null && !cache.isClosed()) {
      DiskStoreImpl diskStore = (DiskStoreImpl) cache.findDiskStore(diskStoreName);
      if(diskStore != null && diskStore.forceCompaction()) {
        persistentID = diskStore.getPersistentID();
      } 
    }
    
    return persistentID;
  }

  public static String getNotExecutedMembers() {
    return notExecutedMembers;
  }

  public int getDSFID() {
    return MGMT_COMPACT_REQUEST;
  }
  
  @Override
  public void fromData(DataInput in) throws IOException,ClassNotFoundException {
    super.fromData(in);
    this.diskStoreName = DataSerializer.readString(in);
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.diskStoreName, out);
  }

  @Override  
  public String toString() {
    return "Compact request sent to " + Arrays.toString(this.getRecipients()) +
      " from " + this.getSender() +" for "+this.diskStoreName;
  }

  private static class CompactReplyProcessor extends AdminMultipleReplyProcessor {
    Map<DistributedMember, PersistentID> results = Collections.synchronizedMap(new HashMap<DistributedMember, PersistentID>());
    
    public CompactReplyProcessor(DM dm, Collection<?> initMembers) {
      super(dm, initMembers);
    }
    
    @Override
    protected boolean stopBecauseOfExceptions() {
      return false;
    }

    @Override
    protected boolean allowReplyFromSender() {
      return true;
    }

    @Override
    protected void process(DistributionMessage msg, boolean warn) {
      if(msg instanceof CompactResponse) {
        final PersistentID persistentId = ((CompactResponse) msg).getPersistentId();
        if(persistentId != null) {
          results.put(msg.getSender(), persistentId);
        }
      }
      super.process(msg, warn);
    }
  }
}
