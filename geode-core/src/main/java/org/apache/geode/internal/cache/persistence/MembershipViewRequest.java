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
package org.apache.geode.internal.cache.persistence;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.i18n.LogWriterI18n;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegionHelper;
import org.apache.geode.internal.cache.partitioned.Bucket;
import org.apache.geode.internal.logging.LogService;

/**
 *
 */
public class MembershipViewRequest extends
  DistributionMessage implements MessageWithReply {

  private static final Logger logger = LogService.getLogger();
  
  private String regionPath;
  private int processorId;
  private boolean targetReinitializing;

  public MembershipViewRequest() {
    
  }
  
  public MembershipViewRequest(String regionPath, int processorId, boolean targetReinitializing) {
    this.regionPath = regionPath;
    this.processorId = processorId;
    this.targetReinitializing = targetReinitializing;
  }

  public static PersistentMembershipView send(
      InternalDistributedMember recipient, DM dm, String regionPath, boolean targetReinitializing) throws ReplyException {
    MembershipViewRequestReplyProcessor processor = new MembershipViewRequestReplyProcessor(dm, recipient);
    MembershipViewRequest msg = new MembershipViewRequest(regionPath, processor.getProcessorId(), targetReinitializing);
    msg.setRecipient(recipient);
    dm.putOutgoing(msg);
    return processor.getResult();
  }
  
  public static Set<PersistentMembershipView> send(
      Set<InternalDistributedMember> recipients, DM dm, String regionPath) throws ReplyException {
    MembershipViewRequestReplyProcessor processor = new MembershipViewRequestReplyProcessor(dm, recipients);
    MembershipViewRequest msg = new MembershipViewRequest(regionPath, processor.getProcessorId(), false);
    msg.setRecipients(recipients);
    dm.putOutgoing(msg);
    return processor.getResults();
  }
  
  @Override  
  final public int getProcessorType() {
    return this.targetReinitializing ? DistributionManager.WAITING_POOL_EXECUTOR :
                              DistributionManager.HIGH_PRIORITY_EXECUTOR;
  }

  @Override
  protected void process(DistributionManager dm) {
    int initLevel = this.targetReinitializing ? LocalRegion.AFTER_INITIAL_IMAGE
        : LocalRegion.ANY_INIT;
    int oldLevel = LocalRegion.setThreadInitLevelRequirement(initLevel);

    PersistentMembershipView view = null;
    ReplyException exception = null;
    try {
      // get the region from the path, but do NOT wait on initialization,
      // otherwise we could have a distributed deadlock

      Cache cache = CacheFactory.getInstance(dm.getSystem());
      Region region = cache.getRegion(this.regionPath);
      PersistenceAdvisor persistenceAdvisor = null;
      if(region instanceof DistributedRegion) {
        persistenceAdvisor = ((DistributedRegion) region).getPersistenceAdvisor();
      } else if ( region == null) {
        Bucket proxy = PartitionedRegionHelper.getProxyBucketRegion(GemFireCacheImpl.getInstance(), this.regionPath, false);
        if(proxy != null) {
          persistenceAdvisor = proxy.getPersistenceAdvisor();
        }
      }
      if(persistenceAdvisor != null) {
        view= persistenceAdvisor.getMembershipView();
      }
    } catch (RegionDestroyedException e) {
//      exception = new ReplyException(e);
      logger.debug("<RegionDestroyed> {}", this);
    }
    catch (CancelException e) {
//      exception = new ReplyException(e);
      logger.debug("<CancelException> {}", this);
    }
    catch(VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch(Throwable t) {
      SystemFailure.checkFailure();
      exception = new ReplyException(t);
    }
    finally {
      LocalRegion.setThreadInitLevelRequirement(oldLevel);
      MembershipViewReplyMessage replyMsg = new MembershipViewReplyMessage();
      replyMsg.setRecipient(getSender());
      replyMsg.setProcessorId(processorId);
      replyMsg.view= view;
      if (logger.isDebugEnabled()) {
        logger.debug("MembershipViewRequest returning view {} for region {}", view, this.regionPath);
      }
      if(exception != null) {
        replyMsg.setException(exception);
      }
      dm.putOutgoing(replyMsg);
    }
  }

  public int getDSFID() {
    return PERSISTENT_MEMBERSHIP_VIEW_REQUEST;
  }
  
  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    processorId = in.readInt();
    regionPath = DataSerializer.readString(in);
    targetReinitializing = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(processorId);
    DataSerializer.writeString(regionPath, out);
    out.writeBoolean(targetReinitializing);
  }
  
  private static class MembershipViewRequestReplyProcessor extends ReplyProcessor21 {
    private Set<PersistentMembershipView> views = new HashSet<PersistentMembershipView>();

    
    
    public MembershipViewRequestReplyProcessor(DM dm,
        InternalDistributedMember member) {
      super(dm, member);
    }
    
    public MembershipViewRequestReplyProcessor(DM dm,
        Set<InternalDistributedMember> members) {
      super(dm, members);
    }

    public PersistentMembershipView getResult() {
      waitForRepliesUninterruptibly();
      if(views.isEmpty()) {
        //TODO prperist internationalize.
        throw new ReplyException("Member departed");
      }
      return views.iterator().next();
    }
    
    public Set<PersistentMembershipView> getResults() {
      waitForRepliesUninterruptibly();
      return views;
    }

    @Override
    public void process(DistributionMessage msg) {
      if(msg instanceof MembershipViewReplyMessage) {
        PersistentMembershipView view = ((MembershipViewReplyMessage) msg).view;
        if (logger.isDebugEnabled()) {
          logger.debug("MembershipViewReplyProcessor received {}", view);
        }
        if(view != null) {
          synchronized(this) {
            this.views.add(view);
          }
        }
      }
      super.process(msg);
    }
  }

  public static class MembershipViewReplyMessage extends ReplyMessage {
    private PersistentMembershipView view;

    @Override
    public int getDSFID() {
      return PERSISTENT_MEMBERSHIP_VIEW_REPLY;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      boolean hasView = in.readBoolean();
      if(hasView) {
        view = new PersistentMembershipView();
        InternalDataSerializer.invokeFromData(view, in);
      }
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      if(view == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        InternalDataSerializer.invokeToData(view, out);
      }
    }
  }
}
