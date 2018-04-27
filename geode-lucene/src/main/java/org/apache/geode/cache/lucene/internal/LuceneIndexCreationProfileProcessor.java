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
package org.apache.geode.cache.lucene.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.CacheDistributionAdvisee;
import org.apache.geode.internal.cache.CacheDistributionAdvisor;
import org.apache.geode.internal.cache.CacheServiceProfile;
import org.apache.geode.internal.cache.DistributedRegion;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.persistence.PersistentMemberID;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

public class LuceneIndexCreationProfileProcessor {
  protected CacheDistributionAdvisee userDataRegion;

  private static final Logger logger = LogService.getLogger();
  private boolean allMembersSkippedChecks = true;

  public LuceneIndexCreationProfileProcessor(CacheDistributionAdvisee userDataRegion) {
    this.userDataRegion = userDataRegion;
  }

  private Set getAdvice() {
    if (this.userDataRegion instanceof BucketRegion) {
      return ((BucketRegion) this.userDataRegion).getBucketAdvisor().adviseProfileExchange();
    } else {
      DistributionAdvisee rgn = this.userDataRegion.getParentAdvisee();
      DistributionAdvisor advisor = rgn.getDistributionAdvisor();
      return advisor.adviseGeneric();
    }
  }

  protected Set getRecipients() {
    DistributionAdvisee parent = this.userDataRegion.getParentAdvisee();
    Set recps = null;
    if (parent == null) { // root region, all recipients
      InternalDistributedSystem system = this.userDataRegion.getSystem();
      recps = system.getDistributionManager().getOtherDistributionManagerIds();
    } else {
      // get recipients that have the parent region defined as distributed.
      recps = getAdvice();
    }
    return recps;
  }

  public void validateLuceneIndexCreationProfiles() {
    for (int retry = 0; retry < 5; retry++) {
      Set remoteRecipients = getRecipients();
      if (remoteRecipients.isEmpty()) {
        if (logger.isDebugEnabled()) {
          logger.debug("No remote members found to validate Lucene Index Creation Profile");
        }
        return;
      }

      LuceneIndexCreationProfileReplyProcessor replyProcessor =
          new LuceneIndexCreationProfileReplyProcessor(remoteRecipients);
      LuceneIndexCreationProfileMessage profileMessage =
          getLuceneIndexCreationProfileMessage(remoteRecipients, replyProcessor);

      if (((LocalRegion) userDataRegion).isUsedForPartitionedRegionBucket()) {
        replyProcessor.enableSevereAlertProcessing();
        profileMessage.severeAlertCompatible = true;
      }

      this.userDataRegion.getDistributionManager().putOutgoing(profileMessage);

      try {
        replyProcessor.waitForRepliesUninterruptibly();
        if (!allMembersSkippedChecks) {
          // If member is still initializing then retry
          break;
        }
      } catch (ReplyException replyException) {
        Throwable t = replyException.getCause();
        if (t instanceof IllegalStateException) {
          // region is incompatible with region in another cache
          throw (IllegalStateException) t;
        }
        replyException.handleCause();
        break;
      } finally {
        replyProcessor.cleanup();
      }
    }
  }

  protected LuceneIndexCreationProfileMessage getLuceneIndexCreationProfileMessage(
      Set remoteRecipients, ReplyProcessor21 replyProcessor) {
    LuceneIndexCreationProfileMessage profileMessage = new LuceneIndexCreationProfileMessage();
    profileMessage.regionPath = this.userDataRegion.getFullPath();
    profileMessage.profile =
        (CacheDistributionAdvisor.CacheProfile) this.userDataRegion.getProfile();
    profileMessage.processorID = replyProcessor.getProcessorId();
    profileMessage.concurrencyChecksEnabled =
        this.userDataRegion.getAttributes().getConcurrencyChecksEnabled();
    profileMessage.setMulticast(false);
    profileMessage.setRecipients(remoteRecipients);
    return profileMessage;
  }

  public static final class LuceneIndexCreationProfileMessage
      extends HighPriorityDistributionMessage implements MessageWithReply {
    protected String regionPath;
    protected CacheDistributionAdvisor.CacheProfile profile;
    protected int processorID;
    protected transient boolean severeAlertCompatible;
    public boolean concurrencyChecksEnabled;
    private transient ReplyException replyException;
    private transient boolean skippedCompatibilityChecks;

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(this.regionPath, out);
      DataSerializer.writeObject(this.profile, out);
      out.writeInt(this.processorID);
      out.writeBoolean(this.concurrencyChecksEnabled);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.regionPath = DataSerializer.readString(in);
      this.profile = (CacheDistributionAdvisor.CacheProfile) DataSerializer.readObject(in);
      this.processorID = in.readInt();
      this.concurrencyChecksEnabled = in.readBoolean();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("LuceneIndexCreationProfileMessage ( region = '");
      sb.append(this.regionPath);
      sb.append("' ; processorID=");
      sb.append(this.processorID);
      sb.append("; concurrencyChecksEnabled=");
      sb.append(this.concurrencyChecksEnabled);
      sb.append("; profile=");
      sb.append(this.profile);
      sb.append(")");

      return sb.toString();
    }

    @Override
    public boolean isSevereAlertCompatible() {
      return severeAlertCompatible;
    }

    @Override
    public boolean sendViaUDP() {
      return true;
    }


    @Override
    protected void process(ClusterDistributionManager dm) {

      int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.ANY_INIT);
      LocalRegion localRegion = null;
      PersistentMemberID destroyedId = null;

      try {
        GemFireCacheImpl cache = (GemFireCacheImpl) CacheFactory.getInstance(dm.getSystem());
        DistributedRegion destroyingRegion = cache.getRegionInDestroy(this.regionPath);
        if (destroyingRegion != null) {
          destroyedId = destroyingRegion.getPersistentID();
        }

        localRegion = (LocalRegion) cache.getRegion(this.regionPath);

        if (localRegion instanceof CacheDistributionAdvisee) {
          if (localRegion.isUsedForPartitionedRegionBucket()) {
            if (!((BucketRegion) localRegion).isPartitionedRegionOpen()) {
              if (logger.isDebugEnabled()) {
                logger.debug("<Partitioned Region Closed or Locally Destroyed> {}", this);
              }
              return;
            }
          }
          // Handle the message
          validateLuceneIndexCreationProfile((CacheDistributionAdvisee) localRegion);
        }
      } catch (RegionDestroyedException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("<RegionDestroyed> {}", this);
        }
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("<CancelException> {}", this);
        }
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        throw err;
      } catch (Throwable t) {
        SystemFailure.checkFailure();
        if (replyException == null) {
          replyException = new ReplyException(t);
        } else {
          logger.warn(LocalizedMessage.create(
              LocalizedStrings.CreateRegionProcessor_MORE_THAN_ONE_EXCEPTION_THROWN_IN__0, this),
              t);
        }
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
        LuceneIndexCreationProfileReplyMessage replyMessage =
            new LuceneIndexCreationProfileReplyMessage();
        replyMessage.setException(replyException);
        replyMessage.setProcessorId(this.processorID);
        replyMessage.setSender(dm.getId());
        replyMessage.skippedCompatibilityChecks = this.skippedCompatibilityChecks;
        replyMessage.destroyedId = destroyedId;
        replyMessage.setRecipient(this.getSender());
        dm.putOutgoing(replyMessage);

      }

    }

    /**
     * When many members are started concurrently, it is possible that an accessor or non-version
     * generating replicate receives CreateRegionMessage before it is initialized, thus preventing
     * persistent members from starting. We skip compatibilityChecks if the region is not
     * initialized, and let other members check compatibility. If all members skipCompatabilit
     * checks, then the CreateRegionMessage should be retried. fixes #45186
     */
    private boolean skipDuringInitialization(CacheDistributionAdvisee rgn) {
      boolean skip = false;
      if (rgn instanceof LocalRegion) {
        LocalRegion lr = (LocalRegion) rgn;
        if (!lr.isInitialized()) {
          Set recipients = new LuceneIndexCreationProfileProcessor(rgn).getRecipients();
          recipients.remove(getSender());
          if (!recipients.isEmpty()) {
            skip = true;
          }
        }
      }
      return skip;
    }

    private void validateLuceneIndexCreationProfile(CacheDistributionAdvisee localRegion) {
      boolean initializing = skipDuringInitialization(localRegion);
      if (initializing) {
        this.skippedCompatibilityChecks = true;
      }

      String errorMessage = null;
      errorMessage = checkCompatibility(localRegion, this.profile);
      if (errorMessage != null) {
        this.replyException = new ReplyException(errorMessage);
      }

    }

    private String checkCompatibility(CacheDistributionAdvisee localRegion,
        CacheDistributionAdvisor.CacheProfile profile) {
      String result = null;
      Map<String, CacheServiceProfile> myProfiles =
          ((LocalRegion) localRegion).getCacheServiceProfiles();
      for (CacheServiceProfile remoteProfile : profile.cacheServiceProfiles) {
        CacheServiceProfile localProfile = myProfiles.get(remoteProfile.getId());
        if (localProfile != null) {
          result = remoteProfile.checkCompatibility(localRegion.getFullPath(), localProfile);
        }
        if (result != null) {
          break;
        }
      }
      return result;
    }

    @Override
    public int getDSFID() {
      return LUCENE_INDEX_CREATION_PROFILE_MSG;
    }
  }

  public static final class LuceneIndexCreationProfileReplyMessage extends ReplyMessage {

    protected PersistentMemberID destroyedId;
    protected boolean skippedCompatibilityChecks;

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      if (in.readBoolean()) {
        this.destroyedId = new PersistentMemberID();
        InternalDataSerializer.invokeFromData(this.destroyedId, in);
      }
      this.skippedCompatibilityChecks = in.readBoolean();
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("LuceneIndexCreationProfileReplyMessage(");
      sb.append("sender =").append(getSender());
      sb.append(", skippedCompatibilityChecks = ").append(this.skippedCompatibilityChecks);
      sb.append(")");

      return sb.toString();
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      if (this.destroyedId != null) {
        out.writeBoolean(true);
        InternalDataSerializer.invokeToData(destroyedId, out);
      } else {
        out.writeBoolean(false);
      }
      out.writeBoolean(this.skippedCompatibilityChecks);

    }

    @Override
    public int getDSFID() {
      return LUCENE_INDEX_CREATION_PROFILE_REPLY_MSG;
    }
  }

  class LuceneIndexCreationProfileReplyProcessor extends ReplyProcessor21 {

    public LuceneIndexCreationProfileReplyProcessor(Set initMembers) {
      super((InternalDistributedSystem) LuceneIndexCreationProfileProcessor.this.userDataRegion
          .getCache().getDistributedSystem(), initMembers);
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        LuceneIndexCreationProfileReplyMessage reply = (LuceneIndexCreationProfileReplyMessage) msg;
        if (reply.destroyedId != null && userDataRegion instanceof DistributedRegion) {
          DistributedRegion dr = (DistributedRegion) userDataRegion;
          dr.getPersistenceAdvisor().removeMember(reply.destroyedId);
        }
        if (!reply.skippedCompatibilityChecks) {
          allMembersSkippedChecks = false;
        }
      } finally {
        super.process(msg);
      }

    }
  }


}
