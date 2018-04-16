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

package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.logging.LogService;

public class ValidateCacheServerProfileProcessor {
  private static final Logger logger = LogService.getLogger();

  protected final DistributionAdvisee advisee;

  public ValidateCacheServerProfileProcessor(DistributionAdvisee distributionAdvisee) {
    this.advisee = distributionAdvisee;
  }

  private ValidateCacheProfileMessage getValidateCacheProfileMessage(
      ValidateProfileUpdateReplyProcessor processor) {
    ValidateCacheProfileMessage validateCacheProfileMessage =
        new ValidateCacheProfileMessage(this.advisee.getFullPath(), processor.getRecipients(),
            processor.getProcessorId(), this.advisee.getProfile());
    return validateCacheProfileMessage;
  }

  void validateCacheServerProfiles() throws IncompatibleCacheServiceProfileException {
    final Set recipients = this.advisee.getDistributionAdvisor().adviseProfileUpdate();

    if (recipients.isEmpty()) {
      return;
    }

    ValidateProfileUpdateReplyProcessor replyProcessor =
        new ValidateProfileUpdateReplyProcessor(this.advisee, recipients);
    ValidateCacheProfileMessage message = getValidateCacheProfileMessage(replyProcessor);
    this.advisee.getDistributionManager().putOutgoing(message);
    waitForProfileResponse(replyProcessor);
  }

  public void waitForProfileResponse(ReplyProcessor21 processor)
      throws IncompatibleCacheServiceProfileException {
    if (processor == null) {
      return;
    }
    DistributionManager mgr = this.advisee.getDistributionManager();
    try {
      // bug 36983 - you can't loop on a reply processor
      mgr.getCancelCriterion().checkCancelInProgress(null);
      try {
        processor.waitForRepliesUninterruptibly();
      } catch (ReplyException e) {
        if (e.getCause() instanceof IncompatibleCacheServiceProfileException) {
          throw (IncompatibleCacheServiceProfileException) e.getCause();
        }
      }
    } finally {
      processor.cleanup();
    }
  }

  class ValidateProfileUpdateReplyProcessor extends ReplyProcessor21 {

    private final DistributionAdvisee advisee;

    ValidateProfileUpdateReplyProcessor(DistributionAdvisee advisee, Set members) {
      super(advisee.getSystem(), members);
      this.advisee = advisee;
    }

    /**
     * Registers this processor as a membership listener and returns a set of the current members.
     *
     * @return a Set of the current members
     * @since Geode 1.7
     */
    @Override
    protected Set addListenerAndGetMembers() {
      DistributionAdvisor da = this.advisee.getDistributionAdvisor();
      if (da.useAdminMembersForDefault()) {
        return getDistributionManager().addAllMembershipListenerAndGetAllIds(this);
      } else {
        return super.addListenerAndGetMembers();
      }
    }

    /**
     * Unregisters this processor as a membership listener
     *
     * @since Geode 1.7
     */
    @Override
    protected void removeListener() {
      DistributionAdvisor da = this.advisee.getDistributionAdvisor();
      if (da.useAdminMembersForDefault()) {
        getDistributionManager().removeAllMembershipListener(this);
      } else {
        super.removeListener();
      }
    }

    /**
     * If this processor being used by controller then return ALL members; otherwise defer to super.
     *
     * @return a Set of the current members
     * @since Geode 1.7
     */
    @Override
    protected Set getDistributionManagerIds() {
      DistributionAdvisor da = this.advisee.getDistributionAdvisor();
      if (da.useAdminMembersForDefault()) {
        return getDistributionManager().getDistributionManagerIdsIncludingAdmin();
      } else {
        return super.getDistributionManagerIds();
      }
    }

    public Collection getRecipients() {
      return Arrays.asList(members);
    }

  }

  public static class ValidateCacheProfileMessage extends HighPriorityDistributionMessage
      implements MessageWithReply {

    private String adviseePath;
    private int processorId;
    private Profile profile;

    public ValidateCacheProfileMessage() {}

    ValidateCacheProfileMessage(String adviseePath, Collection recipients, int processorId,
        Profile profile) {
      this.adviseePath = adviseePath;
      this.setRecipients(recipients);
      this.processorId = processorId;
      this.profile = profile;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      if (adviseePath != null) {
        LocalRegion region = (LocalRegion) dm.getCache().getRegion(adviseePath);
        String compatibility = checkCacheServerProfileCompatibility(region,
            (CacheDistributionAdvisor.CacheProfile) this.profile);

        ReplyException replyException = (compatibility != null
            ? new ReplyException(new IncompatibleCacheServiceProfileException(compatibility))
            : null);
        ValidateCacheServerProfileReplyMessage.send(getSender(), this.processorId, replyException,
            dm);
      }
    }

    private String checkCacheServerProfileCompatibility(LocalRegion localRegion,
        CacheDistributionAdvisor.CacheProfile cacheProfile) {
      String cspResult = null;
      Map<String, CacheServiceProfile> myProfiles = localRegion.getCacheServiceProfiles();
      // Iterate and compare the remote CacheServiceProfiles to the local ones
      for (CacheServiceProfile remoteProfile : cacheProfile.cacheServiceProfiles) {
        CacheServiceProfile localProfile = myProfiles.get(remoteProfile.getId());
        if (localProfile != null) {
          cspResult = remoteProfile.checkCompatibility(localRegion.getFullPath(), localProfile);
        }
        if (cspResult != null) {
          return cspResult;
        }
      }
      return cspResult;
    }

    public int getDSFID() {
      return VALIDATE_CACHE_PROFILE_MESSAGE;
    }

    @Override
    public String toString() {
      StringBuilder buff = new StringBuilder();
      buff.append(this.getClass().getName() + " (adviseePath=");
      buff.append(this.adviseePath);
      buff.append("; processorId=");
      buff.append(this.processorId);
      buff.append("; profile=");
      buff.append(this.profile);
      buff.append(")");
      return buff.toString();
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.adviseePath = DataSerializer.readString(in);
      this.processorId = in.readInt();
      // set the processor ID to be able to send reply to sender in case of any
      // unexpected exception during deserialization etc.
      ReplyProcessor21.setMessageRPId(this.processorId);
      this.profile = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeString(this.adviseePath, out);
      out.writeInt(this.processorId);
      DataSerializer.writeObject(this.profile, out);
    }
  }

  public static class ValidateCacheServerProfileReplyMessage extends ReplyMessage {

    public static void send(InternalDistributedMember recipient, int processorId,
        ReplyException exception, ClusterDistributionManager dm) {
      ValidateCacheServerProfileReplyMessage m = new ValidateCacheServerProfileReplyMessage();

      m.processorId = processorId;
      if (exception != null) {
        m.setException(exception);
        if (logger.isDebugEnabled()) {
          logger.debug("Replying with exception: {}" + m, exception);
        }
      }
      m.setRecipient(recipient);
      dm.putOutgoing(m);
    }

    @Override
    public int getDSFID() {
      return VALIDATE_CACHE_SERVER_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
    }

    @Override
    public String toString() {
      final StringBuilder buff = new StringBuilder();
      buff.append(this.getClass().getName());
      buff.append(" (processorId=");
      buff.append(super.processorId);
      buff.append(")");
      return buff.toString();
    }

    @Override
    public boolean getInlineProcess() {
      return true;
    }
  }

  public static class ValidateCacheServerProfileReplyException extends ReplyException {
    public ValidateCacheServerProfileReplyException() {
      super();
    }

    public ValidateCacheServerProfileReplyException(String msg) {
      super(msg);
    }

    public ValidateCacheServerProfileReplyException(String msg, Throwable cause) {
      super(msg, cause);
    }

    public ValidateCacheServerProfileReplyException(Throwable cause) {
      super(cause);
    }

    @Override
    public void handleCause() {
      super.handleCause();
    }
  }
}
