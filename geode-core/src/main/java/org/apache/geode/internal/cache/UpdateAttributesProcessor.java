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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionAdvisor.Profile;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.MessageWithReply;
import org.apache.geode.distributed.internal.ReplyException;
import org.apache.geode.distributed.internal.ReplyMessage;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * This class is a bit misnamed. It really has more with pushing a DistributionAdvisee's profile out
 * to others and, optionally if <code>profileExchange</code>, fetching the profile of anyone who
 * excepts the pushed profile.
 *
 */
public class UpdateAttributesProcessor {
  private static final Logger logger = LogService.getLogger();

  protected final DistributionAdvisee advisee;
  private boolean profileExchange = false;
  /**
   * If true then sender is telling receiver to remove the sender's profile. No profile exchange is
   * needed in this case.
   *
   * @since GemFire 5.7
   */
  private boolean removeProfile = false;
  private ReplyProcessor21 processor;

  /** Creates a new instance of UpdateAttributesProcessor */
  public UpdateAttributesProcessor(DistributionAdvisee da) {
    this(da, false);
  }

  /**
   * Creates a new instance of UpdateAttributesProcessor
   *
   * @since GemFire 5.7
   */
  public UpdateAttributesProcessor(DistributionAdvisee da, boolean removeProfile) {
    this.advisee = da;
    this.removeProfile = removeProfile;
  }

  /**
   * Distribute new profile version without exchange of profiles. Same as calling
   * {@link #distribute(boolean)} with (false).
   */
  void distribute() {
    distribute(false);
  }

  /**
   * Distribute with optional exchange of profiles but do not create new profile version.
   *
   * @param exchangeProfiles true if we want to receive profile replies
   */
  public void distribute(boolean exchangeProfiles) {
    sendProfileUpdate(exchangeProfiles);
    waitForProfileResponse();
  }

  public void waitForProfileResponse() {
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
        e.handleCause();
      }
    } finally {
      processor.cleanup();
    }
  }

  public void sendProfileUpdate(boolean exchangeProfiles) {
    DistributionManager mgr = this.advisee.getDistributionManager();
    DistributionAdvisor advisor = this.advisee.getDistributionAdvisor();
    this.profileExchange = exchangeProfiles;

    // if this is not intended for the purpose of exchanging profiles but
    // the advisor is uninitialized, then just exchange profiles anyway
    // and be done with it (instead of exchanging profiles and then sending
    // an attributes update)

    if (!exchangeProfiles) {
      if (this.removeProfile) {
        if (!advisor.isInitialized()) {
          // no need to tell the other advisor we are going away since
          // never got initialized.
          return;
        }
      } else if (advisor.initializationGate()) {
        // it just did the profile exchange so we are done
        return;
      }
    }

    final Set recipients;
    if (this.removeProfile) {
      recipients = advisor.adviseProfileRemove();
    } else if (exchangeProfiles) {
      recipients = advisor.adviseProfileExchange();
    } else {
      recipients = advisor.adviseProfileUpdate();
    }

    if (recipients.isEmpty()) {
      return;
    }

    ReplyProcessor21 processor = null;
    // Scope scope = this.region.scope;

    // always require an ack to prevent misordering of messages
    InternalDistributedSystem system = this.advisee.getSystem();
    processor = new UpdateAttributesReplyProcessor(system, recipients);
    UpdateAttributesMessage message = getUpdateAttributesMessage(processor, recipients);
    mgr.putOutgoing(message);
    this.processor = processor;
  }


  UpdateAttributesMessage getUpdateAttributesMessage(ReplyProcessor21 processor, Set recipients) {

    UpdateAttributesMessage msg = new UpdateAttributesMessage();
    msg.adviseePath = this.advisee.getFullPath();
    msg.setRecipients(recipients);
    if (processor != null) {
      msg.processorId = processor.getProcessorId();
    }
    msg.profile = this.advisee.getProfile();
    msg.exchangeProfiles = this.profileExchange;
    msg.removeProfile = this.removeProfile;
    return msg;
  }

  class UpdateAttributesReplyProcessor extends ReplyProcessor21 {

    UpdateAttributesReplyProcessor(InternalDistributedSystem system, Set members) {
      super(system, members);
    }

    /**
     * Registers this processor as a membership listener and returns a set of the current members.
     *
     * @return a Set of the current members
     * @since GemFire 5.7
     */
    @Override
    protected List addListenerAndGetMembers() {
      DistributionAdvisor da = UpdateAttributesProcessor.this.advisee.getDistributionAdvisor();
      if (da.useAdminMembersForDefault()) {
        return getDistributionManager().addAllMembershipListenerAndGetAllIds(this);
      } else {
        return super.addListenerAndGetMembers();
      }
    }

    /**
     * Unregisters this processor as a membership listener
     *
     * @since GemFire 5.7
     */
    @Override
    protected void removeListener() {
      DistributionAdvisor da = UpdateAttributesProcessor.this.advisee.getDistributionAdvisor();
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
     * @since GemFire 5.7
     */
    @Override
    protected List getDistributionManagerIds() {
      DistributionAdvisor da = UpdateAttributesProcessor.this.advisee.getDistributionAdvisor();
      if (da.useAdminMembersForDefault()) {
        return getDistributionManager().getDistributionManagerIdsIncludingAdmin();
      } else {
        return super.getDistributionManagerIds();
      }
    }

    @Override
    public void process(DistributionMessage msg) {
      try {
        if (msg instanceof ProfilesReplyMessage) {
          ProfilesReplyMessage reply = (ProfilesReplyMessage) msg;
          if (reply.profiles != null) {
            for (int i = 0; i < reply.profiles.length; i++) {
              // @todo Add putProfiles to DistributionAdvisor to do this
              // with one call atomically?
              UpdateAttributesProcessor.this.advisee.getDistributionAdvisor()
                  .putProfile(reply.profiles[i]);
            }
          }
        } else if (msg instanceof ProfileReplyMessage) {
          ProfileReplyMessage reply = (ProfileReplyMessage) msg;
          if (reply.profile != null) {
            UpdateAttributesProcessor.this.advisee.getDistributionAdvisor()
                .putProfile(reply.profile);
          }
        }
      } finally {
        super.process(msg);
      }
    }
  }


  public static class UpdateAttributesMessage extends HighPriorityDistributionMessage
      implements MessageWithReply {

    protected String adviseePath;
    protected int processorId = 0;
    protected Profile profile;
    protected boolean exchangeProfiles = false;
    protected boolean removeProfile = false;

    @Override
    public int getProcessorId() {
      return this.processorId;
    }

    @Override
    public boolean sendViaUDP() {
      return true;
    }

    @Override
    protected void process(ClusterDistributionManager dm) {
      Throwable thr = null;
      boolean sendReply = this.processorId != 0;
      List<Profile> replyProfiles = null;
      try {
        if (this.profile != null) {
          if (this.exchangeProfiles) {
            replyProfiles = new ArrayList<Profile>();
          }
          this.profile.processIncoming(dm, this.adviseePath, this.removeProfile,
              this.exchangeProfiles, replyProfiles);
        }
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("<cache closed> ///{}", this);
        }
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above). However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        thr = t;
      } finally {
        if (sendReply) {
          ReplyException rex = null;
          if (thr != null) {
            rex = new ReplyException(thr);
          }
          if (replyProfiles == null || replyProfiles.size() <= 1) {
            Profile p = null;
            if (replyProfiles != null && replyProfiles.size() == 1) {
              p = replyProfiles.get(0);
            }
            ProfileReplyMessage.send(getSender(), this.processorId, rex, dm, p);
          } else {
            Profile[] profiles = new Profile[replyProfiles.size()];
            replyProfiles.toArray(profiles);
            ProfilesReplyMessage.send(getSender(), this.processorId, rex, dm, profiles);
          }
        }
      }
    }

    @Override
    public String toString() {
      StringBuilder buff = new StringBuilder();
      buff.append("UpdateAttributesMessage (adviseePath=");
      buff.append(this.adviseePath);
      buff.append("; processorId=");
      buff.append(this.processorId);
      buff.append("; profile=");
      buff.append(this.profile);
      if (this.exchangeProfiles) {
        buff.append("; exchangeProfiles");
      }
      if (this.removeProfile) {
        buff.append("; removeProfile");
      }
      buff.append(")");
      return buff.toString();
    }

    @Override
    public int getDSFID() {
      return UPDATE_ATTRIBUTES_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.adviseePath = DataSerializer.readString(in);
      this.processorId = in.readInt();
      // set the processor ID to be able to send reply to sender in case of any
      // unexpected exception during deserialization etc.
      ReplyProcessor21.setMessageRPId(this.processorId);
      this.profile = DataSerializer.readObject(in);
      this.exchangeProfiles = in.readBoolean();
      this.removeProfile = in.readBoolean();
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeString(this.adviseePath, out);
      out.writeInt(this.processorId);
      DataSerializer.writeObject(this.profile, out);
      out.writeBoolean(this.exchangeProfiles);
      out.writeBoolean(this.removeProfile);
    }
  }


  public static class ProfileReplyMessage extends ReplyMessage {
    Profile profile;

    public static void send(InternalDistributedMember recipient, int processorId,
        ReplyException exception, ClusterDistributionManager dm, Profile profile) {
      Assert.assertTrue(recipient != null, "Sending a ProfileReplyMessage to ALL");
      ProfileReplyMessage m = new ProfileReplyMessage();

      m.processorId = processorId;
      m.profile = profile;
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
      return PROFILE_REPLY_MESSAGE;
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      this.profile = (Profile) DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(this.profile, out);
    }

    @Override
    public String toString() {
      final StringBuilder buff = new StringBuilder();
      buff.append("ProfileReplyMessage");
      buff.append(" (processorId=");
      buff.append(super.processorId);
      buff.append("; profile=");
      buff.append(this.profile);
      buff.append(")");
      return buff.toString();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.distributed.internal.ReplyMessage#getInlineProcess()
     * ProfileReplyMessages must be processed in-line and not in a pool to keep partitioned region
     * bucket profile exchange from swamping the high priority pool and not allowing other profile
     * exchanges through. This is safe as long as ProfileReplyMessage obtains no extra
     * synchronization locks.
     */
    @Override
    public boolean getInlineProcess() {
      return true;
    }

  }
  /**
   * Used to return multiple profiles
   *
   * @since GemFire 5.7
   */
  public static class ProfilesReplyMessage extends ReplyMessage {
    Profile[] profiles;

    public static void send(InternalDistributedMember recipient, int processorId,
        ReplyException exception, ClusterDistributionManager dm, Profile[] profiles) {
      Assert.assertTrue(recipient != null, "Sending a ProfilesReplyMessage to ALL");
      ProfilesReplyMessage m = new ProfilesReplyMessage();

      m.processorId = processorId;
      m.profiles = profiles;
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
      return PROFILES_REPLY_MESSAGE;
    }



    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      int length = in.readInt();
      if (length == -1) {
        this.profiles = null;
      } else {
        Profile[] array = new Profile[length];
        for (int i = 0; i < length; i++) {
          array[i] = (Profile) DataSerializer.readObject(in);
        }
        this.profiles = array;
      }
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      if (this.profiles == null) {
        out.writeInt(-1);
      } else {
        int length = this.profiles.length;
        out.writeInt(length);
        for (int i = 0; i < length; i++) {
          DataSerializer.writeObject(this.profiles[i], out);
        }
      }
    }

    @Override
    public String toString() {
      final StringBuilder buff = new StringBuilder();
      buff.append("ProfilesReplyMessage");
      buff.append(" (processorId=");
      buff.append(super.processorId);
      if (this.profiles != null) {
        buff.append("; profiles=");
        buff.append(Arrays.asList(this.profiles));
      }
      buff.append(")");
      return buff.toString();
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.distributed.internal.ReplyMessage#getInlineProcess()
     * ProfilesReplyMessages must be processed in-line and not in a pool to keep partitioned region
     * bucket profile exchange from swamping the high priority pool and not allowing other profile
     * exchanges through. This is safe as long as ProfilesReplyMessage obtains no extra
     * synchronization locks.
     */
    @Override
    public boolean getInlineProcess() {
      return true;
    }
  }
}
