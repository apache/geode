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
package com.gemstone.gemfire.management.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * 
 * @author darrel
 * @since 7.0
 */
public class JmxManagerAdvisor extends DistributionAdvisor {

  private static final Logger logger = LogService.getLogger();
  
  private JmxManagerAdvisor(DistributionAdvisee advisee) {
    super(advisee);
    JmxManagerProfile p = new JmxManagerProfile(getDistributionManager().getId(), incrementAndGetVersion());
    advisee.fillInProfile(p);
    ((JmxManagerAdvisee)advisee).initProfile(p);
  }
  
  public static JmxManagerAdvisor createJmxManagerAdvisor(DistributionAdvisee advisee) {
    JmxManagerAdvisor advisor = new JmxManagerAdvisor(advisee);
    advisor.initialize();
    return advisor;
  }
  
  @Override
  public String toString() {
    return new StringBuilder().append("JmxManagerAdvisor for " + getAdvisee()).toString();
  }

  public void broadcastChange() {
    try {
      Set<InternalDistributedMember> recips = adviseGeneric(); // for now just tell everyone
      JmxManagerProfile p = new JmxManagerProfile(getDistributionManager().getId(), incrementAndGetVersion());
      getAdvisee().fillInProfile(p);
      JmxManagerProfileMessage.send(getAdvisee().getSystem().getDistributionManager(), recips, p);
    } catch (CancelException ignore) {
    }
  }

  @SuppressWarnings("unchecked")
  public List<JmxManagerProfile> adviseAlreadyManaging() {
    return fetchProfiles(new Filter() {
      public boolean include(Profile profile) {
        assert profile instanceof JmxManagerProfile;
        JmxManagerProfile jmxProfile = (JmxManagerProfile) profile;
        return jmxProfile.isJmxManagerRunning();
      }
    });
  }
  @SuppressWarnings("unchecked")
  public List<JmxManagerProfile> adviseWillingToManage() {
    return fetchProfiles(new Filter() {
      public boolean include(Profile profile) {
        assert profile instanceof JmxManagerProfile;
        JmxManagerProfile jmxProfile = (JmxManagerProfile) profile;
        return jmxProfile.isJmxManager();
      }
    });
  }

  @Override
  protected Profile instantiateProfile(InternalDistributedMember memberId,
      int version) {
    return new JmxManagerProfile(memberId, version);
  }

  @Override
  /**
   * Overridden to also include our profile.
   * If our profile is included it will always be first.
   */
  protected List/*<Profile>*/ fetchProfiles(Filter f) {
    initializationGate();
    List result = null;
    {
      JmxManagerAdvisee advisee = (JmxManagerAdvisee) getAdvisee();
      Profile myp = advisee.getMyMostRecentProfile();
      if (f == null || f.include(myp)) {
        if (result == null) {
          result = new ArrayList();
        }
        result.add(myp);
      }
    }
    Profile[] locProfiles = this.profiles; // grab current profiles
    for (int i = 0; i < locProfiles.length; i++) {
      Profile profile = locProfiles[i];
      if (f == null || f.include(profile)) {
        if (result == null) {
          result = new ArrayList(locProfiles.length);
        }
        result.add(profile);
      }
    }
    if (result == null) {
      result = Collections.EMPTY_LIST;
    } else {
      result = Collections.unmodifiableList(result);
    }
    return result;
  }
  /**
   * Message used to push event updates to remote VMs
   */
  public static class JmxManagerProfileMessage extends
      HighPriorityDistributionMessage {
    private volatile JmxManagerProfile profile;
    private volatile int processorId;

    /**
     * Default constructor used for de-serialization (used during receipt)
     */
    public JmxManagerProfileMessage() {}
    
    @Override
    public boolean sendViaUDP() {
      return true;
    }

    /**
     * Constructor used to send
     * @param recips
     * @param p
     */
    private JmxManagerProfileMessage(final Set<InternalDistributedMember> recips,
        final JmxManagerProfile p) {
      setRecipients(recips);
      this.processorId = 0;
      this.profile = p;
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.distributed.internal.DistributionMessage#process(com.gemstone.gemfire.distributed.internal.DistributionManager)
     */
    @Override
    protected void process(DistributionManager dm) {
      Throwable thr = null;
      JmxManagerProfile p = null;
      try {
        final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        if (cache != null && !cache.isClosed()) {
          final JmxManagerAdvisor adv = cache.getJmxManagerAdvisor();
          p = this.profile;
          if (p != null) {
            adv.putProfile(p);
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("No cache {}", this);
          }
        }
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Cache closed, ", this);
        }
      } catch (VirtualMachineError err) {
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error.  We're poisoned
        // now, so don't let this thread continue.
        throw err;
      } catch (Throwable t) {
        // Whenever you catch Error or Throwable, you must also
        // catch VirtualMachineError (see above).  However, there is
        // _still_ a possibility that you are dealing with a cascading
        // error condition, so you also need to check to see if the JVM
        // is still usable:
        SystemFailure.checkFailure();
        thr = t;
      } finally {
        if (thr != null) {
          dm.getCancelCriterion().checkCancelInProgress(null);
          logger.info(LocalizedMessage.create(LocalizedStrings.ResourceAdvisor_MEMBER_CAUGHT_EXCEPTION_PROCESSING_PROFILE,
              new Object[] {p, toString()}, thr));
        }
      }
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
     */
    public int getDSFID() {
      return JMX_MANAGER_PROFILE_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.profile = (JmxManagerProfile)DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      DataSerializer.writeObject(this.profile, out);
    }

    /**
     * Send profile to the provided members
     * @param recips The recipients of the message
     * @throws ReplyException
     */
    public static void send(final DM dm, Set<InternalDistributedMember> recips,
        JmxManagerProfile profile) {
      JmxManagerProfileMessage r = new JmxManagerProfileMessage(recips, profile);
      dm.putOutgoing(r);
    }

    @Override
    public String getShortClassName() {
      return "JmxManagerProfileMessage";
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(getShortClassName())
      .append(" (processorId=").append(this.processorId)
      .append("; profile=").append(this.profile);
      sb.append(")");
      return sb.toString();
    }
  }

  public static class JmxManagerProfile extends Profile {
    
    private boolean jmxManager;
    private String host;
    private int port;
    private boolean ssl;
    private boolean started;
    
    // Constructor for de-serialization
    public JmxManagerProfile() {}

    public boolean isJmxManager() {
      return this.jmxManager;
    }

    public boolean isJmxManagerRunning() {
      return this.started;
    }

    public void setInfo(boolean jmxManager2, String host2, int port2, boolean ssl2, boolean started2) {
      this.jmxManager = jmxManager2;
      this.host = host2;
      this.port = port2;
      this.ssl = ssl2;
      this.started = started2;
    }
    
    public String getHost() {
      return this.host;
    }
    
    public int getPort() {
      return this.port;
    }
    
    public boolean getSsl() {
      return this.ssl;
    }

    // Constructor for sending purposes
    public JmxManagerProfile(InternalDistributedMember memberId,
        int version) {
      super(memberId, version);
    }
    
    public StringBuilder getToStringHeader() {
      return new StringBuilder("JmxManagerAdvisor.JmxManagerProfile");
    }

    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      synchronized (this) {
        if (this.jmxManager) {
          sb.append("; jmxManager");
        }
        sb.append("; host=").append(this.host)
        .append("; port=").append(this.port);
        if (this.ssl) {
          sb.append("; ssl");
        }
        if (this.started) {
          sb.append("; started");
        }
      }
    }
    
    @Override
    public void processIncoming(DistributionManager dm, String adviseePath,
        boolean removeProfile, boolean exchangeProfiles,
        final List<Profile> replyProfiles) {
      final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache != null && !cache.isClosed()) {
        handleDistributionAdvisee(cache.getJmxManagerAdvisor().getAdvisee(), removeProfile,
            exchangeProfiles, replyProfiles);
      }
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.jmxManager = DataSerializer.readPrimitiveBoolean(in);
      this.host = DataSerializer.readString(in);
      this.port = DataSerializer.readPrimitiveInt(in);
      this.ssl = DataSerializer.readPrimitiveBoolean(in);
      this.started = DataSerializer.readPrimitiveBoolean(in);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      boolean tmpJmxManager;
      String tmpHost;
      int tmpPort;
      boolean tmpSsl;
      boolean tmpStarted;
      synchronized(this) {
        tmpJmxManager = this.jmxManager;
        tmpHost = this.host;
        tmpPort = this.port;
        tmpSsl = this.ssl;
        tmpStarted = this.started;
      }
      super.toData(out);
      DataSerializer.writePrimitiveBoolean(tmpJmxManager, out);
      DataSerializer.writeString(tmpHost, out);
      DataSerializer.writePrimitiveInt(tmpPort, out);
      DataSerializer.writePrimitiveBoolean(tmpSsl, out);
      DataSerializer.writePrimitiveBoolean(tmpStarted, out);
    }
    
    @Override
    public int getDSFID() {
      return JMX_MANAGER_PROFILE; 
    }
  }

}
