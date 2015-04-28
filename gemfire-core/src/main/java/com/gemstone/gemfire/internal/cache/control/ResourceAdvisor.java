/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache.control;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.HighPriorityDistributionMessage;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.UpdateAttributesProcessor;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.Thresholds;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * The advisor associated with a {@link ResourceManager}.  Allows knowledge of
 * remote {@link ResourceManager} state and distribution of local {@link ResourceManager} state.
 * 
 * @author Mitch Thomas
 * @since 6.0
 */
public class ResourceAdvisor extends DistributionAdvisor {
  private static final Logger logger = LogService.getLogger();

  /**
   * Message used to push event updates to remote VMs
   */
  public static class ResourceProfileMessage extends
      HighPriorityDistributionMessage {
    private volatile ResourceManagerProfile[] profiles;
    private volatile int processorId;

    /**
     * Default constructor used for de-serialization (used during receipt)
     */
    public ResourceProfileMessage() {}

    /**
     * Constructor used to send
     * @param recips
     * @param ps
     */
    private ResourceProfileMessage(final Set<InternalDistributedMember> recips,
        final ResourceManagerProfile[] ps) {
      setRecipients(recips);
      this.processorId = 0;
      this.profiles = ps;
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.distributed.internal.DistributionMessage#process(com.gemstone.gemfire.distributed.internal.DistributionManager)
     */
    @Override
    protected void process(DistributionManager dm) {
      Throwable thr = null;
      ResourceManagerProfile p = null;
      try {
        final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        if (cache != null && !cache.isClosed()) {
          final ResourceAdvisor ra = cache.getResourceManager().getResourceAdvisor();
          if (this.profiles != null) {
            // Early reply to avoid waiting for the following putProfile call
            // to fire (remote) listeners so that the origin member can proceed with
            // firing its (local) listeners

            for (int i=0; i<this.profiles.length; i++) {
              p = this.profiles[i];
              ra.putProfile(p);
            }
          }
        } else {
          if (logger.isDebugEnabled()) {
            logger.debug("No cache: {}", this);
          }
        }
      } catch (CancelException e) {
        if (logger.isDebugEnabled()) {
          logger.debug("Cache closed: {}", this);
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
          logger.info(LocalizedMessage.create(
              LocalizedStrings.ResourceAdvisor_MEMBER_CAUGHT_EXCEPTION_PROCESSING_PROFILE,
              new Object[] {p, toString()}), thr);
        }
      }
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
     */
    public int getDSFID() {
      return RESOURCE_PROFILE_MESSAGE;
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      final int l = in.readInt();
      if (l != -1) {
        this.profiles = new ResourceManagerProfile[l];
        for (int i=0; i<this.profiles.length; i++) {
          final ResourceManagerProfile r = new ResourceManagerProfile();
          InternalDataSerializer.invokeFromData(r, in);
          this.profiles[i] = r;
        }
      } else {
        this.profiles = null;
      }
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      if (this.profiles != null) {
        out.writeInt(this.profiles.length);
        for (int i=0; i<this.profiles.length; i++) {
          InternalDataSerializer.invokeToData(this.profiles[i], out);
        }
      } else {
        out.writeInt(-1);
      }
    }

    /**
     * Send profiles to the provided members
     * @param irm The resource manager which is requesting distribution
     * @param recips The recipients of the message
     * @param profiles one or more profiles to send in this message
     * @throws InterruptedException
     * @throws ReplyException
     */
    public static void send(final InternalResourceManager irm, Set<InternalDistributedMember> recips,
        ResourceManagerProfile[] profiles) {
      final DM dm = irm.getResourceAdvisor().getDistributionManager();
      ResourceProfileMessage r = new ResourceProfileMessage(recips, profiles);
      dm.putOutgoing(r);
    }

    @Override
    public String getShortClassName() {
      return "ResourceProfileMessage";
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(getShortClassName())
      .append(" (processorId=").append(this.processorId)
      .append("; profiles=[");
      for (int i=0; i<this.profiles.length; i++) {
        sb.append(this.profiles[i]);
        if (i < this.profiles.length-1) {
          sb.append(", ");
        }
      }
      sb.append("]");
      sb.append(")");
      return sb.toString();
    }
  }

  /**
   * @param advisee The owner of this advisor 
   * @see ResourceManager
   */
  private ResourceAdvisor(DistributionAdvisee advisee) {
    super(advisee);
  }
  
  public static ResourceAdvisor createResourceAdvisor(DistributionAdvisee advisee) {
    ResourceAdvisor advisor = new ResourceAdvisor(advisee);
    advisor.initialize();
    return advisor;
  }
  
  @Override
  protected Profile instantiateProfile(InternalDistributedMember memberId,
      int version) {
    return new ResourceManagerProfile(memberId, version);
  }
  
  private InternalResourceManager getResourceManager() {
    return ((GemFireCacheImpl) getAdvisee()).getResourceManager(false);
  }

  @Override
  protected boolean evaluateProfiles(final Profile newProfile,
      final Profile oldProfile) {
    // Evaluate profiles before adding them to the ResourceAdvisor so that we
    // can deliver the events for each type, yet forget those which are DISABLED,
    // except the very first profile.  It is necessary to forget the DISABLED
    // state in this scenario:
    // 1) CRITICAL_UP, 2) CRITICAL_DISABLED, 3) EVICTION_DOWN
    // to make it possible to deliver the EVICTION_DOWN event.
    ResourceManagerProfile newp = (ResourceManagerProfile) newProfile;
    ResourceManagerProfile oldp = (ResourceManagerProfile) oldProfile;
    final MemoryEventImpl oldEvent;
    if (oldp != null) {
      oldEvent = oldp.getMemoryEvent();
    } else {
      oldEvent = generateMemoryEventUnknown(newp.getDistributedMember());
    }
    MemoryEventImpl newEvent = newp.getMemoryEvent();
    boolean delivered = getResourceManager().informListenersOfRemoteEvent(newEvent, oldEvent);

    if (oldp != null && (!delivered || newEvent.isDisableEvent())) {
      // Erase new profile state info, keep the new profile version.
      MemoryEventImpl ome = oldp.getMemoryEvent();
      newp.setEventState(ome.getCurrentHeapUsagePercent(), ome.getCurrentHeapBytesUsed(),
          ome.getType(), ome.getThresholds());
      // Remembering the new profile version is essential in handling race conditions e.g.
      // Sender:  1) Critical UP (v1), 2) Critical DOWN (v2)
      // Recipient:  1) Critical DOWN (v2), 2 Critical UP (v1)
    }
    return true;
  }

  /**
   * Construct a new MemoryEventImpl with {@link MemoryEventType#UNKNOWN} type
   * and the provided member id. 
   * @param distributedMember the member the memory event should take
   * @return the new MemoryEventImpl instance
   */
  private static MemoryEventImpl generateMemoryEventUnknown(
      InternalDistributedMember distributedMember) {
    return new MemoryEventImpl(MemoryEventType.UNKNOWN, distributedMember,
                               0, 0, 0, false, new Thresholds(0, false, 0, false, 0, false));
  }

  @Override
  public String toString() {
    return new StringBuilder().append("ResourceAdvisor for ResourceManager " 
        + getAdvisee()).toString();
  }
  
  /**
   * Profile which shares state with other ResourceManagers.
   * The data available in this profile should be enough to 
   * deliver a {@link MemoryEventImpl} for any of the CRITICAL {@link MemoryEventType}s 
   * @author Mitch Thomas
   * @since 6.0
   */
  public static class ResourceManagerProfile extends Profile {
    
    //Resource manager related fields
    private int currentHeapUsagePercent;
    private long currentHeapBytesUsed;
    private MemoryEventType type;
    private Thresholds thresholds;
    
    // Constructor for de-serialization
    public ResourceManagerProfile() {}

    // Constructor for sending purposes
    public ResourceManagerProfile(InternalDistributedMember memberId,
        int version) {
      super(memberId, version);
    }
    
    public synchronized ResourceManagerProfile setEventState(int currentHeapUsagePercent,
        long currentHeapBytesUsed, MemoryEventType type, Thresholds thr) {
      this.currentHeapUsagePercent = currentHeapUsagePercent;
      this.currentHeapBytesUsed = currentHeapBytesUsed;
      this.type = type;
      this.thresholds = thr;
      return this;
    }

    /**
     * @return a new memory event derived from the state of this profile
     */
    public synchronized MemoryEventImpl getMemoryEvent() {
      return new MemoryEventImpl(this.type, getDistributedMember(),
          this.currentHeapUsagePercent, this.currentHeapBytesUsed, 0, false, this.thresholds);
    }
    
    /**
     * Used to process incoming Resource Manager profiles. A reply is expected
     * to contain a profile with state of the local Resource Manager.
     * 
     * @since 6.0
     */
    @Override
    public void processIncoming(DistributionManager dm, String adviseePath,
        boolean removeProfile, boolean exchangeProfiles,
        final List<Profile> replyProfiles) {
      final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
      if (cache != null && !cache.isClosed()) {
        handleDistributionAdvisee(cache, removeProfile,
            exchangeProfiles, replyProfiles);
      }
    }

    @Override
    public StringBuilder getToStringHeader() {
      return new StringBuilder("ResourceAdvisor.ResourceManagerProfile");
    }

    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      synchronized (this) {
        sb.append("; state=").append(this.type)
        .append("; currentHeapUsagePercent=").append(this.currentHeapUsagePercent)
        .append("; currentHeapBytesUsed=").append(this.currentHeapBytesUsed);
      }
    }
    
    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      final int chup = in.readInt();
      final long chbu = in.readLong();
      MemoryEventType s = MemoryEventType.fromData(in);
      Thresholds t = Thresholds.fromData(in);
      setEventState(chup, chbu, s, t);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      final int chup;  final long chbu; final MemoryEventType s; final Thresholds t;
      synchronized(this) {
        chup = this.currentHeapUsagePercent;
        chbu = this.currentHeapBytesUsed;
        s = this.type;
        t = this.thresholds;
      }
      super.toData(out);
      out.writeInt(chup);
      out.writeLong(chbu);
      s.toData(out);
      t.toData(out);
    }
    
    @Override
    public int getDSFID() {
      return RESOURCE_MANAGER_PROFILE; 
    }
    
    public synchronized MemoryEventType getType() {
      return this.type;
    }
  }

  /**
   * Get set of members whose {@linkplain ResourceManager#setCriticalHeapPercentage(float)
   * critical heap threshold} has been met or exceeded.  The set does not include the local VM.
   * The mutability of this set only effects the elements in the set, not the state
   * of the members.
   * @return a mutable set of members in the critical state otherwise {@link Collections#EMPTY_SET}
   */
  public Set<InternalDistributedMember> adviseCritialMembers() {
    return adviseFilter(new Filter() {
      public boolean include(Profile profile) {
        ResourceManagerProfile rmp = (ResourceManagerProfile)profile;
        return rmp.getType().isCriticalUp();
      }});
  }
  
  /**
   * @param eventsToDeliver
   */
  public void informRemoteManagers(final MemoryEventImpl[] eventsToDeliver) {
    Set<InternalDistributedMember> recips = adviseGeneric();

    final ResourceManagerProfile[] ps = new ResourceManagerProfile[eventsToDeliver.length];
    for (int i=0; i<eventsToDeliver.length; i++) {
      MemoryEventImpl e = eventsToDeliver[i];
      ResourceManagerProfile rmp = new ResourceManagerProfile(getDistributionManager().getId(), incrementAndGetVersion());
      ps[i] = rmp.setEventState(e.getCurrentHeapUsagePercent(), e.getCurrentHeapBytesUsed(), e.getType(), e.getThresholds());
    }
    ResourceProfileMessage.send(getResourceManager(), recips, ps);
  }

  @Override
  protected void profileRemoved(Profile profile) {
    ResourceManagerProfile oldp = (ResourceManagerProfile) profile;
    MemoryEventImpl event = new MemoryEventImpl(oldp.getMemoryEvent(),
        MemoryEventType.CRITICAL_DISABLED);
    getResourceManager().informListenersOfRemoteEvent(event, oldp.getMemoryEvent());
  }

  @Override
  public void close() {
    new UpdateAttributesProcessor(this.getAdvisee(), true).distribute(false);
    super.close();
  }
}
