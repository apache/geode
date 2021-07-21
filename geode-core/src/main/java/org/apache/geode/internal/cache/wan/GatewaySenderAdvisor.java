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
package org.apache.geode.internal.cache.wan;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.util.Gateway;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.UpdateAttributesProcessor;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.logging.internal.executors.LoggingThread;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class GatewaySenderAdvisor extends DistributionAdvisor {
  private static final Logger logger = LogService.getLogger();

  private DistributedLockService lockService;

  private volatile boolean isPrimary;

  private final Object primaryLock = new Object();

  private final String lockToken;

  private Thread lockObtainingThread;

  private final AbstractGatewaySender sender;

  private GatewaySenderAdvisor(DistributionAdvisee sender) {
    super(sender);
    this.sender = (AbstractGatewaySender) sender;
    lockToken = getDLockServiceName() + "-token";
  }

  public static GatewaySenderAdvisor createGatewaySenderAdvisor(DistributionAdvisee sender) {
    GatewaySenderAdvisor advisor = new GatewaySenderAdvisor(sender);
    advisor.initialize();
    return advisor;
  }

  public String getDLockServiceName() {
    return getClass().getName() + "_" + sender.getId();
  }

  public Thread getLockObtainingThread() {
    return lockObtainingThread;
  }

  /** Instantiate new Sender profile for this member */
  @Override
  protected Profile instantiateProfile(InternalDistributedMember memberId, int version) {
    return new GatewaySenderProfile(memberId, version);
  }

  /**
   * The profile will be created when the sender is added to the cache. here we are not starting the
   * sender. so we should not release or acquire any lock for the sender to become primary based on
   * creation only.
   */
  @Override
  public void profileCreated(Profile profile) {
    if (profile instanceof GatewaySenderProfile) {
      GatewaySenderProfile sp = (GatewaySenderProfile) profile;
      checkCompatibility(sp);
    }
  }

  private void checkCompatibility(GatewaySenderProfile sp) {
    if (sp.remoteDSId != sender.getRemoteDSId()) {
      throw new IllegalStateException(
          String.format(
              "Cannot create Gateway Sender %s with remote ds id %s because another cache has the same Gateway Sender defined with remote ds id %s.",
              sp.Id, sp.remoteDSId, sender.remoteDSId));
    }
    if (sp.isParallel && !sender.isParallel()) {
      throw new IllegalStateException(
          String.format(
              "Cannot create Gateway Sender %s as parallel gateway sender because another cache has the same sender as serial gateway sender",
              sp.Id));
    }
    if (!sp.isParallel && sender.isParallel()) {
      throw new IllegalStateException(
          String.format(
              "Cannot create Gateway Sender %s as serial gateway sender because another cache has the same sender as parallel gateway sender",
              sp.Id));
    }

    if (sp.isBatchConflationEnabled != sender.isBatchConflationEnabled()) {
      throw new IllegalStateException(
          String.format(
              "Cannot create Gateway Sender %s with isBatchConflationEnabled %s because another cache has the same Gateway Sender defined with isBatchConflationEnabled %s",
              sp.Id, sp.isBatchConflationEnabled,
              sender.isBatchConflationEnabled()));
    }
    if (sp.isPersistenceEnabled != sender.isPersistenceEnabled()) {
      throw new IllegalStateException(
          String.format(
              "Cannot create Gateway Sender %s with isPersistentEnabled %s because another cache has the same Gateway Sender defined with isPersistentEnabled %s",
              sp.Id, sp.isPersistenceEnabled, sender.isPersistenceEnabled()));
    }
    if (sp.alertThreshold != sender.getAlertThreshold()) {
      throw new IllegalStateException(
          String.format(
              "Cannot create Gateway Sender %s with alertThreshold %s because another cache has the same Gateway Sender defined with alertThreshold %s",
              sp.Id, sp.alertThreshold, sender.getAlertThreshold()));
    }
    if (!sender.isParallel()) {
      if (sp.manualStart != sender.isManualStart()) {
        throw new IllegalStateException(
            String.format(
                "Cannot create Gateway Sender %s with manual start %s because another cache has the same Gateway Sender defined with manual start %s",
                sp.Id, sp.manualStart, sender.isManualStart()));
      }
    }

    if (!sp.isParallel) {
      if (sp.orderPolicy != sender.getOrderPolicy()) {
        throw new IllegalStateException(
            String.format(
                "Cannot create Gateway Sender %s with orderPolicy %s because another cache has the same Gateway Sender defined with orderPolicy %s",
                sp.Id, sp.orderPolicy, sender.getOrderPolicy()));
      }
    }

    List<String> senderEventFilterClassNames = new ArrayList<>();
    for (org.apache.geode.cache.wan.GatewayEventFilter filter : sender.getGatewayEventFilters()) {
      senderEventFilterClassNames.add(filter.getClass().getName());
    }
    if (sp.eventFiltersClassNames.size() != senderEventFilterClassNames.size()) {
      throw new IllegalStateException(
          String.format(
              "Cannot create Gateway Sender %s with GatewayEventFilters %s because another cache has the same Gateway Sender defined with GatewayEventFilters %s",
              sp.Id, sp.eventFiltersClassNames, senderEventFilterClassNames));
    } else {
      for (String filterName : senderEventFilterClassNames) {
        if (!sp.eventFiltersClassNames.contains(filterName)) {
          throw new IllegalStateException(
              String.format(
                  "Cannot create Gateway Sender %s with GatewayEventFilters %s because another cache has the same Gateway Sender defined with GatewayEventFilters %s",
                  sp.Id, sp.eventFiltersClassNames, senderEventFilterClassNames));
        }
      }
    }

    Set<String> senderTransportFilterClassNames = new LinkedHashSet<>();
    for (GatewayTransportFilter filter : sender.getGatewayTransportFilters()) {
      senderTransportFilterClassNames.add(filter.getClass().getName());
    }
    if (sp.transFiltersClassNames.size() != senderTransportFilterClassNames.size()) {
      throw new IllegalStateException(
          String.format(
              "Cannot create Gateway Sender %s with GatewayTransportFilters %s because another cache has the same Gateway Sender defined with GatewayTransportFilters %s",
              sp.Id, sp.transFiltersClassNames, senderTransportFilterClassNames));
    } else {
      Iterator<String> i1 = sp.transFiltersClassNames.iterator();
      Iterator<String> i2 = senderTransportFilterClassNames.iterator();
      while (i1.hasNext() && i2.hasNext()) {
        if (!i1.next().equals(i2.next())) {
          throw new IllegalStateException(
              String.format(
                  "Cannot create Gateway Sender %s with GatewayTransportFilters %s because another cache has the same Gateway Sender defined with GatewayTransportFilters %s",
                  sp.Id, sp.transFiltersClassNames,
                  senderTransportFilterClassNames));
        }
      }
    }
    List<String> senderEventListenerClassNames = new ArrayList<>();
    for (AsyncEventListener listener : sender.getAsyncEventListeners()) {
      senderEventListenerClassNames.add(listener.getClass().getName());
    }
    if (sp.senderEventListenerClassNames.size() != senderEventListenerClassNames.size()) {
      throw new IllegalStateException(
          String.format(
              "Cannot create Gateway Sender %s with AsyncEventListeners %s because another cache has the same Gateway Sender defined with AsyncEventListener %s",
              sp.Id, sp.senderEventListenerClassNames,
              senderEventListenerClassNames));
    } else {
      for (String listenerName : senderEventListenerClassNames) {
        if (!sp.senderEventListenerClassNames.contains(listenerName)) {
          throw new IllegalStateException(
              String.format(
                  "Cannot create Gateway Sender %s with AsyncEventListeners %s because another cache has the same Gateway Sender defined with AsyncEventListener %s",
                  sp.Id, sp.senderEventListenerClassNames,
                  senderEventListenerClassNames));
        }
      }
    }
    if (sp.isDiskSynchronous != sender.isDiskSynchronous()) {
      throw new IllegalStateException(
          String.format(
              "Cannot create Gateway Sender %s with isDiskSynchronous %s because another cache has the same Gateway Sender defined with isDiskSynchronous %s",
              sp.Id, sp.isDiskSynchronous, sender.isDiskSynchronous()));
    }
    if (sp.getDistributedMember().getVersion().isNotOlderThan(KnownVersion.GEODE_1_14_0)) {
      if (sp.enforceThreadsConnectSameReceiver != sender.getEnforceThreadsConnectSameReceiver()) {
        throw new IllegalStateException(
            String.format(
                "Cannot create Gateway Sender %s with enforceThreadsConnectSameReceiver %s because another cache has the same Gateway Sender defined with enforceThreadsConnectSameReceiver %s",
                sp.Id, sp.enforceThreadsConnectSameReceiver,
                sender.getEnforceThreadsConnectSameReceiver()));
      }
    }
  }

  /**
   * If there is change in sender which having policy as primary. 1. If that sender is stopped then
   * if there are no other primary senders then this sender should volunteer for primary. 2. If this
   * sender is primary and its policy is secondary then this sender should release the lock so that
   * other primary sender which s waiting on lock will get the lock.
   */
  @Override
  public void profileUpdated(Profile profile) {
    if (profile instanceof GatewaySenderProfile) {
      GatewaySenderProfile sp = (GatewaySenderProfile) profile;
      if (!sp.isParallel) { // SerialGatewaySender
        if (!sp.isRunning) {
          if (advisePrimaryGatewaySender() != null) {
            return;
          }
          // IF this sender is not primary
          if (!sender.isPrimary()) {
            if (!adviseEldestGatewaySender()) {// AND this is not the eldest
                                               // sender
              if (logger.isDebugEnabled()) {
                logger.debug(
                    "Sender {} is not the eldest in the system. Giving preference to eldest sender to become primary...",
                    sender);
              }
              return;
            }
            launchLockObtainingVolunteerThread();
          }
        } else {
          if (sp.serverLocation != null) {
            sender.setServerLocation(sp.serverLocation);
          }
        }
      }
    }
  }

  /**
   * When the sender profile is removed, then check for the primary members if they are not
   * available then this secondary sender should volunteer for primary
   */
  @Override
  protected void profileRemoved(Profile profile) {
    if (profile instanceof GatewaySenderProfile) {
      GatewaySenderProfile sp = (GatewaySenderProfile) profile;
      if (!sp.isParallel) {// SerialGatewaySender
        // if there is a primary sender, then don't volunteer for primary
        if (advisePrimaryGatewaySender() != null) {
          return;
        }
        if (!sender.isPrimary()) {// IF this sender is not primary
          if (!adviseEldestGatewaySender()) {// AND this is not the eldest sender
            if (logger.isDebugEnabled()) {
              logger.debug(
                  "Sender {} is not the eldest in the system. Giving preference to eldest sender to become primary...",
                  sender);
            }
            return;
          }
          launchLockObtainingVolunteerThread();
        }
      }
    }
  }

  public boolean isPrimary() {
    return sender.isParallel() || isPrimary;
  }

  public void initDLockService() {
    InternalDistributedSystem ds = sender.getCache().getInternalDistributedSystem();
    String dlsName = getDLockServiceName();
    lockService = DistributedLockService.getServiceNamed(dlsName);
    if (lockService == null) {
      lockService = DLockService.create(dlsName, ds, true, true);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Obtained DistributedLockService: {}", this, lockService);
    }
  }

  public boolean volunteerForPrimary() {
    if (logger.isDebugEnabled()) {
      logger.debug("Sender : {} is volunteering for Primary ", sender.getId());
    }

    if (advisePrimaryGatewaySender() == null) {
      if (!adviseEldestGatewaySender()) {
        if (logger.isDebugEnabled()) {
          logger.debug(
              "Sender {} is not the eldest in the system. Giving preference to eldest sender to become primary...",
              sender);
        }
        return false;
      }
      if (logger.isDebugEnabled()) {
        logger.debug("Sender : {} no Primary available. So going to acquire distributed lock",
            sender);
      }
      lockService.lock(lockToken, 10000, -1);
      return lockService.isHeldByCurrentThread(lockToken);
    }
    return false;
  }

  /**
   * Find out if this sender is the eldest in the DS. Returns true if: 1. No other sender is running
   * 2. At least one sender is running in the system apart from this sender AND this sender's start
   * time is lesser of all (i.e. this sender is oldest)
   *
   * @return boolean true if this eldest sender; false otherwise
   */
  private boolean adviseEldestGatewaySender() {
    Profile[] snapshot = profiles;

    // sender with minimum startTime is eldest. Find out the minimum start time
    // of remote senders.
    TreeSet<Long> senderStartTimes = new TreeSet<>();
    for (Profile profile : snapshot) {
      GatewaySenderProfile sp = (GatewaySenderProfile) profile;
      if (!sp.isParallel && sp.isRunning) {
        senderStartTimes.add(sp.startTime);
      }
    }

    // if none of the remote senders is running, then this sender should
    // volunteer for primary
    // if there are remote senders running and this sender is not running then
    // it should give up
    // and allow existing running senders to volunteer
    return (senderStartTimes.isEmpty())
        || (sender.isRunning() && (sender.startTime <= senderStartTimes.first()));
  }

  public void makePrimary() {
    logger.info("{} : Starting as primary", sender);
    AbstractGatewaySenderEventProcessor eventProcessor = sender.getEventProcessor();
    if (eventProcessor != null) {
      eventProcessor.removeCacheListener();
    }

    logger.info("{} : Becoming primary gateway sender", sender);
    notifyAndBecomePrimary();
    new UpdateAttributesProcessor(sender).distribute(false);
  }

  public void notifyAndBecomePrimary() {
    synchronized (primaryLock) {
      setIsPrimary(true);
      notifyPrimaryLock();
    }
  }

  public void notifyPrimaryLock() {
    synchronized (primaryLock) {
      primaryLock.notifyAll();
    }
  }

  public void makeSecondary() {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Did not obtain the lock on {}. Starting as secondary gateway sender.",
          sender, lockToken);
    }

    // Set primary flag to false
    logger.info(
        "{} starting as secondary because primary gateway sender is available on member :{}",
        new Object[] {sender.getId(), advisePrimaryGatewaySender()});
    isPrimary = false;
    new UpdateAttributesProcessor(sender).distribute(false);
  }

  public void launchLockObtainingVolunteerThread() {
    String threadName = "Gateway Sender Primary Lock Acquisition Thread Volunteer";
    lockObtainingThread = new LoggingThread(threadName, () -> {
      sender.getLifeCycleLock().readLock().lock();
      try {
        // Attempt to obtain the lock
        if (!(sender.isRunning())) {
          return;
        }
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Obtaining the lock on {}", this, lockToken);
        }

        if (volunteerForPrimary()) {
          if (logger.isDebugEnabled()) {
            logger.debug("{}: Obtained the lock on {}", this,
                lockToken);
          }
          logger.info("{} is becoming primary gateway Sender.", this);

          // As soon as the lock is obtained, set primary
          makePrimary();
        }
      } catch (CancelException e) {
        // no action necessary
      } catch (Exception e) {
        if (!sender.getStopper().isCancelInProgress()) {
          logger.fatal(String.format(
              "%s: The thread to obtain the failover lock was interrupted. This gateway sender will never become the primary.",
              this),
              e);
        }
      } finally {
        sender.getLifeCycleLock().readLock().unlock();
      }
    });
    lockObtainingThread.start();
  }

  public void waitToBecomePrimary(AbstractGatewaySenderEventProcessor callingProcessor)
      throws InterruptedException {
    if (isPrimary()) {
      return;
    }
    synchronized (primaryLock) {
      logger.info("{} : Waiting to become primary gateway", sender.getId());
      while (!isPrimary()) {
        primaryLock.wait(1000);
        if (sender.getEventProcessor() != null && callingProcessor.isStopped()) {
          logger.info("The event processor is stopped, not to wait for being primary any more.");
          return;
        }
      }
    }
  }

  /**
   * Profile information for a remote counterpart.
   */
  public static class GatewaySenderProfile extends DistributionAdvisor.Profile {
    public String Id;

    public long startTime;

    public int remoteDSId;

    /**
     * I need this boolean to make sure the sender which is volunteer for primary is running. not
     * running sender should not become primary.
     */
    public boolean isRunning;

    public boolean isPrimary;

    public boolean isParallel;

    public boolean isBatchConflationEnabled;

    public boolean isPersistenceEnabled;

    public int alertThreshold;

    public boolean manualStart;

    public ArrayList<String> eventFiltersClassNames = new ArrayList<>();

    public ArrayList<String> transFiltersClassNames = new ArrayList<>();

    public ArrayList<String> senderEventListenerClassNames = new ArrayList<>();

    public boolean isDiskSynchronous;

    public int dispatcherThreads;

    public OrderPolicy orderPolicy;

    public ServerLocation serverLocation;

    public boolean enforceThreadsConnectSameReceiver = false;

    public GatewaySenderProfile(InternalDistributedMember memberId, int version) {
      super(memberId, version);
    }

    public GatewaySenderProfile() {}

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      fromDataPre_GEODE_1_14_0_0(in, context);
      enforceThreadsConnectSameReceiver = in.readBoolean();
    }

    public void fromDataPre_GEODE_1_14_0_0(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      Id = DataSerializer.readString(in);
      startTime = in.readLong();
      remoteDSId = in.readInt();
      isRunning = in.readBoolean();
      isPrimary = in.readBoolean();
      isParallel = in.readBoolean();
      isBatchConflationEnabled = in.readBoolean();
      isPersistenceEnabled = in.readBoolean();
      alertThreshold = in.readInt();
      manualStart = in.readBoolean();
      eventFiltersClassNames = DataSerializer.readArrayList(in);
      transFiltersClassNames = DataSerializer.readArrayList(in);
      senderEventListenerClassNames = DataSerializer.readArrayList(in);
      isDiskSynchronous = in.readBoolean();
      dispatcherThreads = in.readInt();
      if (StaticSerialization.getVersionForDataStream(in).isOlderThan(KnownVersion.GFE_90)) {
        Gateway.OrderPolicy oldOrderPolicy = DataSerializer.readObject(in);
        if (oldOrderPolicy != null) {
          if (oldOrderPolicy.name().equals(OrderPolicy.KEY.name())) {
            orderPolicy = OrderPolicy.KEY;
          } else if (oldOrderPolicy.name().equals(OrderPolicy.THREAD.name())) {
            orderPolicy = OrderPolicy.THREAD;
          } else {
            orderPolicy = OrderPolicy.PARTITION;
          }
        } else {
          orderPolicy = null;
        }
      } else {
        orderPolicy = DataSerializer.readObject(in);
      }
      boolean serverLocationFound = DataSerializer.readPrimitiveBoolean(in);
      if (serverLocationFound) {
        serverLocation = new ServerLocation();
        InternalDataSerializer.invokeFromData(serverLocation, in);
      }
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      toDataPre_GEODE_1_14_0_0(out, context);
      out.writeBoolean(enforceThreadsConnectSameReceiver);
    }

    public void toDataPre_GEODE_1_14_0_0(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeString(Id, out);
      out.writeLong(startTime);
      out.writeInt(remoteDSId);
      out.writeBoolean(isRunning);
      out.writeBoolean(isPrimary);
      out.writeBoolean(isParallel);
      out.writeBoolean(isBatchConflationEnabled);
      out.writeBoolean(isPersistenceEnabled);
      out.writeInt(alertThreshold);
      out.writeBoolean(manualStart);
      DataSerializer.writeArrayList(eventFiltersClassNames, out);
      DataSerializer.writeArrayList(transFiltersClassNames, out);
      DataSerializer.writeArrayList(senderEventListenerClassNames, out);
      out.writeBoolean(isDiskSynchronous);
      out.writeInt(dispatcherThreads);
      if (StaticSerialization.getVersionForDataStream(out).isOlderThan(KnownVersion.GFE_90)
          && orderPolicy != null) {
        String orderPolicyName = orderPolicy.name();
        if (orderPolicyName.equals(Gateway.OrderPolicy.KEY.name())) {
          DataSerializer.writeObject(Gateway.OrderPolicy.KEY, out);
        } else if (orderPolicyName.equals(Gateway.OrderPolicy.THREAD.name())) {
          DataSerializer.writeObject(Gateway.OrderPolicy.THREAD, out);
        } else {
          DataSerializer.writeObject(Gateway.OrderPolicy.PARTITION, out);
        }
      } else {
        DataSerializer.writeObject(orderPolicy, out);
      }
      boolean serverLocationFound = (serverLocation != null);
      DataSerializer.writePrimitiveBoolean(serverLocationFound, out);
      if (serverLocationFound) {
        InternalDataSerializer.invokeToData(serverLocation, out);
      }
    }

    @Immutable
    private static final KnownVersion[] serializationVersions =
        new KnownVersion[] {KnownVersion.GEODE_1_14_0};

    @Override
    public KnownVersion[] getSerializationVersions() {
      return serializationVersions;
    }

    @Override
    public int getDSFID() {
      return GATEWAY_SENDER_PROFILE;
    }

    @Override
    public void processIncoming(ClusterDistributionManager dm, String adviseePath,
        boolean removeProfile, boolean exchangeProfiles, final List<Profile> replyProfiles) {
      InternalCache cache = dm.getCache();
      if (cache != null) {
        AbstractGatewaySender sender = (AbstractGatewaySender) cache.getGatewaySender(adviseePath);
        handleDistributionAdvisee(sender, removeProfile, exchangeProfiles, replyProfiles);
      }
    }

    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      sb.append("; id=").append(Id);
      sb.append("; remoteDSName=").append(remoteDSId);
      sb.append("; isRunning=").append(isRunning);
      sb.append("; isPrimary=").append(isPrimary);
    }
  }

  public InternalDistributedMember advisePrimaryGatewaySender() {
    Profile[] snapshot = profiles;
    for (Profile profile : snapshot) {
      GatewaySenderProfile sp = (GatewaySenderProfile) profile;
      if (!sp.isParallel && sp.isPrimary) {
        return sp.getDistributedMember();
      }
    }
    return null;
  }

  public void setIsPrimary(boolean isPrimary) {
    this.isPrimary = isPrimary;
  }

  @Override
  public void close() {
    new UpdateAttributesProcessor(getAdvisee(), true).distribute(false);
    super.close();
  }
}
