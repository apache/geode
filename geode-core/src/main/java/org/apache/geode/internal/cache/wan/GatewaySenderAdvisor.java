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
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.asyncqueue.AsyncEventListener;
import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.cache.wan.GatewayTransportFilter;
import org.apache.geode.distributed.DistributedLockService;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.DistributionAdvisor;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.locks.DLockService;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.UpdateAttributesProcessor;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingThreadGroup;
import org.apache.geode.internal.logging.log4j.LocalizedMessage;

public class GatewaySenderAdvisor extends DistributionAdvisor {
  private static final Logger logger = LogService.getLogger();

  private DistributedLockService lockService;
  
  private volatile boolean isPrimary;
  
  private final Object primaryLock = new Object();
  
  private final String lockToken;
  
  private Thread lockObtainingThread;
  
  private final ThreadGroup threadGroup = 
    LoggingThreadGroup.createThreadGroup("GatewaySenderAdvisor Threads");
  
  private AbstractGatewaySender sender;
  
  private GatewaySenderAdvisor(DistributionAdvisee sender) {
    super(sender);
    this.sender = (AbstractGatewaySender)sender;
    this.lockToken = getDLockServiceName() + "-token";
  }
  
  public static GatewaySenderAdvisor createGatewaySenderAdvisor(DistributionAdvisee sender) {
    GatewaySenderAdvisor advisor = new GatewaySenderAdvisor(sender);
    advisor.initialize();
    return advisor;
  }

  public String getDLockServiceName(){
    return getClass().getName() + "_" + this.sender.getId();
  }
  
  public Thread getLockObtainingThread() {
    return this.lockObtainingThread;
  }
  /** Instantiate new Sender profile for this member */
  @Override
  protected Profile instantiateProfile(InternalDistributedMember memberId,
      int version) {
    return new GatewaySenderProfile(memberId, version);
  }

  /**
   * The profile will be created when the sender is added to the cache. here we
   * are not starting the sender. so we should not release or acquire any lock
   * for the sender to become primary based on creation only.
   */
  @Override
  public void profileCreated(Profile profile) {
    if (profile instanceof GatewaySenderProfile) {
      GatewaySenderProfile sp = (GatewaySenderProfile)profile;
      checkCompatibility(sp);
    }
  }

  private void checkCompatibility(GatewaySenderProfile sp) {
    if (sp.remoteDSId != sender.getRemoteDSId()) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_REMOTE_DS_ID_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_2_REMOTE_DS_ID
              .toString(new Object[] { sp.Id, sp.remoteDSId, sender.remoteDSId }));
    }
    if (sp.isParallel && !sender.isParallel()) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_AS_PARALLEL_GATEWAY_SENDER_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_AS_SERIAL_GATEWAY_SENDER
              .toString(new Object[] { sp.Id }));
    }
    if (!sp.isParallel && sender.isParallel()) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_AS_SERIAL_GATEWAY_SENDER_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_AS_PARALLEL_GATEWAY_SENDER
              .toString(new Object[] { sp.Id }));
    }

    if (sp.isBatchConflationEnabled != sender.isBatchConflationEnabled()) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_IS_BACTH_CONFLATION_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_IS_BATCH_CONFLATION_2
              .toString(new Object[] { sp.Id, sp.isBatchConflationEnabled,
                  sender.isBatchConflationEnabled() }));
    }
    if (sp.isPersistenceEnabled != sender.isPersistenceEnabled()) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_IS_PERSISTENT_ENABLED_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_IS_PERSISTENT_ENABLED_2
              .toString(new Object[] { sp.Id, sp.isPersistenceEnabled,
                  sender.isPersistenceEnabled() }));
    }
    if (sp.alertThreshold != sender.getAlertThreshold()) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_ALERT_THRESHOLD_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_ALERT_THRESHOLD_2
              .toString(new Object[] { sp.Id, sp.alertThreshold,
                  sender.getAlertThreshold() }));
    }
    if (!sender.isParallel()) {
      if (sp.manualStart != sender.isManualStart()) {
        throw new IllegalStateException(
            LocalizedStrings.GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_MANUAL_START_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_MANUAL_START_2
                .toString(new Object[] { sp.Id, sp.manualStart,
                    sender.isManualStart() }));
      }
    }
    /*if(sp.dispatcherThreads != sender.getDispatcherThreads()) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_DISPATCHER_THREAD_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_DISPATCHER_THREAD_2
              .toString(new Object[] { sp.Id, sp.dispatcherThreads,
                  sender.getDispatcherThreads() }));
    }*/
    
    if(!sp.isParallel) {
      if(sp.orderPolicy != sender.getOrderPolicy()) {
        throw new IllegalStateException(
            LocalizedStrings.GatewaySenderAdvisor_CANNOT_CREATE_GATEWAYSENDER_0_WITH_ORDER_POLICY_1_BECAUSE_ANOTHER_CACHE_HAS_THE_SAME_SENDER_WITH_ORDER_POLICY_2
                .toString(new Object[] { sp.Id, sp.orderPolicy,
                    sender.getOrderPolicy() }));
      }
    }

    List<String> senderEventFilterClassNames = new ArrayList<String>();
    for (org.apache.geode.cache.wan.GatewayEventFilter filter : sender
        .getGatewayEventFilters()) {
      senderEventFilterClassNames.add(filter.getClass().getName());
    }
    if (sp.eventFiltersClassNames.size() != senderEventFilterClassNames.size()) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderAdvisor_GATEWAY_EVENT_FILTERS_MISMATCH
              .toString(new Object[] { sp.Id, sp.eventFiltersClassNames,
                  senderEventFilterClassNames }));
    }
    else {
      for(String filterName : senderEventFilterClassNames){
        if(!sp.eventFiltersClassNames.contains(filterName)){
          throw new IllegalStateException(
              LocalizedStrings.GatewaySenderAdvisor_GATEWAY_EVENT_FILTERS_MISMATCH
                  .toString(new Object[] { sp.Id, sp.eventFiltersClassNames,
                      senderEventFilterClassNames }));
        }
      }
    }
    
    Set<String> senderTransportFilterClassNames = new LinkedHashSet<String>();
    for (GatewayTransportFilter filter : sender
        .getGatewayTransportFilters()) {
      senderTransportFilterClassNames.add(filter.getClass().getName());
    }
    if (sp.transFiltersClassNames.size() != senderTransportFilterClassNames.size()) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderAdvisor_GATEWAY_TRANSPORT_FILTERS_MISMATCH
              .toString(new Object[] { sp.Id, sp.transFiltersClassNames,
                  senderTransportFilterClassNames }));
    }
    else {
      Iterator<String> i1 = sp.transFiltersClassNames.iterator();
      Iterator<String> i2 = senderTransportFilterClassNames.iterator();
      while(i1.hasNext() && i2.hasNext()){
        if(!i1.next().equals(i2.next())){
          throw new IllegalStateException(
              LocalizedStrings.GatewaySenderAdvisor_GATEWAY_TRANSPORT_FILTERS_MISMATCH
                  .toString(new Object[] { sp.Id, sp.transFiltersClassNames,
                      senderTransportFilterClassNames }));    
        }
      }
    }
    List<String> senderEventListenerClassNames = new ArrayList<String>();
    for (AsyncEventListener listener : sender
        .getAsyncEventListeners()) {
      senderEventListenerClassNames.add(listener.getClass().getName());
    }
    if (sp.senderEventListenerClassNames.size() != senderEventListenerClassNames.size()) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderAdvisor_GATEWAY_SENDER_LISTENER_MISMATCH
              .toString(new Object[] { sp.Id, sp.senderEventListenerClassNames,
                  senderEventListenerClassNames }));
    }
    else {
      for(String listenerName : senderEventListenerClassNames){
        if(!sp.senderEventListenerClassNames.contains(listenerName)){
          throw new IllegalStateException(
              LocalizedStrings.GatewaySenderAdvisor_GATEWAY_SENDER_LISTENER_MISMATCH
                  .toString(new Object[] { sp.Id, sp.senderEventListenerClassNames,
                      senderEventListenerClassNames }));
        }
      }
    }
    if (sp.isDiskSynchronous != sender.isDiskSynchronous()) {
      throw new IllegalStateException(
          LocalizedStrings.GatewaySenderAdvisor_GATEWAY_SENDER_IS_DISK_SYNCHRONOUS_MISMATCH
              .toString(new Object[] { sp.Id, sp.isDiskSynchronous,
                  sender.isDiskSynchronous() }));
    }
  }

  /**
   * If there is change in sender which having policy as primary.
   * 1. If that sender is stopped then if there are no other primary senders then this sender should volunteer for primary.
   * 2. If this sender is primary and its policy is secondary then this sender should release the lock so that other primary sender which s waiting on lock will get the lock.
   * 
   */

  @Override
  public void profileUpdated(Profile profile) {
    if (profile instanceof GatewaySenderProfile) {
      GatewaySenderProfile sp = (GatewaySenderProfile)profile;
      if (!sp.isParallel) { // SerialGatewaySender
        if (!sp.isRunning) {
          if (advisePrimaryGatewaySender() != null) {
            return;
          }
          // IF this sender is not primary
          if (!this.sender.isPrimary()) {
            if (!adviseEldestGatewaySender()) {// AND this is not the eldest
                                               // sender
              if (logger.isDebugEnabled()) {
                logger.debug("Sender {} is not the eldest in the system. Giving preference to eldest sender to become primary...", this.sender);
              }
              return;
            }
            launchLockObtainingVolunteerThread();
          }
        } else {
          if (sp.serverLocation != null) {
            this.sender.setServerLocation(sp.serverLocation);
          }
        }
      }
    }
  }

  /**
   * When the sender profile is removed, 
   * then check for the primary members if they are not available then this secondary sender should volunteer for primary
   */
  @Override
  protected void profileRemoved(Profile profile) {
    if (profile instanceof GatewaySenderProfile) {
      GatewaySenderProfile sp = (GatewaySenderProfile)profile;
      if (!sp.isParallel) {// SerialGatewaySender
    	//if there is a primary sender, then don't volunteer for primary 
        if (advisePrimaryGatewaySender() != null) {
          return;
        }
        if (!this.sender.isPrimary()) {//IF this sender is not primary
          if (!adviseEldestGatewaySender()) {//AND this is not the eldest sender 
        	if (logger.isDebugEnabled()) {
        	  logger.debug("Sender {} is not the eldest in the system. Giving preference to eldest sender to become primary...", this.sender);
        	}
        	return;
          }
          launchLockObtainingVolunteerThread();
        }
      }
    }
  }
  
  public boolean isPrimary() {
    return sender.isParallel() || this.isPrimary;
  }

  public void initDLockService() {
    InternalDistributedSystem ds = ((GemFireCacheImpl)this.sender.getCache())
        .getDistributedSystem();
    String dlsName = getDLockServiceName();
    this.lockService = DistributedLockService.getServiceNamed(dlsName);
    if (this.lockService == null) {
      this.lockService = DLockService.create(dlsName, ds, true, true, true);
    }
    Assert.assertTrue(this.lockService != null);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Obtained DistributedLockService: {}", this, this.lockService);
    }
  }
  
  public boolean volunteerForPrimary() {
    if (logger.isDebugEnabled()) {
      logger.debug("Sender : {} is volunteering for Primary ", this.sender.getId());
    }

    if (advisePrimaryGatewaySender() == null) {
        if (!adviseEldestGatewaySender()) {
           	if (logger.isDebugEnabled()) {
           	  logger.debug("Sender {} is not the eldest in the system. Giving preference to eldest sender to become primary...", this.sender);
           	}
           	return false;
        }
      if (logger.isDebugEnabled()) {
        logger.debug("Sender : {} no Primary available. So going to acquire distributed lock", this.sender);
      }
      this.lockService.lock(this.lockToken, 10000, -1);
      return this.lockService.isHeldByCurrentThread(this.lockToken);
    }
    return false;
  }

  /**
   * Find out if this sender is the eldest in the DS. Returns true if: 1. No
   * other sender is running 2. At least one sender is running in the system
   * apart from this sender AND this sender's start time is lesser of all (i.e.
   * this sender is oldest)
   * 
   * @return boolean true if this eldest sender; false otherwise
   */
  private boolean adviseEldestGatewaySender() {
    Profile[] snapshot = this.profiles;

    // sender with minimum startTime is eldest. Find out the minimum start time
    // of remote senders.
    TreeSet<Long> senderStartTimes = new TreeSet<Long>();
    for (Profile profile : snapshot) {
      GatewaySenderProfile sp = (GatewaySenderProfile)profile;
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
        || (this.sender.isRunning() && (this.sender.startTime <= senderStartTimes
            .first()));
  }
  
  private InternalDistributedMember adviseEldestGatewaySenderNode() {
    Profile[] snapshot = this.profiles;

    // sender with minimum startTime is eldest. Find out the minimum start time
    // of remote senders.
    InternalDistributedMember node = null;
    GatewaySenderProfile eldestProfile = null;
    for (Profile profile : snapshot) {
      GatewaySenderProfile sp = (GatewaySenderProfile)profile;
      if (!sp.isParallel && sp.isRunning) {
        if (eldestProfile == null) {
          eldestProfile = sp;
        }
        if (sp.startTime < eldestProfile.startTime) {
          eldestProfile = sp;
        }
      }
    }

    if (eldestProfile != null) {
      node = eldestProfile.getDistributedMember();
    }
    return node;
  }
  
  public void makePrimary() {
    logger.info(LocalizedMessage.create(LocalizedStrings.SerialGatewaySenderImpl_0__STARTING_AS_PRIMARY, this.sender));
    AbstractGatewaySenderEventProcessor eventProcessor = this.sender.getEventProcessor();
    if (eventProcessor != null) {
      eventProcessor.removeCacheListener();
    }
    
    synchronized (this.primaryLock) {
      this.isPrimary = true;
      logger.info(LocalizedMessage.create(LocalizedStrings.SerialGatewaySenderImpl_0__BECOMING_PRIMARY_GATEWAYSENDER, this.sender));
      this.primaryLock.notifyAll();
    }
    new UpdateAttributesProcessor(this.sender).distribute(false);
  }
  
  public void makeSecondary() {
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Did not obtain the lock on {}. Starting as secondary gateway sender.", this.sender, this.lockToken);
    }

    // Set primary flag to false
    logger.info(LocalizedMessage.create(
        LocalizedStrings.SerialGatewaySenderImpl_0__STARTING_AS_SECONDARY_BECAUSE_PRIMARY_GATEWAY_SENDER_IS_AVAIALABLE_ON_MEMBER_2,
        new Object[] {this.sender.getId(), advisePrimaryGatewaySender()}));
    this.isPrimary = false;
    new UpdateAttributesProcessor(this.sender).distribute(false);
  }
  
  public void launchLockObtainingVolunteerThread() {
    this.lockObtainingThread = new Thread(threadGroup, new Runnable() {
      @SuppressWarnings("synthetic-access")
      public void run() {
        GatewaySenderAdvisor.this.sender.getLifeCycleLock()
                    .readLock().lock(); 
          try {
            // Attempt to obtain the lock
            if (!(GatewaySenderAdvisor.this.sender
                .isRunning())) {
              return;
            }
            if (logger.isDebugEnabled()) {
              logger.debug("{}: Obtaining the lock on {}", this, GatewaySenderAdvisor.this.lockToken);
            }

            if (volunteerForPrimary()) {
              if (logger.isDebugEnabled()) {
                logger.debug("{}: Obtained the lock on {}", this, GatewaySenderAdvisor.this.lockToken);
              }
              logger.info(LocalizedMessage.create(LocalizedStrings.GatewaySender_0_IS_BECOMING_PRIMARY_GATEWAY_Sender,
                  GatewaySenderAdvisor.this));

              // As soon as the lock is obtained, set primary
              GatewaySenderAdvisor.this.makePrimary();
            }
          }
          catch (CancelException e) {
            // no action necessary
          }
          catch (Exception e) {
            if (!sender.getStopper().isCancelInProgress()) {
              logger.fatal(LocalizedMessage.create(
                  LocalizedStrings.GatewaySenderAdvisor_0_THE_THREAD_TO_OBTAIN_THE_FAILOVER_LOCK_WAS_INTERRUPTED__THIS_GATEWAY_SENDER_WILL_NEVER_BECOME_THE_PRIMARY,
                  GatewaySenderAdvisor.this), e);
            }
          }
          finally{
            GatewaySenderAdvisor.this.sender.getLifeCycleLock()
                          .readLock().unlock();
        }
      }
    }, "Gateway Sender Primary Lock Acquisition Thread Volunteer");

    this.lockObtainingThread.setDaemon(true);
    this.lockObtainingThread.start();
  }

  public void waitToBecomePrimary() throws InterruptedException
  {
    if (isPrimary()) {
      return;
    }
    synchronized (this.primaryLock) {
      while (!isPrimary()) {
        logger.info(LocalizedMessage.create(LocalizedStrings.GatewayImpl_0__WAITING_TO_BECOME_PRIMARY_GATEWAY, this.sender.getId()));
        this.primaryLock.wait();
      }
    }
  }
  /**
   * Profile information for a remote counterpart.
   */
  public static final class GatewaySenderProfile extends DistributionAdvisor.Profile {
    public String Id;

    public long startTime;
    
    public int remoteDSId;
    
    /**
     * I need this boolean to make sure the sender which is volunteer for primary is running. not running sender should not become primary. 
     */
    public boolean isRunning;
    
    public boolean isPrimary;
    
    public boolean isParallel;
    
    public boolean isBatchConflationEnabled;

    public boolean isPersistenceEnabled;

    public int alertThreshold;

    public boolean manualStart;

    public ArrayList<String> eventFiltersClassNames = new ArrayList<String>();

    public ArrayList<String> transFiltersClassNames = new ArrayList<String>();

    public ArrayList<String> senderEventListenerClassNames = new ArrayList<String>();
    
    public boolean isDiskSynchronous; 
    
    public int dispatcherThreads;
    
    public OrderPolicy orderPolicy;
    
    public ServerLocation serverLocation;
    
    public GatewaySenderProfile(InternalDistributedMember memberId, int version) {
      super(memberId, version);
    }

    public GatewaySenderProfile() {
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      this.Id = DataSerializer.readString(in);
      this.startTime = in.readLong();
      this.remoteDSId = in.readInt();
      this.isRunning = in.readBoolean();
      this.isPrimary = in.readBoolean();
      this.isParallel = in.readBoolean();
      this.isBatchConflationEnabled = in.readBoolean();
      this.isPersistenceEnabled = in.readBoolean();
      this.alertThreshold = in.readInt();
      this.manualStart = in.readBoolean();
      this.eventFiltersClassNames = DataSerializer.readArrayList(in);
      this.transFiltersClassNames = DataSerializer.readArrayList(in);
      this.senderEventListenerClassNames = DataSerializer.readArrayList(in);
      this.isDiskSynchronous = in.readBoolean();
      this.dispatcherThreads = in.readInt();
      if (InternalDataSerializer.getVersionForDataStream(in).compareTo(
          Version.CURRENT) < 0) {
        org.apache.geode.cache.util.Gateway.OrderPolicy oldOrderPolicy = DataSerializer
            .readObject(in);
        if (oldOrderPolicy != null) {
          if (oldOrderPolicy.name().equals(OrderPolicy.KEY.name())) {
            this.orderPolicy = OrderPolicy.KEY;
          }
          else if (oldOrderPolicy.name().equals(OrderPolicy.THREAD.name())) {
            this.orderPolicy = OrderPolicy.THREAD;
          }
          else {
            this.orderPolicy = OrderPolicy.PARTITION;
          }}
        else {
          this.orderPolicy = null;
        }
      }
      else {
        this.orderPolicy = DataSerializer.readObject(in);
      }
      boolean serverLocationFound = DataSerializer.readPrimitiveBoolean(in);
      if (serverLocationFound) {
          this.serverLocation = new ServerLocation();
          InternalDataSerializer.invokeFromData(this.serverLocation, in);
      }
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
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
      if (InternalDataSerializer.getVersionForDataStream(out).compareTo(
          Version.CURRENT) < 0 && this.orderPolicy != null) {
        String orderPolicyName = this.orderPolicy.name();
        if (orderPolicyName.equals(org.apache.geode.cache.util.Gateway.OrderPolicy.KEY.name())) {
          DataSerializer.writeObject(
              org.apache.geode.cache.util.Gateway.OrderPolicy.KEY, out);
        }
        else if(orderPolicyName.equals(org.apache.geode.cache.util.Gateway.OrderPolicy.THREAD.name())) {
          DataSerializer.writeObject(
              org.apache.geode.cache.util.Gateway.OrderPolicy.THREAD, out);
        }else{
          DataSerializer.writeObject(
              org.apache.geode.cache.util.Gateway.OrderPolicy.PARTITION, out);
        }
      }
      else {
        DataSerializer.writeObject(orderPolicy, out);
      }
      boolean serverLocationFound = (this.serverLocation != null);
      DataSerializer.writePrimitiveBoolean(serverLocationFound, out);
      if(serverLocationFound) {
        InternalDataSerializer.invokeToData(serverLocation, out);
      }
    }

    public void fromDataPre_GFE_8_0_0_0(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      this.Id = DataSerializer.readString(in);
      this.startTime = in.readLong();
      this.remoteDSId = in.readInt();
      this.isRunning = in.readBoolean();
      this.isPrimary = in.readBoolean();
      this.isParallel = in.readBoolean();
      this.isBatchConflationEnabled = in.readBoolean();
      this.isPersistenceEnabled = in.readBoolean();
      this.alertThreshold = in.readInt();
      this.manualStart = in.readBoolean();
      this.eventFiltersClassNames = DataSerializer.readArrayList(in);
      this.transFiltersClassNames = DataSerializer.readArrayList(in);
      this.senderEventListenerClassNames = DataSerializer.readArrayList(in);
      this.isDiskSynchronous = in.readBoolean();
      this.dispatcherThreads = in.readInt();
      this.orderPolicy = DataSerializer.readObject(in);
      boolean serverLocationFound = DataSerializer.readPrimitiveBoolean(in);
      if (serverLocationFound) {
          this.serverLocation = new ServerLocation();
          InternalDataSerializer.invokeFromData(this.serverLocation, in);
      }
    }

    public void toDataPre_GFE_8_0_0_0(DataOutput out) throws IOException {
      super.toData(out);
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
      //out.writeInt(dispatcherThreads);
      if(isParallel)
        out.writeInt(1);//it was 1 on previous version of gemfire
      else if (orderPolicy == null)
        out.writeInt(1);//it was 1 on previous version of gemfire
      else
        out.writeInt(dispatcherThreads);

      if(isParallel)
        DataSerializer.writeObject(null, out);
      else
        DataSerializer.writeObject(orderPolicy, out);
      
      boolean serverLocationFound = (this.serverLocation != null);
      DataSerializer.writePrimitiveBoolean(serverLocationFound, out);
      if(serverLocationFound) {
        InternalDataSerializer.invokeToData(serverLocation, out);
      }
    }
    
    private final static Version[] serializationVersions = new Version[] {Version.GFE_80}; 
    
    @Override
    public Version[] getSerializationVersions() {
      return serializationVersions;
    }
    
    @Override
    public int getDSFID() {
      return GATEWAY_SENDER_PROFILE;
    }

    @Override
    public void processIncoming(DistributionManager dm, String adviseePath,
        boolean removeProfile, boolean exchangeProfiles,
        final List<Profile> replyProfiles) {
      Cache cache = GemFireCacheImpl.getInstance();
      if (cache != null) {
        AbstractGatewaySender sender = (AbstractGatewaySender)((GemFireCacheImpl)cache)
            .getGatewaySender(adviseePath);
        handleDistributionAdvisee(sender, removeProfile, exchangeProfiles,
            replyProfiles);
      }
    }
    
    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      sb.append("; id=" + this.Id);
      sb.append("; remoteDSName=" + this.remoteDSId);
      sb.append("; isRunning=" + this.isRunning);
      sb.append("; isPrimary=" + this.isPrimary);
      
    }
  }

  public InternalDistributedMember advisePrimaryGatewaySender() {
    Profile[] snapshot = this.profiles;
    for (Profile profile : snapshot) {
      GatewaySenderProfile sp = (GatewaySenderProfile)profile;
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
    new UpdateAttributesProcessor(this.getAdvisee(), true).distribute(false);
    super.close();
  }
}
