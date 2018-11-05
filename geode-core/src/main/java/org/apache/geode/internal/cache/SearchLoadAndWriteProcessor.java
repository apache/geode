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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.logging.log4j.Logger;

import org.apache.geode.CancelCriterion;
import org.apache.geode.CancelException;
import org.apache.geode.DataSerializer;
import org.apache.geode.GemFireException;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.CacheEvent;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionDestroyedException;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.HighPriorityDistributionMessage;
import org.apache.geode.distributed.internal.MembershipListener;
import org.apache.geode.distributed.internal.PooledDistributionMessage;
import org.apache.geode.distributed.internal.ProcessorKeeper21;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.distributed.internal.SerialDistributionMessage;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.versions.DiskVersionTag;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.Releasable;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;

/**
 * Implementation for distributed search, load and write operations in the GemFire system. Provides
 * an API for doing netSearch, netLoad, netSearchAndLoad and netWrite. The class uses the
 * DistributionAdvisor to route requests to the appropriate members. It also uses the
 * DistributionAdvisor to get region scope and applies rules based on the scope. It makes uses of
 * intelligent acceptors that allow netSearch to happen as a one phase operation at all
 * times.netLoad happens as a one phase operation in all cases except where the scope is GLOBAL At
 * the receiving end, the request is converted into an appropriate message whose process method
 * responds to the request.
 */
public class SearchLoadAndWriteProcessor implements MembershipListener {
  private static final Logger logger = LogService.getLogger();

  public static final int SMALL_BLOB_SIZE =
      Integer.getInteger("DistributionManager.OptimizedUpdateByteLimit", 2000);

  static final long RETRY_TIME =
      Long.getLong(DistributionConfig.GEMFIRE_PREFIX + "search-retry-interval", 2000);

  private volatile InternalDistributedMember selectedNode;
  private boolean selectedNodeDead = false;
  private int timeout;
  private boolean netSearchDone = false;
  private boolean netLoadDone = false;
  private long lastModified = 0;
  protected int processorId;
  private Object aCallbackArgument;
  private String regionName;
  private Object key;
  protected LocalRegion region;
  private Object result = null;
  private boolean isSerialized = false; // is result serialized?
  private CacheDistributionAdvisor advisor = null;
  protected Exception remoteException = null;
  public DistributionManager distributionManager = null;
  private volatile boolean requestInProgress = false;
  private boolean remoteGetInProgress = false;
  private volatile boolean authorative = false;
  /** remoteLoadInProgress is volatile to make sure response threads see the current value */
  private volatile boolean remoteLoadInProgress = false;
  private static final ProcessorKeeper21 processorKeeper = new ProcessorKeeper21(false);
  // private static Set availableAcceptHelperSet = new HashSet();
  /** The members that haven't replied yet */
  // changed pendingResponders to synchronized Set to fix bug 38972
  private final Set pendingResponders = Collections.synchronizedSet(new HashSet());
  private List responseQueue = null;
  private boolean netWriteSucceeded = false;
  private int remainingTimeout = 0;
  private long startTimeSnapShot = 0;
  private long endTimeSnapShot = 0;

  private boolean netSearch = false;
  private boolean netLoad = false;
  private boolean attemptedLocalLoad = false; // added for bug 39738
  private boolean localLoad = false;
  private boolean localWrite = false;
  private boolean netWrite = false;

  private final Object membersLock = new Object();

  private ArrayList<InternalDistributedMember> departedMembers;

  private Lock lock = null; // if non-null, then needs to be unlocked in release

  static final int NETSEARCH = 0;
  static final int NETLOAD = 1;
  static final int NETWRITE = 2;

  static final int BEFORECREATE = 0;
  static final int BEFOREDESTROY = 1;
  static final int BEFOREUPDATE = 2;
  static final int BEFOREREGIONDESTROY = 3;
  static final int BEFOREREGIONCLEAR = 4;


  /************** Public Methods ************************/

  Object doNetSearch() throws TimeoutException {

    resetResults();
    RegionAttributes attrs = region.getAttributes();
    this.requestInProgress = true;
    Scope scope = attrs.getScope();
    Assert.assertTrue(scope != Scope.LOCAL);
    netSearchForBlob();
    this.requestInProgress = false;
    return this.result;
  }



  void doSearchAndLoad(EntryEventImpl event, TXStateInterface txState, Object localValue,
      boolean preferCD) throws CacheLoaderException, TimeoutException {
    this.requestInProgress = true;
    RegionAttributes attrs = region.getAttributes();
    Scope scope = attrs.getScope();
    CacheLoader loader = ((AbstractRegion) region).basicGetLoader();
    if (scope.isLocal()) {
      Object obj = doLocalLoad(loader, false, preferCD);
      event.setNewValue(obj);
    } else {
      searchAndLoad(event, txState, localValue, preferCD);
    }
    this.requestInProgress = false;
    if (this.netSearch) {
      if (event.getOperation().isCreate()) {
        event.setOperation(Operation.SEARCH_CREATE);
      } else {
        event.setOperation(Operation.SEARCH_UPDATE);
      }
    } else if (this.netLoad) {
      if (event.getOperation().isCreate()) {
        event.setOperation(Operation.NET_LOAD_CREATE);
      } else {
        event.setOperation(Operation.NET_LOAD_UPDATE);
      }
    } else if (this.localLoad) {
      if (event.getOperation().isCreate()) {
        event.setOperation(Operation.LOCAL_LOAD_CREATE);
      } else {
        event.setOperation(Operation.LOCAL_LOAD_UPDATE);
      }
    }
  }

  /** return true if a CacheWriter was actually invoked */
  boolean doNetWrite(CacheEvent event, Set netWriteRecipients, CacheWriter localWriter, int paction)
      throws CacheWriterException, TimeoutException {

    int action = paction;
    this.requestInProgress = true;
    Scope scope = this.region.getScope();
    if (localWriter != null) {
      doLocalWrite(localWriter, event, action);
      this.requestInProgress = false;
      return true;
    }
    if (scope == Scope.LOCAL && (region.getPartitionAttributes() == null)) {
      return false;
    }
    @Released
    CacheEvent listenerEvent = getEventForListener(event);
    try {
      if (action == BEFOREUPDATE && listenerEvent.getOperation().isCreate()) {
        action = BEFORECREATE;
      }
      boolean cacheWrote = netWrite(listenerEvent, action, netWriteRecipients);
      this.requestInProgress = false;
      return cacheWrote;
    } finally {
      if (event != listenerEvent) {
        if (listenerEvent instanceof EntryEventImpl) {
          ((Releasable) listenerEvent).release();
        }
      }
    }
  }

  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {
    // Ignore - if they just joined, they don't have what we want
  }

  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {}

  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

  public void memberDeparted(DistributionManager distributionManager,
      final InternalDistributedMember id, final boolean crashed) {

    synchronized (membersLock) {
      pendingResponders.remove(id);
    }
    synchronized (this) {
      if (id.equals(selectedNode) && (this.requestInProgress) && (this.remoteGetInProgress)) {
        if (departedMembers == null) {
          departedMembers = new ArrayList<InternalDistributedMember>();
        }
        departedMembers.add(id);
        selectedNode = null;
        selectedNodeDead = true;
        computeRemainingTimeout();
        if (logger.isDebugEnabled()) {
          logger.debug("{}: processing loss of member {}", this, id);
        }
        this.lastNotifySpot = 3;
        notifyAll(); // signal the waiter; we are not done; but we need the waiter to call
                     // sendNetSearchRequest
      }
      if (responseQueue != null) {
        responseQueue.remove(id);
      }
      checkIfDone();
    }
  }

  int getProcessorId() {
    return this.processorId;
  }

  synchronized void checkIfDone() {
    if (!this.remoteGetInProgress && this.pendingResponders.isEmpty()) {
      // Synchronize in case a different response/reply is still
      // in progress, and it's the one thats got the goods (bug 28741)
      signalDone();
    }


  }

  synchronized void signalDone() {
    this.requestInProgress = false;
    this.lastNotifySpot = 1;
    notifyAll();
  }

  synchronized void signalTimedOut() {
    this.lastNotifySpot = 2;
    notifyAll();
  }

  private volatile int lastNotifySpot = 0;

  private VersionTag versionTag;

  synchronized void nackResponseComplete() {
    /*
     * if (this.requestInProgress && currentHelper != null && optimizer != null &&
     * optimizer.allRepliesReceived(currentHelper.nackServerChannel)) { this.requestInProgress =
     * false; signalDone(); }
     */

  }

  static ProcessorKeeper21 getProcessorKeeper() {
    return processorKeeper;
  }

  boolean isNetSearch() {
    return netSearch;
  }

  boolean isNetLoad() {
    return netLoad;
  }

  boolean isLocalLoad() {
    return localLoad;
  }

  boolean isNetWrite() {
    return netWrite;
  }

  boolean isLocalWrite() {
    return localWrite;
  }

  long getLastModified() {
    return lastModified;
  }

  boolean resultIsSerialized() {
    return this.isSerialized;
  }

  static SearchLoadAndWriteProcessor getProcessor() {
    SearchLoadAndWriteProcessor processor = new SearchLoadAndWriteProcessor();
    processor.processorId = getProcessorKeeper().put(processor);
    return processor;
  }

  void release() {
    try {
      if (this.lock != null) {
        try {
          this.lock.unlock();
        } catch (CancelException ignore) {
        }
        this.lock = null;
      }
    } finally {
      try {
        if (this.advisor != null) {
          this.advisor.removeMembershipListener(this);
        }
      } catch (IllegalArgumentException ignore) {
      } finally {
        getProcessorKeeper().remove(this.processorId);
      }
    }
  }

  void remove() {
    getProcessorKeeper().remove(this.processorId);
  }

  void initialize(LocalRegion theRegion, Object theKey, Object theCallbackArg) {
    this.region = theRegion;
    this.regionName = theRegion.getFullPath();
    this.key = theKey;
    this.aCallbackArgument = theCallbackArg;
    RegionAttributes attrs = theRegion.getAttributes();
    Scope scope = attrs.getScope();
    if (scope.isDistributed()) {
      this.advisor = ((CacheDistributionAdvisee) this.region).getCacheDistributionAdvisor();
      this.distributionManager = theRegion.getDistributionManager();
      this.timeout = getSearchTimeout();
      this.advisor.addMembershipListener(this);
    }
  }

  void setKey(Object key) {
    this.key = key;
  }

  protected void setSelectedNode(InternalDistributedMember selectedNode) {
    this.selectedNode = selectedNode;
    this.selectedNodeDead = false;
  }

  protected int getTimeout() {
    return this.timeout;
  }

  protected Object getKey() {
    return this.key;
  }

  InternalDistributedMember getSelectedNode() {
    return this.selectedNode;
  }

  /**
   * Even though SearchLoadAndWriteProcessor may be in invoked in the context of a local region,
   * most of the services it provides are relevant to distribution only. The 3 services it provides
   * are netSearch, netLoad, netWrite
   */
  private SearchLoadAndWriteProcessor() {
    resetResults();
    this.pendingResponders.clear();
    this.attemptedLocalLoad = false;
    this.netSearchDone = false;
    this.isSerialized = false;
    this.result = null;
    this.key = null;
    this.requestInProgress = false;
    this.netWriteSucceeded = false;
    this.remoteGetInProgress = false;
    this.responseQueue = null;
  }

  /**
   * If we have a local cache loader and the region is not global, then invoke the loader If the
   * region is local, or the result is non-null, then return whatever the loader returned do a
   * netSearch amongst selected peers if netSearch returns a blob, deserialize the blob and return
   * that as the result netSearch failed, so all we can do at this point is do a load return result
   * from load
   *
   */
  private void searchAndLoad(EntryEventImpl event, TXStateInterface txState, Object localValue,
      boolean preferCD) throws CacheLoaderException, TimeoutException {

    RegionAttributes attrs = region.getAttributes();
    Scope scope = attrs.getScope();
    DataPolicy dataPolicy = attrs.getDataPolicy();

    if (txState != null) {
      TXEntryState tx = txState.txReadEntry(event.getKeyInfo(), region, false,
          true/* create txEntry is absent */);
      if (tx != null) {
        if (tx.noValueInSystem()) {
          // If the tx view has it invalid or destroyed everywhere
          // then don't do a netsearch. We want to see the
          // transactional view.
          load(event, preferCD);
          return;
        }
      }
    }

    // if mirrored then we can optimize by skipping netsearch in some cases,
    // and if also skip netSearch if we find an INVALID token since we
    // know it was distributed. (Otherwise it would be a LOCAL_INVALID token)
    {
      if (localValue == Token.INVALID || dataPolicy.withReplication()) {
        load(event, preferCD);
        return;
      }
    }

    Object obj = null;
    if (!scope.isGlobal()) {
      // copy into local var to prevent race condition
      CacheLoader loader = ((AbstractRegion) region).basicGetLoader();
      if (loader != null) {
        obj = doLocalLoad(loader, true, preferCD);
        Assert.assertTrue(obj != Token.INVALID && obj != Token.LOCAL_INVALID);
        event.setNewValue(obj);
        this.isSerialized = false;
        this.result = obj;
        return;
      }
      if (scope.isLocal()) {
        return;
      }
    }
    netSearchForBlob();
    if (this.result != null) {
      Assert.assertTrue(this.result != Token.INVALID && this.result != Token.LOCAL_INVALID);
      if (this.isSerialized) {
        event.setSerializedNewValue((byte[]) this.result);
      } else {
        event.setNewValue(this.result);
      }
      event.setVersionTag(this.versionTag);
      return;
    }

    load(event, preferCD);
  }

  /** perform a net-search, setting this.result to the object found in the search */
  private void netSearchForBlob() throws TimeoutException {
    if (this.netSearchDone)
      return;
    this.netSearchDone = true;
    CachePerfStats stats = region.getCachePerfStats();
    long start = 0;
    Set sendSet = null;

    this.result = null;
    RegionAttributes attrs = region.getAttributes();
    // Object aCallbackArgument = null;
    this.requestInProgress = true;
    this.selectedNodeDead = false;
    initRemainingTimeout();
    start = stats.startNetsearch();
    try {
      List<InternalDistributedMember> replicates =
          new ArrayList(advisor.adviseInitializedReplicates());
      if (replicates.size() > 1) {
        Collections.shuffle(replicates);
      }
      for (InternalDistributedMember replicate : replicates) {
        synchronized (this.pendingResponders) {
          this.pendingResponders.clear();
        }

        synchronized (this) {
          this.requestInProgress = true;
          this.remoteGetInProgress = true;
          setSelectedNode(replicate);
          this.lastNotifySpot = 0;

          sendValueRequest(replicate);
          waitForObject2(this.remainingTimeout);

          if (this.authorative) {
            if (this.result != null) {
              this.netSearch = true;
            }
            return;
          } else {
            // clear anything that might have been set by our query.
            this.selectedNode = null;
            this.selectedNodeDead = false;
            this.lastNotifySpot = 0;
            this.result = null;
          }
        }
      }
      synchronized (membersLock) {
        Set recipients = this.advisor.adviseNetSearch();
        if (recipients.isEmpty()) {
          return;
        }
        ArrayList list = new ArrayList(recipients);
        Collections.shuffle(list);
        sendSet = new HashSet(list);
        synchronized (this.pendingResponders) {
          this.pendingResponders.clear();
          this.pendingResponders.addAll(list);
        }
      }

      boolean useMulticast = region.getMulticastEnabled() && (region instanceof DistributedRegion)
          && ((DistributedRegion) region).getSystem().getConfig().getMcastPort() != 0;

      // moved outside the sync to fix bug 39458
      QueryMessage.sendMessage(this, this.regionName, this.key, useMulticast, sendSet,
          this.remainingTimeout, attrs.getEntryTimeToLive().getTimeout(),
          attrs.getEntryIdleTimeout().getTimeout());

      synchronized (this) {
        // moved this send back into sync to fix bug 37132
        // QueryMessage.sendMessage(this, this.regionName,this.key,useMulticast,
        // sendSet,this.remainingTimeout ,
        // attrs.getEntryTimeToLive().getTimeout(),
        // attrs.getEntryIdleTimeout().getTimeout());
        boolean done = false;
        do {
          waitForObject2(this.remainingTimeout);
          if (this.selectedNodeDead && remoteGetInProgress) {
            sendNetSearchRequest();
          } else
            done = true;
        } while (!done);

        if (this.result != null) {
          this.netSearch = true;
        }
        return;
      }
    } finally {
      stats.endNetsearch(start);
    }
  }

  private void load(EntryEventImpl event, boolean preferCD)
      throws CacheLoaderException, TimeoutException {
    Object obj = null;
    RegionAttributes attrs = this.region.getAttributes();
    Scope scope = attrs.getScope();
    CacheLoader loader = ((AbstractRegion) region).basicGetLoader();
    Assert.assertTrue(scope.isDistributed());

    if ((loader != null) && (!scope.isGlobal())) {
      obj = doLocalLoad(loader, false, preferCD);
      event.setNewValue(obj);
      Assert.assertTrue(obj != Token.INVALID && obj != Token.LOCAL_INVALID);
      return;
    }

    if (scope.isGlobal()) {
      Assert.assertTrue(this.lock == null);
      Set loadCandidatesSet = advisor.adviseNetLoad();
      if ((loader == null) && (loadCandidatesSet.isEmpty())) {
        // no one has a data Loader. No point getting a lock
        return;
      }

      this.lock = region.getDistributedLock(this.key);
      boolean locked = false;
      try {
        final CancelCriterion stopper = region.getCancelCriterion();
        for (;;) {
          stopper.checkCancelInProgress(null);
          boolean interrupted = Thread.interrupted();
          try {
            locked = this.lock.tryLock(region.getCache().getLockTimeout(), TimeUnit.SECONDS);
            if (!locked) {
              throw new TimeoutException(
                  String.format("Timed out locking %s before load",
                      key));
            }
            break;
          } catch (InterruptedException ignore) {
            interrupted = true;
            region.getCancelCriterion().checkCancelInProgress(null);
            // continue;
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for
        if (loader == null) {
          this.localLoad = false;
          if (scope.isDistributed()) {
            this.isSerialized = false;
            obj = doNetLoad();
            Assert.assertTrue(obj != Token.INVALID && obj != Token.LOCAL_INVALID);
            if (this.isSerialized && obj != null) {
              event.setSerializedNewValue((byte[]) obj);
            } else {
              event.setNewValue(obj);
            }
          }
        } else {
          obj = doLocalLoad(loader, false, preferCD);
          Assert.assertTrue(obj != Token.INVALID && obj != Token.LOCAL_INVALID);
          event.setNewValue(obj);
        }
        return;
      } finally {
        // The lock will not actually be released until release is
        // called on this processor
        if (!locked)
          this.lock = null;
      }
    }
    if (scope.isDistributed()) {
      // long start = System.currentTimeMillis();
      obj = doNetLoad();
      if (this.isSerialized && obj != null) {
        event.setSerializedNewValue((byte[]) obj);
      } else {
        Assert.assertTrue(obj != Token.INVALID && obj != Token.LOCAL_INVALID);
        event.setNewValue(obj);
      }
      // long end = System.currentTimeMillis();
    }
  }

  Object doNetLoad() throws CacheLoaderException, TimeoutException {
    if (this.netLoadDone)
      return null;
    this.netLoadDone = true;
    if (advisor != null) {
      Set loadCandidatesSet = advisor.adviseNetLoad();
      if (loadCandidatesSet.isEmpty()) {
        return null;
      }
      CachePerfStats stats = region.getCachePerfStats();
      long start = stats.startNetload();
      ArrayList list = new ArrayList(loadCandidatesSet);
      Collections.shuffle(list);
      InternalDistributedMember[] loadCandidates =
          (InternalDistributedMember[]) list.toArray(new InternalDistributedMember[list.size()]);
      initRemainingTimeout();

      RegionAttributes attrs = region.getAttributes();
      int index = 0;
      boolean stayInLoop = false; // never set to true!
      this.remoteLoadInProgress = true;
      try {
        do { // the only time this loop repeats is when continue is called
          InternalDistributedMember next = loadCandidates[index++];
          setSelectedNode(next);
          this.lastNotifySpot = 0;
          this.requestInProgress = true;
          if (this.remainingTimeout <= 0) { // @todo this looks wrong; why not a timeout exception?
            break;
          }
          this.remoteException = null;
          NetLoadRequestMessage.sendMessage(this, this.regionName, this.key, this.aCallbackArgument,
              next, this.remainingTimeout, attrs.getEntryTimeToLive().getTimeout(),
              attrs.getEntryIdleTimeout().getTimeout());
          waitForObject2(this.remainingTimeout);
          if (this.remoteException == null) {
            if (!this.requestInProgress) {
              // note even if result is null we are done; see bug 39738
              this.localLoad = false;
              if (this.result != null) {
                this.netLoad = true;
              }
              return this.result;
            } else {
              // Why does the following test for selectedNodeDead?
              // Seems like this will cause us to quit trying netLoad
              // even if we don't have a result yet and have not tried everyone.
              if ((this.selectedNodeDead) && (index < loadCandidates.length)) {
                continue;
              }
              // otherwise we are done
            }
          } else {
            Throwable cause;
            if (this.remoteException instanceof TryAgainException) {
              if (index < loadCandidates.length) {
                continue;
              } else {
                break;
              }
            }
            if (this.remoteException instanceof CacheLoaderException) {
              cause = this.remoteException.getCause();
            } else {
              cause = this.remoteException;
            }
            throw new CacheLoaderException(
                String.format("While invoking a remote netLoad: %s",
                    cause),
                cause);
          }

        } while (stayInLoop);
      } finally {
        stats.endNetload(start);
      }

    }
    return null;
  }

  /**
   * This exception is just used in the class to tell the caller that it should try again.
   * InternalGemFireException used to be used for this which seems dangerous.
   */
  private static class TryAgainException extends GemFireException {

    ////////////////////// Constructors //////////////////////

    public TryAgainException() {
      super();
    }

    /**
     * Creates a new <code>TryAgainException</code>.
     */
    public TryAgainException(String message) {
      super(message);
    }
  }


  private Object doLocalLoad(CacheLoader loader, boolean netSearchAllowed, boolean preferCD)
      throws CacheLoaderException {
    Object obj = null;
    if (loader != null && !this.attemptedLocalLoad) {
      this.attemptedLocalLoad = true;
      CachePerfStats stats = region.getCachePerfStats();
      LoaderHelper loaderHelper = this.region.loaderHelperFactory.createLoaderHelper(this.key,
          this.aCallbackArgument, netSearchAllowed, true /* netLoadAllowed */, this);
      long statStart = stats.startLoad();
      try {
        obj = loader.load(loaderHelper);
        obj = this.region.getCache().convertPdxInstanceIfNeeded(obj, preferCD);
      } finally {
        stats.endLoad(statStart);
      }
      if (obj != null) {
        this.localLoad = true;
      }
    }
    return obj;
  }

  /**
   * Returns an event for listener notification. The event's operation may be altered to conform to
   * the ConcurrentMap implementation specification. If the returned value is not == to the event
   * parameter then the caller is responsible for releasing it.
   *
   * @param event the original event
   * @return the original event or a new event having a change in operation
   */
  @Retained
  private CacheEvent getEventForListener(CacheEvent event) {
    Operation op = event.getOperation();
    if (!op.isEntry()) {
      return event;
    } else {
      EntryEventImpl r = (EntryEventImpl) event;
      @Retained
      EntryEventImpl result = r;
      if (r.isSingleHop()) {
        // fix for bug #46130 - origin remote incorrect for one-hop operation in receiver
        result = new EntryEventImpl(r);
        result.setOriginRemote(true);
        // if this is from a non-replicate and it's not in a tx it should be seen as a Create
        // because that's what the sender would use in notifying listeners. bug #46955
        if (result.getOperation().isUpdate() && (result.getTransactionId() == null)) {
          result.makeCreate();
        }
      }
      if (op == Operation.REPLACE) {
        if (result == r)
          result = new EntryEventImpl(r);
        result.setOperation(Operation.UPDATE);
      } else if (op == Operation.PUT_IF_ABSENT) {
        if (result == r)
          result = new EntryEventImpl(r);
        result.setOperation(Operation.CREATE);
      } else if (op == Operation.REMOVE) {
        if (result == r)
          result = new EntryEventImpl(r);
        result.setOperation(Operation.DESTROY);
      }
      return result;
    }
  }

  private boolean doLocalWrite(CacheWriter writer, CacheEvent pevent, int paction)
      throws CacheWriterException {
    // Return if the inhibit all notifications flag is set
    if (pevent instanceof EntryEventImpl) {
      if (((EntryEventImpl) pevent).inhibitAllNotifications()) {
        if (logger.isDebugEnabled()) {
          logger.debug("Notification inhibited for key {}", pevent);
        }
        return false;
      }
    }
    @Released
    CacheEvent event = getEventForListener(pevent);

    int action = paction;
    if (event.getOperation().isCreate() && action == BEFOREUPDATE) {
      action = BEFORECREATE;
    }
    if (event instanceof EntryEventImpl) {
      ((EntryEventImpl) event).setReadOldValueFromDisk(true);
    }
    try {
      switch (action) {
        case BEFORECREATE:
          writer.beforeCreate((EntryEvent) event);
          break;
        case BEFOREDESTROY:
          writer.beforeDestroy((EntryEvent) event);
          break;
        case BEFOREUPDATE:
          writer.beforeUpdate((EntryEvent) event);
          break;
        case BEFOREREGIONDESTROY:
          writer.beforeRegionDestroy((RegionEvent) event);
          break;
        case BEFOREREGIONCLEAR:
          writer.beforeRegionClear((RegionEvent) event);
          break;
        default:
          break;

      }
    } finally {
      if (event instanceof EntryEventImpl) {
        ((EntryEventImpl) event).setReadOldValueFromDisk(false);
      }
      if (event != pevent) {
        if (event instanceof EntryEventImpl) {
          ((Releasable) event).release();
        }
      }
    }
    this.localWrite = true;
    return true;

  }

  /** Return true if cache writer was invoked */
  private boolean netWrite(CacheEvent event, int action, Set writeCandidateSet)
      throws CacheWriterException, TimeoutException {
    if (writeCandidateSet == null || writeCandidateSet.isEmpty()) {
      return false;
    }
    ArrayList list = new ArrayList(writeCandidateSet);
    Collections.shuffle(list);
    InternalDistributedMember[] writeCandidates =
        (InternalDistributedMember[]) list.toArray(new InternalDistributedMember[list.size()]);
    initRemainingTimeout();
    int index = 0;
    do {
      InternalDistributedMember next = writeCandidates[index++];
      Set set = new HashSet();
      set.add(next);
      this.netWriteSucceeded = false;
      this.requestInProgress = true;
      this.remoteException = null;
      NetWriteRequestMessage.sendMessage(this, this.regionName, this.remainingTimeout, event, set,
          action);
      if (this.remainingTimeout <= 0) { // @todo: should this throw a timeout exception?
        break;
      }
      waitForObject2(this.remainingTimeout);
      if (this.netWriteSucceeded) {
        this.netWrite = true;
        break;
      }
      if (this.remoteException != null) {
        Throwable cause;
        if (this.remoteException instanceof TryAgainException) {
          if (index < writeCandidates.length) {
            continue;
          } else {
            break;
          }
        }
        if (this.remoteException instanceof CacheWriterException
            && this.remoteException.getCause() != null) {
          cause = this.remoteException.getCause();
        } else {
          cause = this.remoteException;
        }
        throw new CacheWriterException(
            String.format("While invoking a remote netWrite: %s",
                cause),
            cause);
      }
    } while (index < writeCandidates.length);

    return this.netWriteSucceeded;
  }


  /**
   * process a QueryMessage netsearch response
   *
   * @param versionTag TODO
   */
  protected synchronized void incomingResponse(Object obj, long lastModifiedTime, boolean isPresent,
      boolean serialized, final boolean requestorTimedOut, final InternalDistributedMember sender,
      ClusterDistributionManager dm, VersionTag versionTag) {
    // NOTE: keep this method efficient since it is optimized
    // by executing it in the p2p reader.
    // This is done with this line in DistributionMessage.java:
    // || c.equals(SearchLoadAndWriteProcessor.ResponseMessage.class)

    // bug 35266 - don't pay attention to late breaking "get" responses
    if (this.remoteLoadInProgress) {
      if (logger.isDebugEnabled()) {
        logger.debug("Ignoring netsearch response from {} because we're now doing a netload",
            sender);
      }
      return;
    }

    if (this.pendingResponders.isEmpty()) {
      return;
    }
    if (!this.pendingResponders.remove(sender)) {
      return;
    } else {
      if (logger.isDebugEnabled()) {
        // only log the message if it's from a recipient we're waiting for.
        // it could have been multicast to all members, which would give us
        // one response per member
        logger.debug(
            "Processing response for processorId={}, isPresent is {}, sender is {}, key is {}, value is {}, version is {}",
            this.processorId, isPresent, sender, this.key, serialized, versionTag);
      }
    }

    // Another thread got a response and that contained the value.
    // Ignore this response.
    if (this.result != null) {
      return;
    }

    if (isPresent) {
      if (obj != null) {
        Assert.assertTrue(obj != Token.INVALID && obj != Token.LOCAL_INVALID);
        synchronized (this) {
          this.result = obj;
          this.lastModified = lastModifiedTime;
          this.isSerialized = serialized;
          this.versionTag = versionTag;
          signalDone();
          return;
        }
      } else {
        if (!remoteGetInProgress) {
          // send a message to this responder asking for the value
          // do this on the waiting pool in case the send blocks
          try {
            dm.getWaitingThreadPool().execute(new Runnable() {
              public void run() {
                sendValueRequest(sender);
              }
            });
            // need to do this here before releasing sync to fix bug 37132
            this.requestInProgress = true;
            this.remoteGetInProgress = true;
            setSelectedNode(sender);
            return; // sendValueRequest does the rest of the work
          } catch (RejectedExecutionException ignore) {
            // just fall through since we must be shutting down.
          }
        }
        if (responseQueue == null)
          responseQueue = new LinkedList();
        if (logger.isDebugEnabled()) {
          logger.debug("Saving isPresent response, requestInProgress {}", sender);
        }
        responseQueue.add(sender);
      }
    }
    if (requestorTimedOut) {
      signalTimedOut();
    }

    boolean endRequest = false;
    if (this.pendingResponders.isEmpty() && (!remoteGetInProgress)) {
      endRequest = true;
    }
    if (endRequest) {
      signalDone();
    }
  }

  protected synchronized void sendValueRequest(final InternalDistributedMember sender) {
    // send a message to this responder asking for the value
    // do this on the waiting pool in case the send blocks
    // Always attempt to send the message to fix bug 37149
    RegionAttributes attrs = this.region.getAttributes();
    NetSearchRequestMessage.sendMessage(this, this.regionName, this.key, sender,
        this.remainingTimeout, attrs.getEntryTimeToLive().getTimeout(),
        attrs.getEntryIdleTimeout().getTimeout());
    // if it turns out that we can't send a message to this member then
    // our membership listener should save the day and schedule a send
    // to someone else or give up and report a failed search.
  }

  // @todo creation times need to be handled properly
  /**
   * This is the response from the accepted responder. Grab the result and store it.Unlike 2.0 where
   * the the response was a 2 phase operation, here it is a single phase operation.
   */
  protected void incomingNetLoadReply(Object obj, long lastModifiedTime, Object callbackArg,
      Exception e, boolean serialized, boolean requestorTimedOut) {
    synchronized (this) {
      if (requestorTimedOut) {
        signalTimedOut();
        return;
      }
      this.result = obj;
      this.lastModified = lastModifiedTime;
      this.remoteException = e;
      this.aCallbackArgument = callbackArg;
      computeRemainingTimeout();
      this.isSerialized = serialized;
      signalDone();
    }


  }

  @SuppressWarnings("hiding")
  protected synchronized void incomingNetSearchReply(byte[] value, long lastModifiedTime,
      boolean serialized, boolean requestorTimedOut, boolean authorative, VersionTag versionTag,
      InternalDistributedMember responder) {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (departedMembers != null && departedMembers.contains(responder)) {
      if (isDebugEnabled) {
        logger.debug("ignore the reply received from a departed member");
      }
      return;
    }

    if (this.requestInProgress) {
      if (requestorTimedOut) {
        // Force a timeout exception.
        if (isDebugEnabled) {
          logger.debug("incomingNetSearchReply() - requestorTimedOut {}", this);
        }
        signalTimedOut();
      }
      computeRemainingTimeout();
      if (value != null || authorative) {
        synchronized (this) {
          this.result = value;
          this.lastModified = lastModifiedTime;
          this.isSerialized = serialized;
          this.remoteGetInProgress = false;
          this.authorative = authorative;
          this.versionTag = versionTag;
          if (isDebugEnabled) {
            logger.debug("incomingNetSearchReply() - got obj {}", this);
          }
          signalDone();
        }
      } else if (this.remainingTimeout <= 0) {
        this.remoteGetInProgress = false;
        if (isDebugEnabled) {
          logger.debug("incomingNetSearchReply() - null obj, no more time {}", this);
        }
        signalDone(); // @todo: is this a bug? should call signalTimedOut?
      } else {
        if (isDebugEnabled) {
          logger.debug("incomingNetSearchReply() - null obj, sendNetSearchRequest {}", this);
        }
        sendNetSearchRequest();
      }
    } else {
      if (isDebugEnabled) {
        logger.debug("incomingNetSearchReply() - requestInProgress is false {}", this);
      }
      // should we checkIfDone?
      // sure why not?
      checkIfDone();
    }
  }

  /**
   * Return the next responder on the responseQueue, or null if empty
   */
  private InternalDistributedMember nextAppropriateResponder() {
    if (responseQueue != null && responseQueue.size() > 0) {
      return (InternalDistributedMember) responseQueue.remove(0);
    } else {
      return null;
    }
  }

  private synchronized void sendNetSearchRequest() {
    InternalDistributedMember nextResponder = nextAppropriateResponder();
    if (nextResponder != null) {
      // Make a request to the next responder in the queue
      RegionAttributes attrs = this.region.getAttributes();
      setSelectedNode(nextResponder);
      this.requestInProgress = true;
      this.remoteGetInProgress = true;
      NetSearchRequestMessage.sendMessage(this, this.regionName, this.key, nextResponder,
          this.remainingTimeout, attrs.getEntryTimeToLive().getTimeout(),
          attrs.getEntryIdleTimeout().getTimeout());

    } else {
      this.remoteGetInProgress = false;
      checkIfDone();

    }
  }

  /**
   * This is the response from the accepted responder.
   */
  protected void incomingNetWriteReply(boolean netWriteSuccessful, Exception e, boolean exe) {
    synchronized (this) {
      this.remoteException = e;
      this.netWriteSucceeded = netWriteSuccessful;
      computeRemainingTimeout();
      signalDone();
    }
  }

  private synchronized void initRemainingTimeout() {
    this.remainingTimeout = this.timeout * 1000;
    this.startTimeSnapShot = this.distributionManager.cacheTimeMillis();
  }

  private synchronized void computeRemainingTimeout() {
    if (this.startTimeSnapShot > 0) { // @todo this should be an assertion
      this.endTimeSnapShot = this.distributionManager.cacheTimeMillis();
      long delta = this.endTimeSnapShot - this.startTimeSnapShot;
      if (delta > 0) {
        this.remainingTimeout -= delta;
      }
      this.startTimeSnapShot = this.endTimeSnapShot;
    }
  }

  private synchronized void waitForObject2(final int timeoutMs) throws TimeoutException {
    if (this.requestInProgress) {
      try {
        final DistributionManager dm =
            this.region.getCache().getInternalDistributedSystem().getDistributionManager();
        long waitTimeMs = timeoutMs;
        final long endTime = System.currentTimeMillis() + waitTimeMs;
        for (;;) {
          if (!this.requestInProgress) {
            return;
          }
          if (waitTimeMs <= 0) {
            throw new TimeoutException(
                String.format(
                    "Timed out while doing netsearch/netload/netwrite processorId= %s Key is %s",
                    new Object[] {this.processorId, this.key}));
          }

          boolean interrupted = Thread.interrupted();
          int lastNS = this.lastNotifySpot;
          try {
            {
              boolean done = (lastNS != 0);
              while (!done && waitTimeMs > 0) {
                this.region.getCancelCriterion().checkCancelInProgress(null);
                interrupted = Thread.interrupted() || interrupted;
                long wt = Math.min(RETRY_TIME, waitTimeMs);
                wait(wt); // spurious wakeup ok
                lastNS = this.lastNotifySpot;
                done = (lastNS != 0);
                if (!done) {
                  // calc remaing wait time to fix bug 37196
                  waitTimeMs = endTime - System.currentTimeMillis();
                }
              } // while
              if (done) {
                this.lastNotifySpot = 0;
              }
            }
            if (this.requestInProgress && !this.selectedNodeDead) {
              // added the test of "!this.selectedNodeDead" for bug 37196
              StringBuilder sb = new StringBuilder(200);
              sb.append("processorId=").append(this.processorId);
              sb.append(" Key is ").append(this.key);
              sb.append(" searchTimeoutMs ").append(timeoutMs);
              if (waitTimeMs > 0) {
                sb.append(" msRemaining=").append(waitTimeMs);
              }
              if (lastNS != 0) {
                sb.append(" lastNotifySpot=").append(lastNS);
              }
              throw new TimeoutException(
                  String.format("Timeout during netsearch/netload/netwrite. Details: %s",
                      sb));
            }
            return;
          } catch (InterruptedException ignore) {
            interrupted = true;
            region.getCancelCriterion().checkCancelInProgress(null);
            // keep waiting until we are done
            waitTimeMs = endTime - System.currentTimeMillis();
            // continue
          } finally {
            if (interrupted) {
              Thread.currentThread().interrupt();
            }
          }
        } // for
      } finally {
        computeRemainingTimeout();
      }
    } // requestInProgress
  }

  private int getSearchTimeout() {
    return region.getCache().getSearchTimeout();
  }

  private void resetResults() {
    this.netSearch = false;
    this.netLoad = false;
    this.localLoad = false;
    this.localWrite = false;
    this.netWrite = false;
    this.lastModified = 0;
    this.isSerialized = false;
  }


  // private AcceptHelper getAcceptHelper(boolean ackPortInit) {
  // AcceptHelper helper = null;
  // synchronized(availableAcceptHelperSet) {
  // if (availableAcceptHelperSet.size() <= 0) {
  // helper = new AcceptHelper();
  // }
  // else {
  // helper = (AcceptHelper)availableAcceptHelperSet.iterator().next();
  // availableAcceptHelperSet.remove(helper);
  // }
  // }
  // helper.reset(ackPortInit);
  // return helper;
  //
  // }

  // private void releaseAcceptHelper(AcceptHelper helper) {
  // synchronized(availableAcceptHelperSet) {
  // if (!availableAcceptHelperSet.contains(helper))
  // availableAcceptHelperSet.add(helper);
  // }
  //
  // }

  @Override
  public String toString() {
    return super.toString() + " processorId " + this.processorId;
  }

  /**
   * methods to set/remove htree reference (Bug 40299).
   */
  protected static void setClearCountReference(LocalRegion rgn) {
    DiskRegion dr = rgn.getDiskRegion();
    if (dr != null) {
      dr.setClearCountReference();
    }
  }

  protected static void removeClearCountReference(LocalRegion rgn) {
    DiskRegion dr = rgn.getDiskRegion();
    if (dr != null) {
      dr.removeClearCountReference();
    }
  }

  /**
   * A QueryMessage is broadcast to every node that has the region defined, to find out who has a
   * valid copy of the requested object.
   */
  public static class QueryMessage extends SerialDistributionMessage {

    /**
     * The object id of the processor object on the initiator node. This will be communicated back
     * in the response to enable transferring the result to the initiating VM.
     */
    private int processorId;

    /** The fully qualified name of the Region */
    private String regionName;

    /** The object name */
    private Object key;

    /** Amount of time to wait before giving up */
    private int timeoutMs;



    /** The originator's expiration criteria */
    private int ttl, idleTime;

    /**
     * if true then always send back value even if it is large. Added for bug 35942.
     */
    private boolean alwaysSendResult;

    // using available bitmask flags
    private static final short HAS_TTL = UNRESERVED_FLAGS_START;
    private static final short HAS_IDLE_TIME = (HAS_TTL << 1);
    private static final short ALWAYS_SEND_RESULT = (HAS_IDLE_TIME << 1);

    public QueryMessage() {
      // do nothing
    }

    /**
     * Using a new or pooled message instance, create and send the query to all nodes.
     */
    public static void sendMessage(SearchLoadAndWriteProcessor processor, String regionName,
        Object key, boolean multicast, Set recipients, int timeoutMs, int ttl, int idleTime) {

      // create a message
      QueryMessage msg = new QueryMessage();
      msg.initialize(processor, regionName, key, multicast, timeoutMs, ttl, idleTime);
      msg.setRecipients(recipients);
      if (!multicast && recipients.size() == 1) {
        // we are only searching one recipient, so tell it to send the value
        msg.alwaysSendResult = true;
      }
      processor.distributionManager.putOutgoing(msg);

    }

    private void initialize(SearchLoadAndWriteProcessor processor, String theRegionName,
        Object theKey, boolean multicast, int theTimeoutMs, int theTtl, int theIdleTime) {
      this.processorId = processor.processorId;
      this.regionName = theRegionName;
      setMulticast(multicast);
      this.key = theKey;
      this.timeoutMs = theTimeoutMs;
      this.ttl = theTtl;
      this.idleTime = theIdleTime;
      Assert.assertTrue(processor.region.getScope().isDistributed());
    }


    /**
     * This method execute's on the receiver's node, and checks to see if the requested object
     * exists in shared memory on this node, and if so, sends back a ResponseMessage.
     */
    @Override
    protected void process(ClusterDistributionManager dm) {
      doGet(dm);

    }

    public int getDSFID() {
      return QUERY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);

      short flags = 0;
      if (this.processorId != 0)
        flags |= HAS_PROCESSOR_ID;
      if (this.ttl != 0)
        flags |= HAS_TTL;
      if (this.idleTime != 0)
        flags |= HAS_IDLE_TIME;
      if (this.alwaysSendResult)
        flags |= ALWAYS_SEND_RESULT;
      out.writeShort(flags);

      if (this.processorId != 0) {
        out.writeInt(this.processorId);
      }
      out.writeUTF(this.regionName);
      DataSerializer.writeObject(this.key, out);
      out.writeInt(this.timeoutMs);
      if (this.ttl != 0) {
        InternalDataSerializer.writeSignedVL(this.ttl, out);
      }
      if (this.idleTime != 0) {
        InternalDataSerializer.writeSignedVL(this.idleTime, out);
      }
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      short flags = in.readShort();
      if ((flags & HAS_PROCESSOR_ID) != 0) {
        this.processorId = in.readInt();
        ReplyProcessor21.setMessageRPId(this.processorId);
      }
      this.regionName = in.readUTF();
      this.key = DataSerializer.readObject(in);
      this.timeoutMs = in.readInt();
      if ((flags & HAS_TTL) != 0) {
        this.ttl = (int) InternalDataSerializer.readSignedVL(in);
      }
      if ((flags & HAS_IDLE_TIME) != 0) {
        this.idleTime = (int) InternalDataSerializer.readSignedVL(in);
      }
      this.alwaysSendResult = (flags & ALWAYS_SEND_RESULT) != 0;
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.QueryMessage for \"" + this.key + "\" in region \""
          + this.regionName + "\", processorId " + processorId + ", timeoutMs=" + this.timeoutMs
          + ", ttl=" + this.ttl + ", idleTime=" + this.idleTime;
    }

    private void doGet(ClusterDistributionManager dm) {
      long startTime = dm.cacheTimeMillis();
      // boolean retVal = true;
      boolean isPresent = false;
      boolean sendResult = false;
      boolean isSer = false;
      long lastModifiedCacheTime = 0;
      boolean requestorTimedOut = false;
      VersionTag tag = null;

      if (dm.getDMType() == ClusterDistributionManager.ADMIN_ONLY_DM_TYPE
          || getSender().equals(dm.getDistributionManagerId())) {
        // this was probably a multicast message
        // replyWithNull(dm); - bug 35266: don't send a reply
        return;
      }

      int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
      try {
        // check to see if we would have to wait on initialization latch (if global)
        // if so abort and reply with null
        InternalCache cache = dm.getExistingCache();
        if (cache.isGlobalRegionInitializing(this.regionName)) {
          replyWithNull(dm);
          if (logger.isDebugEnabled()) {
            logger.debug("Global Region not initialized yet");
          }
          return;
        }

        LocalRegion region = (LocalRegion) dm.getExistingCache().getRegion(this.regionName);
        Object o = null;

        if (region != null) {
          setClearCountReference(region);
          try {
            RegionEntry entry = region.basicGetEntry(this.key);
            if (entry != null) {
              synchronized (entry) {
                assert region.isInitialized();
                if (dm.cacheTimeMillis() - startTime < timeoutMs) {
                  o = region.getNoLRU(this.key, false, true, true); // OFFHEAP: incrc, copy bytes,
                                                                    // decrc
                  if (o != null && !Token.isInvalid(o) && !Token.isRemoved(o)
                      && !region.isExpiredWithRegardTo(this.key, this.ttl, this.idleTime)) {
                    isPresent = true;
                    VersionStamp stamp = entry.getVersionStamp();
                    if (stamp != null && stamp.hasValidVersion()) {
                      tag = stamp.asVersionTag();
                    }
                    lastModifiedCacheTime = entry.getLastModified();
                    isSer = o instanceof CachedDeserializable;
                    if (isSer) {
                      o = ((CachedDeserializable) o).getSerializedValue();
                    }
                    if (isPresent && (this.alwaysSendResult
                        || (ObjectSizer.DEFAULT.sizeof(o) < SMALL_BLOB_SIZE))) {
                      sendResult = true;
                    }
                  }
                } else {
                  requestorTimedOut = true;
                }
              }
            } else if (logger.isDebugEnabled()) {
              logger.debug("Entry is null");
            }
          } finally {
            removeClearCountReference(region);
          }
        }
        ResponseMessage.sendMessage(this.key, this.getSender(), processorId,
            (sendResult ? o : null), lastModifiedCacheTime, isPresent, isSer, requestorTimedOut, dm,
            tag);
      } catch (RegionDestroyedException ignore) {
        logger.debug("Region Destroyed Exception in QueryMessage doGet, null");
        replyWithNull(dm);
      } catch (CancelException ignore) {
        logger.debug("CacheClosedException in QueryMessage doGet, null");
        replyWithNull(dm);
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
        logger.debug("Throwable in QueryMessage doGet, null", t);
        replyWithNull(dm);
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    }

    private void replyWithNull(ClusterDistributionManager dm) {
      ResponseMessage.sendMessage(this.key, this.getSender(), processorId, null, 0, false, false,
          false, dm, null);
    }
  }

  /**
   * The ResponseMessage is a reply to a QueryMessage, and contains the object's value, if it is
   * below the byte limit, otherwise an indication of whether the sender has the value.
   */
  public static class ResponseMessage extends HighPriorityDistributionMessage {

    private Object key;

    /** The gemfire id of the SearchLoadAndWrite object waiting for response */
    private int processorId;

    /** The value being transferred */
    private Object result;

    /** Object creation time on remote node */
    private long lastModified;

    /** is the value present */
    private boolean isPresent;

    /** Is blob serialized? */
    private boolean isSerialized;

    /** did the request time out at the sender */
    private boolean requestorTimedOut;

    /** the version of the object being returned */
    private VersionTag versionTag;


    public ResponseMessage() {}

    public static void sendMessage(Object key, InternalDistributedMember recipient, int processorId,
        Object result, long lastModified, boolean isPresent, boolean isSerialized,
        boolean requestorTimedOut, ClusterDistributionManager distributionManager,
        VersionTag versionTag) {

      // create a message
      ResponseMessage msg = new ResponseMessage();
      msg.initialize(key, processorId, result, lastModified, isPresent, isSerialized,
          requestorTimedOut, versionTag);
      msg.setRecipient(recipient);
      distributionManager.putOutgoing(msg);

    }

    private void initialize(Object theKey, int theProcessorId, Object theResult,
        long lastModifiedTime, boolean ispresent, boolean isserialized,
        boolean requestorTimedOutFlag, VersionTag versionTag) {
      this.key = theKey;
      this.processorId = theProcessorId;
      this.result = theResult;
      this.lastModified = lastModifiedTime;
      this.isPresent = ispresent;
      this.isSerialized = isserialized;
      this.requestorTimedOut = requestorTimedOutFlag;
      this.versionTag = versionTag;
    }

    /**
     * Invoked on the receiver - which, in this case, was the initiator of the QueryMessage .
     */
    @Override
    protected void process(ClusterDistributionManager dm) {
      // NOTE: keep this method efficient since it is optimized
      // by executing it in the p2p reader.
      // This is done with this line in DistributionMessage.java:
      // || c.equals(SearchLoadAndWriteProcessor.ResponseMessage.class)
      SearchLoadAndWriteProcessor processor = null;
      processor = (SearchLoadAndWriteProcessor) getProcessorKeeper().retrieve(this.processorId);
      if (processor == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Response() SearchLoadAndWriteProcessor no longer exists");
        }
        return;
      }
      long lastModifiedSystemTime = 0;
      if (this.lastModified != 0) {
        lastModifiedSystemTime = this.lastModified;
      }
      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(getSender());
      }

      processor.incomingResponse(this.result, lastModifiedSystemTime, this.isPresent,
          this.isSerialized, this.requestorTimedOut, this.getSender(), dm, versionTag);
    }

    @Override
    public boolean getInlineProcess() { // optimization for bug 37075
      return true;
    }

    public int getDSFID() {
      return RESPONSE_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      DataSerializer.writeObject(this.key, out);
      out.writeInt(this.processorId);
      DataSerializer.writeObject(this.result, out);
      out.writeLong(this.lastModified);
      out.writeBoolean(this.isPresent);
      out.writeBoolean(this.isSerialized);
      out.writeBoolean(this.requestorTimedOut);
      DataSerializer.writeObject(this.versionTag, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.key = DataSerializer.readObject(in);
      this.processorId = in.readInt();
      this.result = DataSerializer.readObject(in);
      this.lastModified = in.readLong();
      this.isPresent = in.readBoolean();
      this.isSerialized = in.readBoolean();
      this.requestorTimedOut = in.readBoolean();
      this.versionTag = (VersionTag) DataSerializer.readObject(in);
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.ResponseMessage for processorId " + processorId
          + ", blob is " + this.result + ", isPresent is " + isPresent + ", requestorTimedOut is "
          + requestorTimedOut + ", version is " + versionTag;
    }

  }

  /********************* NetSearchRequestMessage ***************************************/

  public static class NetSearchRequestMessage extends PooledDistributionMessage {

    /**
     * The object id of the processor object on the initiator node. This will be communicated back
     * in the response to enable transferring the result to the initiating VM.
     */
    private int processorId;

    /** The fully qualified name of the Region */
    private String regionName;

    /** The object name */
    private Object key;

    /** Amount of time to wait before giving up */
    private int timeoutMs;

    /** The originator's expiration criteria */
    private int ttl, idleTime;

    // using available bitmask flags
    private static final short HAS_TTL = UNRESERVED_FLAGS_START;
    private static final short HAS_IDLE_TIME = (HAS_TTL << 1);

    public NetSearchRequestMessage() {}

    /**
     * Using a new or pooled message instance, create and send the request for object value to the
     * specified node.
     */
    public static void sendMessage(SearchLoadAndWriteProcessor processor, String regionName,
        Object key, InternalDistributedMember recipient, int timeoutMs, int ttl, int idleTime) {

      // create a message
      NetSearchRequestMessage msg = new NetSearchRequestMessage();
      msg.initialize(processor, regionName, key, timeoutMs, ttl, idleTime);
      msg.setRecipient(recipient);
      processor.distributionManager.putOutgoing(msg);

    }

    void initialize(SearchLoadAndWriteProcessor processor, String theRegionName, Object theKey,
        int timeoutMS, int ttlMS, int idleTimeMS) {
      this.processorId = processor.processorId;
      this.regionName = theRegionName;
      this.key = theKey;
      this.timeoutMs = timeoutMS;
      this.ttl = ttlMS;
      this.idleTime = idleTimeMS;
      Assert.assertTrue(processor.region.getScope().isDistributed());
    }

    /** Invoked on the node that has the object */
    @Override
    protected void process(ClusterDistributionManager dm) {
      doGet(dm);
    }

    public int getDSFID() {
      return NET_SEARCH_REQUEST_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);

      short flags = 0;
      if (this.processorId != 0)
        flags |= HAS_PROCESSOR_ID;
      if (this.ttl != 0)
        flags |= HAS_TTL;
      if (this.idleTime != 0)
        flags |= HAS_IDLE_TIME;
      out.writeShort(flags);

      if (this.processorId != 0) {
        out.writeInt(this.processorId);
      }
      out.writeUTF(this.regionName);
      DataSerializer.writeObject(this.key, out);
      out.writeInt(this.timeoutMs);
      if (this.ttl != 0) {
        InternalDataSerializer.writeSignedVL(this.ttl, out);
      }
      if (this.idleTime != 0) {
        InternalDataSerializer.writeSignedVL(this.idleTime, out);
      }
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      short flags = in.readShort();
      if ((flags & HAS_PROCESSOR_ID) != 0) {
        this.processorId = in.readInt();
        ReplyProcessor21.setMessageRPId(this.processorId);
      }
      this.regionName = in.readUTF();
      this.key = DataSerializer.readObject(in);
      this.timeoutMs = in.readInt();
      if ((flags & HAS_TTL) != 0) {
        this.ttl = (int) InternalDataSerializer.readSignedVL(in);
      }
      if ((flags & HAS_IDLE_TIME) != 0) {
        this.idleTime = (int) InternalDataSerializer.readSignedVL(in);
      }
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.NetSearchRequestMessage for \"" + this.key
          + "\" in region \"" + this.regionName + "\", processorId " + processorId;
    }

    void doGet(ClusterDistributionManager dm) {
      long startTime = dm.cacheTimeMillis();
      // boolean retVal = true;
      byte[] ebv = null;
      Object ebvObj = null;
      int ebvLen = 0;
      long lastModifiedCacheTime = 0;
      boolean isSer = false;
      boolean requestorTimedOut = false;
      boolean authoritative = false;
      VersionTag versionTag = null;

      int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
      try {
        LocalRegion region = (LocalRegion) dm.getExistingCache().getRegion(this.regionName);
        if (region != null) {
          setClearCountReference(region);
          try {
            boolean initialized = region.isInitialized();
            RegionEntry entry = region.basicGetEntry(this.key);
            if (entry != null) {
              synchronized (entry) {
                // get the value and version under synchronization so they don't change
                VersionStamp versionStamp = entry.getVersionStamp();
                if (versionStamp != null) {
                  versionTag = versionStamp.asVersionTag();
                }
                Object eov = region.getNoLRU(this.key, false, true, true); // OFFHEAP: incrc, copy
                                                                           // bytes, decrc
                if (eov != null) {
                  if (eov == Token.INVALID || eov == Token.LOCAL_INVALID) {
                    // nothing?
                  } else if (dm.cacheTimeMillis() - startTime < timeoutMs) {
                    if (!region.isExpiredWithRegardTo(this.key, this.ttl, this.idleTime)) {
                      lastModifiedCacheTime = entry.getLastModified();
                      if (eov instanceof CachedDeserializable) {
                        CachedDeserializable cd = (CachedDeserializable) eov;
                        if (!cd.isSerialized()) {
                          isSer = false;
                          ebv = (byte[]) cd.getDeserializedForReading();
                          ebvLen = ebv.length;
                        } else {
                          // don't serialize here if it is not already serialized
                          Object tmp = cd.getValue();
                          if (tmp instanceof byte[]) {
                            byte[] bb = (byte[]) tmp;
                            ebv = bb;
                            ebvLen = bb.length;
                          } else {
                            ebvObj = tmp;
                          }
                          isSer = true;
                        }
                      } else if (!(eov instanceof byte[])) {
                        ebvObj = eov;
                        isSer = true;
                      } else {
                        ebv = (byte[]) eov;
                        ebvLen = ebv.length;
                      }
                    }
                  } else {
                    requestorTimedOut = true;
                  }
                }
              }
            }
            authoritative =
                region.getDataPolicy().withReplication() && initialized && !region.isDestroyed;
          } finally {
            removeClearCountReference(region);
          }
        }
        NetSearchReplyMessage.sendMessage(NetSearchRequestMessage.this.getSender(), processorId,
            this.key, ebv, ebvObj, ebvLen, lastModifiedCacheTime, isSer, requestorTimedOut,
            authoritative, dm, versionTag);
      } catch (RegionDestroyedException ignore) {
        replyWithNull(dm);

      } catch (CancelException ignore) {
        replyWithNull(dm);

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
        logger.warn("Unexpected exception creating net search reply", t);
        replyWithNull(dm);
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    }

    private void replyWithNull(ClusterDistributionManager dm) {
      NetSearchReplyMessage.sendMessage(NetSearchRequestMessage.this.getSender(), processorId,
          this.key, null, null, 0, 0, false, false, false, dm, null);
    }
  }

  /**
   * The NetSearchReplyMessage is a reply to a NetSearchRequestMessage, and contains the object's
   * value.
   */
  public static class NetSearchReplyMessage extends HighPriorityDistributionMessage {
    private static final byte SERIALIZED = 0x01;
    private static final byte REQUESTOR_TIMEOUT = 0x02;
    private static final byte AUTHORATIVE = 0x04;
    private static final byte VERSIONED = 0x08;
    private static final byte PERSISTENT = 0x10;

    /** The gemfire id of the SearchLoadAndWrite object waiting for response */
    private int processorId;

    /** The object value being transferred */
    private byte[] value;

    private transient Object valueObj; // only used by toData
    private transient int valueLen; // only used by toData

    /** Object creation time on remote node */
    private long lastModified;

    /** Is blob serialized? */
    private boolean isSerialized;

    /** did the request time out at the sender */
    private boolean requestorTimedOut;

    /**
     * Does this member authoritatively know the value? This is used to distinguish a null response
     * indicating the region was missing vs. a null value.
     */
    private boolean authoritative;

    /** the version of the returned entry */
    private VersionTag versionTag;

    public NetSearchReplyMessage() {}

    public static void sendMessage(InternalDistributedMember recipient, int processorId, Object key,
        byte[] value, Object valueObj, int valueLen, long lastModified, boolean isSerialized,
        boolean requestorTimedOut, boolean authoritative,
        ClusterDistributionManager distributionManager, VersionTag versionTag) {
      // create a message
      NetSearchReplyMessage msg = new NetSearchReplyMessage();
      msg.initialize(processorId, value, valueObj, valueLen, lastModified, isSerialized,
          requestorTimedOut, authoritative, versionTag);
      msg.setRecipient(recipient);
      distributionManager.putOutgoing(msg);

    }

    @SuppressWarnings("hiding")
    private void initialize(int procId, byte[] theValue, Object theValueObj, int valueObjLen,
        long lastModifiedTime, boolean isserialized, boolean requestorTimedout,
        boolean authoritative, VersionTag versionTag) {
      this.processorId = procId;
      this.value = theValue;
      this.valueObj = theValueObj;
      this.valueLen = valueObjLen;
      this.lastModified = lastModifiedTime;
      this.isSerialized = isserialized;
      this.requestorTimedOut = requestorTimedout;
      this.authoritative = authoritative;
      this.versionTag = versionTag;
    }

    /**
     * Invoked on the receiver - which, in this case, was the initiator of the
     * NetSearchRequestMessage. This concludes the net request, by communicating an object value.
     */
    @Override
    protected void process(ClusterDistributionManager dm) {
      SearchLoadAndWriteProcessor processor = null;
      processor = (SearchLoadAndWriteProcessor) getProcessorKeeper().retrieve(processorId);
      if (processor == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("NetSearchReplyMessage() SearchLoadAndWriteProcessor {} no longer exists",
              processorId);
        }
        return;
      }
      long lastModifiedSystemTime = 0;
      if (this.lastModified != 0) {
        lastModifiedSystemTime = this.lastModified;
      }
      if (this.versionTag != null) {
        this.versionTag.replaceNullIDs(getSender());
      }
      processor.incomingNetSearchReply(this.value, lastModifiedSystemTime, this.isSerialized,
          this.requestorTimedOut, this.authoritative, this.versionTag, getSender());
    }

    public int getDSFID() {
      return NET_SEARCH_REPLY_MESSAGE;
    }


    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      if (this.valueObj != null) {
        DataSerializer.writeObjectAsByteArray(this.valueObj, out);
      } else {
        DataSerializer.writeByteArray(this.value, this.valueLen, out);
      }
      out.writeLong(this.lastModified);
      byte booleans = 0;
      if (this.isSerialized)
        booleans |= SERIALIZED;
      if (this.requestorTimedOut)
        booleans |= REQUESTOR_TIMEOUT;
      if (this.authoritative)
        booleans |= AUTHORATIVE;
      if (this.versionTag != null)
        booleans |= VERSIONED;
      if (this.versionTag instanceof DiskVersionTag)
        booleans |= PERSISTENT;
      out.writeByte(booleans);
      if (this.versionTag != null) {
        InternalDataSerializer.invokeToData(this.versionTag, out);
      }
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.value = DataSerializer.readByteArray(in);
      if (this.value != null) {
        this.valueLen = this.value.length;
      }
      this.lastModified = in.readLong();
      byte booleans = in.readByte();

      this.isSerialized = (booleans & SERIALIZED) != 0;
      this.requestorTimedOut = (booleans & REQUESTOR_TIMEOUT) != 0;
      this.authoritative = (booleans & AUTHORATIVE) != 0;
      if ((booleans & VERSIONED) != 0) {
        boolean persistentTag = (booleans & PERSISTENT) != 0;
        this.versionTag = VersionTag.create(persistentTag, in);
      }
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.NetSearchReplyMessage for processorId " + processorId
          + ", blob is " +
          // this.value
          (this.value == null ? "null" : "(" + this.value.length + " bytes)") + " authorative="
          + authoritative + " versionTag=" + this.versionTag;
    }

  }


  /******************************** NetLoadRequestMessage **********************/

  public static class NetLoadRequestMessage extends PooledDistributionMessage {

    /**
     * The object id of the processor object on the initiator node. This will be communicated back
     * in the response to enable transferring the result to the initiating VM.
     */
    private int processorId;

    /** The fully qualified name of the Region */
    private String regionName;

    /** The object name */
    private Object key;

    /** Parameter to use when invoking loader */
    private Object aCallbackArgument;

    /** Amount of time to wait before giving up */
    private int timeoutMs;


    /** The originator's expiration criteria */
    private int ttl, idleTime;

    public NetLoadRequestMessage() {}

    /**
     * Using a new or pooled message instance, create and send the request for object value to the
     * specified node.
     */
    public static void sendMessage(SearchLoadAndWriteProcessor processor, String regionName,
        Object key, Object aCallbackArgument, InternalDistributedMember recipient, int timeoutMs,
        int ttl, int idleTime) {

      // create a message
      NetLoadRequestMessage msg = new NetLoadRequestMessage();
      msg.initialize(processor, regionName, key, aCallbackArgument, timeoutMs, ttl, idleTime);
      msg.setRecipient(recipient);

      try {
        processor.distributionManager.putOutgoing(msg);
      } catch (InternalGemFireException e) {
        throw new IllegalArgumentException(
            "Message not serializable");
      }
    }

    private void initialize(SearchLoadAndWriteProcessor processor, String theRegionName,
        Object theKey, Object callbackArgument, int timeoutMS, int ttlMS, int idleTimeMS) {
      this.processorId = processor.processorId;
      this.regionName = theRegionName;
      this.key = theKey;
      this.aCallbackArgument = callbackArgument;
      this.timeoutMs = timeoutMS;
      this.ttl = ttlMS;
      this.idleTime = idleTimeMS;
      Assert.assertTrue(processor.region.getScope().isDistributed());
    }

    /** Invoked on the node that has the object */
    @Override
    protected void process(ClusterDistributionManager dm) {
      doLoad(dm);
    }

    public int getDSFID() {
      return NET_LOAD_REQUEST_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      out.writeUTF(this.regionName);
      DataSerializer.writeObject(this.key, out);
      DataSerializer.writeObject(this.aCallbackArgument, out);
      out.writeInt(this.timeoutMs);
      out.writeInt(this.ttl);
      out.writeInt(this.idleTime);

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.regionName = in.readUTF();
      this.key = DataSerializer.readObject(in);
      this.aCallbackArgument = DataSerializer.readObject(in);
      this.timeoutMs = in.readInt();
      this.ttl = in.readInt();
      this.idleTime = in.readInt();
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.NetLoadRequestMessage for \"" + this.key
          + "\" in region \"" + this.regionName + "\", processorId " + processorId;
    }

    private void doLoad(ClusterDistributionManager dm) {
      long startTime = dm.cacheTimeMillis();
      int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
      try {
        InternalCache gfc = dm.getExistingCache();
        LocalRegion region = (LocalRegion) gfc.getRegion(this.regionName);
        if (region != null && region.isInitialized()
            && (dm.cacheTimeMillis() - startTime < timeoutMs)) {
          CacheLoader loader = ((AbstractRegion) region).basicGetLoader();
          if (loader != null) {
            LoaderHelper loaderHelper = region.loaderHelperFactory.createLoaderHelper(this.key,
                this.aCallbackArgument, false, false, null);
            CachePerfStats stats = region.getCachePerfStats();
            long start = stats.startLoad();
            try {
              Object o = loader.load(loaderHelper);
              // no need to call convertPdxInstanceIfNeeded since we are serializing
              // this into the NetLoadRequestMessage. The loaded object will be deserialized
              // on the other side and have the correct form in that member.
              Assert.assertTrue(o != Token.INVALID && o != Token.LOCAL_INVALID);
              NetLoadReplyMessage.sendMessage(NetLoadRequestMessage.this.getSender(), processorId,
                  o, dm, loaderHelper.getArgument(), null, false, false);

            } catch (Exception e) {
              replyWithException(e, dm);
            } finally {
              stats.endLoad(start);
            }
          } else {
            replyWithException(new TryAgainException("No loader defined"),
                dm);
          }

        } else {
          replyWithException(new TryAgainException(
              "Timeout expired or region not ready"),
              dm);
        }

      } catch (RegionDestroyedException rde) {
        replyWithException(rde, dm);

      } catch (CancelException cce) {
        replyWithException(cce, dm);

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
        replyWithException(new InternalGemFireException(
            "Error processing request",
            t), dm);
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }

    }

    void replyWithException(Exception e, ClusterDistributionManager dm) {
      NetLoadReplyMessage.sendMessage(NetLoadRequestMessage.this.getSender(), processorId, null, dm,
          this.aCallbackArgument, e, false, false);
    }
  }

  /**
   * The NetLoadReplyMessage is a reply to a RequestMessage, and contains the object's value.
   */
  public static class NetLoadReplyMessage extends HighPriorityDistributionMessage {

    /** The gemfire id of the SearchLoadAndWrite object waiting for response */
    private int processorId;

    /** The object value being transferred */
    private Object result;

    /** Loader parameter returned to sender */
    private Object aCallbackArgument;

    /** Exception thrown by remote node */
    private Exception e;

    /** Is blob serialized? */
    private boolean isSerialized;

    /** ??? */
    private boolean requestorTimedOut;

    public NetLoadReplyMessage() {}

    public static void sendMessage(InternalDistributedMember recipient, int processorId, Object obj,
        ClusterDistributionManager distributionManager, Object aCallbackArgument, Exception e,
        boolean isSerialized, boolean requestorTimedOut) {
      // create a message
      NetLoadReplyMessage msg = new NetLoadReplyMessage();
      msg.initialize(processorId, obj, aCallbackArgument, e, isSerialized, requestorTimedOut);
      msg.setRecipient(recipient);
      distributionManager.putOutgoing(msg);
    }

    private void initialize(int procId, Object obj, Object callbackArgument, Exception exe,
        boolean isserialized, boolean requestorTimedout) {
      this.processorId = procId;
      this.result = obj;
      this.e = exe;
      this.aCallbackArgument = callbackArgument;
      this.isSerialized = isserialized;
      this.requestorTimedOut = requestorTimedout;
    }

    /**
     * Invoked on the receiver - which, in this case, was the initiator of the
     * NetLoadRequestMessage. This concludes the net request, by communicating an object value.
     */
    @Override
    protected void process(ClusterDistributionManager dm) {
      SearchLoadAndWriteProcessor processor = null;
      processor = (SearchLoadAndWriteProcessor) getProcessorKeeper().retrieve(processorId);
      if (processor == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("NetLoadReplyMessage() SearchLoadAndWriteProcessor no longer exists");
        }
        return;
      }
      processor.incomingNetLoadReply(this.result, 0, this.aCallbackArgument, this.e,
          this.isSerialized, this.requestorTimedOut);
    }

    public int getDSFID() {
      return NET_LOAD_REPLY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      boolean isSerialized = this.isSerialized;
      if (result instanceof byte[]) {
        DataSerializer.writeByteArray((byte[]) this.result, out);
      } else {
        DataSerializer.writeObjectAsByteArray(this.result, out);
        isSerialized = true;
      }
      DataSerializer.writeObject(this.aCallbackArgument, out);
      DataSerializer.writeObject(this.e, out);
      out.writeBoolean(isSerialized);
      out.writeBoolean(this.requestorTimedOut);

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.result = DataSerializer.readByteArray(in);
      this.aCallbackArgument = DataSerializer.readObject(in);
      this.e = (Exception) DataSerializer.readObject(in);
      this.isSerialized = in.readBoolean();
      this.requestorTimedOut = in.readBoolean();

    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.NetLoadReplyMessage for processorId " + processorId
          + ", blob is " + this.result;
    }

  }

  /********************* NetWriteRequestMessage *******************************/

  public static class NetWriteRequestMessage extends PooledDistributionMessage {

    /**
     * The object id of the processor object on the initiator node. This will be communicated back
     * in the response to enable transferring the result to the initiating VM.
     */
    private int processorId;

    /** The fully qualified name of the Region */
    private String regionName;

    /** The event being sent over to the remote writer */
    CacheEvent event;

    private int timeoutMs;
    /** Action requested by sender */
    int action;

    public NetWriteRequestMessage() {}

    /**
     * Using a new or pooled message instance, create and send the request for object value to the
     * specified node.
     */
    public static void sendMessage(SearchLoadAndWriteProcessor processor, String regionName,
        int timeoutMs, CacheEvent event, Set recipients, int action) {

      NetWriteRequestMessage msg = new NetWriteRequestMessage();
      msg.initialize(processor, regionName, timeoutMs, event, action);
      msg.setRecipients(recipients);
      processor.distributionManager.putOutgoing(msg);

    }

    private void initialize(SearchLoadAndWriteProcessor processor, String theRegionName,
        int timeoutMS, CacheEvent theEvent, int actionType) {
      this.processorId = processor.processorId;
      this.regionName = theRegionName;
      this.timeoutMs = timeoutMS;
      this.event = theEvent;
      this.action = actionType;
      Assert.assertTrue(processor.region.getScope().isDistributed());
    }

    public int getDSFID() {
      return NET_WRITE_REQUEST_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      out.writeUTF(this.regionName);
      out.writeInt(this.timeoutMs);
      DataSerializer.writeObject(this.event, out);
      out.writeInt(this.action);

    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.regionName = in.readUTF();
      this.timeoutMs = in.readInt();
      this.event = (CacheEvent) DataSerializer.readObject(in);
      this.action = in.readInt();
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.NetWriteRequestMessage " + " for region \""
          + this.regionName + "\", processorId " + processorId;
    }

    /** Invoked on the node that has the object */
    @Override
    protected void process(ClusterDistributionManager dm) {
      long startTime = dm.cacheTimeMillis();
      int oldLevel = LocalRegion.setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
      try {
        InternalCache gfc = dm.getExistingCache();
        LocalRegion region = (LocalRegion) gfc.getRegion(this.regionName);
        if (region != null && region.isInitialized()
            && (dm.cacheTimeMillis() - startTime < timeoutMs)) {
          CacheWriter writer = region.basicGetWriter();
          EntryEventImpl entryEvtImpl = null;
          RegionEventImpl regionEvtImpl = null;
          if (this.event instanceof EntryEventImpl) {
            entryEvtImpl = (EntryEventImpl) this.event;
            entryEvtImpl.setRegion(region);
            Operation op = entryEvtImpl.getOperation();
            if (op == Operation.REPLACE) {
              entryEvtImpl.setOperation(Operation.UPDATE);
            } else if (op == Operation.PUT_IF_ABSENT) {
              entryEvtImpl.setOperation(Operation.CREATE);
            } else if (op == Operation.REMOVE) {
              entryEvtImpl.setOperation(Operation.DESTROY);
            }
            // [bruce] even if this event was transmitted from another VM, its operation may have
            // originated in this VM. PartitionedRegion.put() with a cacheWriter is one
            // situation where this can occur
            entryEvtImpl.setOriginRemote(event.getDistributedMember() == null
                || !event.getDistributedMember().equals(dm.getDistributionManagerId()));
          } else if (this.event instanceof RegionEventImpl) {
            regionEvtImpl = (RegionEventImpl) this.event;
            regionEvtImpl.region = region;
            regionEvtImpl.originRemote = true;
          }

          if (writer != null) {
            if (entryEvtImpl != null) {
              entryEvtImpl.setReadOldValueFromDisk(true);
            }
            try {
              switch (action) {
                case BEFORECREATE:
                  writer.beforeCreate(entryEvtImpl);
                  break;
                case BEFOREDESTROY:
                  writer.beforeDestroy(entryEvtImpl);
                  break;
                case BEFOREUPDATE:
                  writer.beforeUpdate(entryEvtImpl);
                  break;
                case BEFOREREGIONDESTROY:
                  writer.beforeRegionDestroy(regionEvtImpl);
                  break;
                case BEFOREREGIONCLEAR:
                  writer.beforeRegionClear(regionEvtImpl);
                  break;
                default:
                  break;

              }
              NetWriteReplyMessage.sendMessage(NetWriteRequestMessage.this.getSender(), processorId,
                  dm, true, null, false);
            } catch (CacheWriterException cwe) {
              NetWriteReplyMessage.sendMessage(NetWriteRequestMessage.this.getSender(), processorId,
                  dm, false, cwe, true);
            } catch (Exception e) {
              NetWriteReplyMessage.sendMessage(NetWriteRequestMessage.this.getSender(), processorId,
                  dm, false, e, false);
            } finally {
              if (entryEvtImpl != null) {
                entryEvtImpl.setReadOldValueFromDisk(false);
              }
            }

          } else {
            NetWriteReplyMessage.sendMessage(NetWriteRequestMessage.this.getSender(), processorId,
                dm, false,
                new TryAgainException(
                    "No cachewriter defined"),
                true);
          }

        } else {
          NetWriteReplyMessage.sendMessage(NetWriteRequestMessage.this.getSender(), processorId, dm,
              false,
              new TryAgainException("Timeout expired or region not ready"),
              true);

        }
      } catch (RegionDestroyedException ignore) {
        NetWriteReplyMessage.sendMessage(NetWriteRequestMessage.this.getSender(), processorId, dm,
            false, null, false);

      } catch (DistributedSystemDisconnectedException e) {
        // shutdown condition
        throw e;
      } catch (CancelException cce) {
        dm.getCancelCriterion().checkCancelInProgress(cce); // TODO anyway to find the region or
                                                            // cache here?
        NetWriteReplyMessage.sendMessage(NetWriteRequestMessage.this.getSender(), processorId, dm,
            false, null, false);
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
        NetWriteReplyMessage.sendMessage(NetWriteRequestMessage.this.getSender(), processorId, dm,
            false,
            new InternalGemFireException(
                "Error processing request",
                t),
            true);
      } finally {
        LocalRegion.setThreadInitLevelRequirement(oldLevel);
      }
    }
  }

  /**
   * The NetWriteReplyMessage is a reply to a NetWriteRequestMessage, and contains the success code
   * or exception that is propagated back to the requestor
   */
  public static class NetWriteReplyMessage extends HighPriorityDistributionMessage {

    /** The gemfire id of the SearchLoadAndWrite object waiting for response */
    private int processorId;

    /** Indicates whether the write succeeded */
    private boolean netWriteSucceeded;


    /** Exception thrown by remote node */
    private Exception e;

    /** Is the exception a cacheLoaderException */
    private boolean cacheWriterException;

    public NetWriteReplyMessage() {}

    public static void sendMessage(InternalDistributedMember recipient, int processorId,
        ClusterDistributionManager distributionManager, boolean netWriteSucceeded, Exception e,
        boolean cacheWriterException) {
      // create a message
      NetWriteReplyMessage msg = new NetWriteReplyMessage();
      msg.initialize(processorId, netWriteSucceeded, e, cacheWriterException);
      msg.setRecipient(recipient);
      distributionManager.putOutgoing(msg);
    }

    private void initialize(int procId, boolean netwriteSucceeded, Exception except,
        boolean cacheWriterExcept) {
      this.processorId = procId;
      this.netWriteSucceeded = netwriteSucceeded;
      this.e = except;
      this.cacheWriterException = cacheWriterExcept;
    }

    /**
     * Invoked on the receiver - which, in this case, was the initiator of the
     * NetWriteRequestMessage. This concludes the net write request, by communicating an object
     * value.
     */
    @Override
    protected void process(ClusterDistributionManager dm) {
      SearchLoadAndWriteProcessor processor = null;
      processor = (SearchLoadAndWriteProcessor) getProcessorKeeper().retrieve(processorId);
      if (processor == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("NetWriteReplyMessage() SearchLoadAndWriteProcessor no longer exists");
        }
        return;
      }
      processor.incomingNetWriteReply(this.netWriteSucceeded, this.e, this.cacheWriterException);
    }

    public int getDSFID() {
      return NET_WRITE_REPLY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      out.writeBoolean(this.netWriteSucceeded);
      DataSerializer.writeObject(this.e, out);
      out.writeBoolean(this.cacheWriterException);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.processorId = in.readInt();
      this.netWriteSucceeded = in.readBoolean();
      this.e = (Exception) DataSerializer.readObject(in);
      this.cacheWriterException = in.readBoolean();
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.NetWriteReplyMessage for processorId " + processorId;
    }
  }
}
