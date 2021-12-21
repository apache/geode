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

import static org.apache.geode.internal.cache.LocalRegion.InitializationLevel.BEFORE_INITIAL_IMAGE;

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
import org.apache.geode.annotations.internal.MakeNotStatic;
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
import org.apache.geode.internal.cache.LocalRegion.InitializationLevel;
import org.apache.geode.internal.cache.versions.DiskVersionTag;
import org.apache.geode.internal.cache.versions.VersionStamp;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.offheap.Releasable;
import org.apache.geode.internal.offheap.annotations.Released;
import org.apache.geode.internal.offheap.annotations.Retained;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.util.internal.GeodeGlossary;

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
      Long.getLong(GeodeGlossary.GEMFIRE_PREFIX + "search-retry-interval", 2000);

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
  @MakeNotStatic
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
    requestInProgress = true;
    Scope scope = attrs.getScope();
    Assert.assertTrue(scope != Scope.LOCAL);
    netSearchForBlob();
    requestInProgress = false;
    return result;
  }



  void doSearchAndLoad(EntryEventImpl event, TXStateInterface txState, Object localValue,
      boolean preferCD) throws CacheLoaderException, TimeoutException {
    requestInProgress = true;
    RegionAttributes attrs = region.getAttributes();
    Scope scope = attrs.getScope();
    CacheLoader loader = region.basicGetLoader();
    if (scope.isLocal()) {
      Object obj = doLocalLoad(loader, false, preferCD);
      event.setNewValue(obj);
    } else {
      searchAndLoad(event, txState, localValue, preferCD);
    }
    requestInProgress = false;
    if (netSearch) {
      if (event.getOperation().isCreate()) {
        event.setOperation(Operation.SEARCH_CREATE);
      } else {
        event.setOperation(Operation.SEARCH_UPDATE);
      }
    } else if (netLoad) {
      if (event.getOperation().isCreate()) {
        event.setOperation(Operation.NET_LOAD_CREATE);
      } else {
        event.setOperation(Operation.NET_LOAD_UPDATE);
      }
    } else if (localLoad) {
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
    requestInProgress = true;
    Scope scope = region.getScope();
    if (localWriter != null) {
      doLocalWrite(localWriter, event, action);
      requestInProgress = false;
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
      requestInProgress = false;
      return cacheWrote;
    } finally {
      if (event != listenerEvent) {
        if (listenerEvent instanceof EntryEventImpl) {
          ((Releasable) listenerEvent).release();
        }
      }
    }
  }

  @Override
  public void memberJoined(DistributionManager distributionManager, InternalDistributedMember id) {
    // Ignore - if they just joined, they don't have what we want
  }

  @Override
  public void memberSuspect(DistributionManager distributionManager, InternalDistributedMember id,
      InternalDistributedMember whoSuspected, String reason) {}

  @Override
  public void quorumLost(DistributionManager distributionManager,
      Set<InternalDistributedMember> failures, List<InternalDistributedMember> remaining) {}

  @Override
  public void memberDeparted(DistributionManager distributionManager,
      final InternalDistributedMember id, final boolean crashed) {

    synchronized (membersLock) {
      pendingResponders.remove(id);
    }
    synchronized (this) {
      if (id.equals(selectedNode) && (requestInProgress) && (remoteGetInProgress)) {
        if (departedMembers == null) {
          departedMembers = new ArrayList<>();
        }
        departedMembers.add(id);
        selectedNode = null;
        selectedNodeDead = true;
        computeRemainingTimeout();
        if (logger.isDebugEnabled()) {
          logger.debug("{}: processing loss of member {}", this, id);
        }
        lastNotifySpot = 3;
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
    return processorId;
  }

  synchronized void checkIfDone() {
    if (!remoteGetInProgress && pendingResponders.isEmpty()) {
      // Synchronize in case a different response/reply is still
      // in progress, and it's the one thats got the goods (bug 28741)
      signalDone();
    }


  }

  synchronized void signalDone() {
    requestInProgress = false;
    lastNotifySpot = 1;
    notifyAll();
  }

  synchronized void signalTimedOut() {
    lastNotifySpot = 2;
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
    return isSerialized;
  }

  static SearchLoadAndWriteProcessor getProcessor() {
    SearchLoadAndWriteProcessor processor = new SearchLoadAndWriteProcessor();
    processor.processorId = getProcessorKeeper().put(processor);
    return processor;
  }

  void release() {
    try {
      if (lock != null) {
        try {
          lock.unlock();
        } catch (CancelException ignore) {
        }
        lock = null;
      }
    } finally {
      try {
        if (advisor != null) {
          advisor.removeMembershipListener(this);
        }
      } catch (IllegalArgumentException ignore) {
      } finally {
        getProcessorKeeper().remove(processorId);
      }
    }
  }

  void remove() {
    getProcessorKeeper().remove(processorId);
  }

  void initialize(LocalRegion theRegion, Object theKey, Object theCallbackArg) {
    region = theRegion;
    regionName = theRegion.getFullPath();
    key = theKey;
    aCallbackArgument = theCallbackArg;
    RegionAttributes attrs = theRegion.getAttributes();
    Scope scope = attrs.getScope();
    if (scope.isDistributed()) {
      advisor = ((CacheDistributionAdvisee) region).getCacheDistributionAdvisor();
      distributionManager = theRegion.getDistributionManager();
      timeout = getSearchTimeout();
      advisor.addMembershipListener(this);
    }
  }

  void setKey(Object key) {
    this.key = key;
  }

  protected void setSelectedNode(InternalDistributedMember selectedNode) {
    this.selectedNode = selectedNode;
    selectedNodeDead = false;
  }

  protected int getTimeout() {
    return timeout;
  }

  protected Object getKey() {
    return key;
  }

  InternalDistributedMember getSelectedNode() {
    return selectedNode;
  }

  /**
   * Even though SearchLoadAndWriteProcessor may be in invoked in the context of a local region,
   * most of the services it provides are relevant to distribution only. The 3 services it provides
   * are netSearch, netLoad, netWrite
   */
  private SearchLoadAndWriteProcessor() {
    resetResults();
    pendingResponders.clear();
    attemptedLocalLoad = false;
    netSearchDone = false;
    isSerialized = false;
    result = null;
    key = null;
    requestInProgress = false;
    netWriteSucceeded = false;
    remoteGetInProgress = false;
    responseQueue = null;
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
      CacheLoader loader = region.basicGetLoader();
      if (loader != null) {
        obj = doLocalLoad(loader, true, preferCD);
        Assert.assertTrue(obj != Token.INVALID && obj != Token.LOCAL_INVALID);
        event.setNewValue(obj);
        isSerialized = false;
        result = obj;
        return;
      }
      if (scope.isLocal()) {
        return;
      }
    }
    netSearchForBlob();
    if (result != null) {
      Assert.assertTrue(result != Token.INVALID && result != Token.LOCAL_INVALID);
      if (isSerialized) {
        event.setSerializedNewValue((byte[]) result);
      } else {
        event.setNewValue(result);
      }
      event.setVersionTag(versionTag);
      return;
    }

    load(event, preferCD);
  }

  /** perform a net-search, setting this.result to the object found in the search */
  private void netSearchForBlob() throws TimeoutException {
    if (netSearchDone) {
      return;
    }
    netSearchDone = true;
    CachePerfStats stats = region.getCachePerfStats();
    long start = 0;
    Set sendSet = null;

    result = null;
    RegionAttributes attrs = region.getAttributes();
    // Object aCallbackArgument = null;
    requestInProgress = true;
    selectedNodeDead = false;
    initRemainingTimeout();
    start = stats.startNetsearch();
    try {
      List<InternalDistributedMember> replicates =
          new ArrayList(advisor.adviseInitializedReplicates());
      if (replicates.size() > 1) {
        Collections.shuffle(replicates);
      }
      for (InternalDistributedMember replicate : replicates) {
        synchronized (pendingResponders) {
          pendingResponders.clear();
        }

        synchronized (this) {
          requestInProgress = true;
          remoteGetInProgress = true;
          setSelectedNode(replicate);
          lastNotifySpot = 0;

          sendValueRequest(replicate);
          waitForObject2(remainingTimeout);

          if (authorative) {
            if (result != null) {
              netSearch = true;
            }
            return;
          } else {
            // clear anything that might have been set by our query.
            selectedNode = null;
            selectedNodeDead = false;
            lastNotifySpot = 0;
            result = null;
          }
        }
      }
      synchronized (membersLock) {
        Set recipients = advisor.adviseNetSearch();
        if (recipients.isEmpty()) {
          return;
        }
        ArrayList list = new ArrayList(recipients);
        Collections.shuffle(list);
        sendSet = new HashSet(list);
        synchronized (pendingResponders) {
          pendingResponders.clear();
          pendingResponders.addAll(list);
        }
      }

      boolean useMulticast = region.getMulticastEnabled() && (region instanceof DistributedRegion)
          && region.getSystem().getConfig().getMcastPort() != 0;

      // moved outside the sync to fix bug 39458
      QueryMessage.sendMessage(this, regionName, key, useMulticast, sendSet,
          remainingTimeout, attrs.getEntryTimeToLive().getTimeout(),
          attrs.getEntryIdleTimeout().getTimeout());

      synchronized (this) {
        // moved this send back into sync to fix bug 37132
        // QueryMessage.sendMessage(this, this.regionName,this.key,useMulticast,
        // sendSet,this.remainingTimeout ,
        // attrs.getEntryTimeToLive().getTimeout(),
        // attrs.getEntryIdleTimeout().getTimeout());
        boolean done = false;
        do {
          waitForObject2(remainingTimeout);
          if (selectedNodeDead && remoteGetInProgress) {
            sendNetSearchRequest();
          } else {
            done = true;
          }
        } while (!done);

        if (result != null) {
          netSearch = true;
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
    RegionAttributes attrs = region.getAttributes();
    Scope scope = attrs.getScope();
    CacheLoader loader = region.basicGetLoader();
    Assert.assertTrue(scope.isDistributed());

    if ((loader != null) && (!scope.isGlobal())) {
      obj = doLocalLoad(loader, false, preferCD);
      event.setNewValue(obj);
      Assert.assertTrue(obj != Token.INVALID && obj != Token.LOCAL_INVALID);
      return;
    }

    if (scope.isGlobal()) {
      Assert.assertTrue(lock == null);
      Set loadCandidatesSet = advisor.adviseNetLoad();
      if ((loader == null) && (loadCandidatesSet.isEmpty())) {
        // no one has a data Loader. No point getting a lock
        return;
      }

      lock = region.getDistributedLock(key);
      boolean locked = false;
      try {
        final CancelCriterion stopper = region.getCancelCriterion();
        for (;;) {
          stopper.checkCancelInProgress(null);
          boolean interrupted = Thread.interrupted();
          try {
            locked = lock.tryLock(region.getCache().getLockTimeout(), TimeUnit.SECONDS);
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
          localLoad = false;
          if (scope.isDistributed()) {
            isSerialized = false;
            obj = doNetLoad();
            Assert.assertTrue(obj != Token.INVALID && obj != Token.LOCAL_INVALID);
            if (isSerialized && obj != null) {
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
        if (!locked) {
          lock = null;
        }
      }
    }
    if (scope.isDistributed()) {
      // long start = System.currentTimeMillis();
      obj = doNetLoad();
      if (isSerialized && obj != null) {
        event.setSerializedNewValue((byte[]) obj);
      } else {
        Assert.assertTrue(obj != Token.INVALID && obj != Token.LOCAL_INVALID);
        event.setNewValue(obj);
      }
      // long end = System.currentTimeMillis();
    }
  }

  Object doNetLoad() throws CacheLoaderException, TimeoutException {
    if (netLoadDone) {
      return null;
    }
    netLoadDone = true;
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
          (InternalDistributedMember[]) list.toArray(new InternalDistributedMember[0]);
      initRemainingTimeout();

      RegionAttributes attrs = region.getAttributes();
      int index = 0;
      boolean stayInLoop = false; // never set to true!
      remoteLoadInProgress = true;
      try {
        do { // the only time this loop repeats is when continue is called
          InternalDistributedMember next = loadCandidates[index++];
          setSelectedNode(next);
          lastNotifySpot = 0;
          requestInProgress = true;
          if (remainingTimeout <= 0) { // @todo this looks wrong; why not a timeout exception?
            break;
          }
          remoteException = null;
          NetLoadRequestMessage.sendMessage(this, regionName, key, aCallbackArgument,
              next, remainingTimeout, attrs.getEntryTimeToLive().getTimeout(),
              attrs.getEntryIdleTimeout().getTimeout());
          waitForObject2(remainingTimeout);
          if (remoteException == null) {
            if (!requestInProgress) {
              // note even if result is null we are done; see bug 39738
              localLoad = false;
              if (result != null) {
                netLoad = true;
              }
              return result;
            } else {
              // Why does the following test for selectedNodeDead?
              // Seems like this will cause us to quit trying netLoad
              // even if we don't have a result yet and have not tried everyone.
              if ((selectedNodeDead) && (index < loadCandidates.length)) {
                continue;
              }
              // otherwise we are done
            }
          } else {
            Throwable cause;
            if (remoteException instanceof TryAgainException) {
              if (index < loadCandidates.length) {
                continue;
              } else {
                break;
              }
            }
            if (remoteException instanceof CacheLoaderException) {
              cause = remoteException.getCause();
            } else {
              cause = remoteException;
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
    if (loader != null && !attemptedLocalLoad) {
      attemptedLocalLoad = true;
      CachePerfStats stats = region.getCachePerfStats();
      LoaderHelper loaderHelper = region.loaderHelperFactory.createLoaderHelper(key,
          aCallbackArgument, netSearchAllowed, true /* netLoadAllowed */, this);
      long statStart = stats.startLoad();
      try {
        obj = loader.load(loaderHelper);
        obj = region.getCache().convertPdxInstanceIfNeeded(obj, preferCD);
      } finally {
        stats.endLoad(statStart);
      }
      if (obj != null) {
        localLoad = true;
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
        if (result == r) {
          result = new EntryEventImpl(r);
        }
        result.setOperation(Operation.UPDATE);
      } else if (op == Operation.PUT_IF_ABSENT) {
        if (result == r) {
          result = new EntryEventImpl(r);
        }
        result.setOperation(Operation.CREATE);
      } else if (op == Operation.REMOVE) {
        if (result == r) {
          result = new EntryEventImpl(r);
        }
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
    localWrite = true;
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
        (InternalDistributedMember[]) list.toArray(new InternalDistributedMember[0]);
    initRemainingTimeout();
    int index = 0;
    do {
      InternalDistributedMember next = writeCandidates[index++];
      Set set = new HashSet();
      set.add(next);
      netWriteSucceeded = false;
      requestInProgress = true;
      remoteException = null;
      NetWriteRequestMessage.sendMessage(this, regionName, remainingTimeout, event, set,
          action);
      if (remainingTimeout <= 0) { // @todo: should this throw a timeout exception?
        break;
      }
      waitForObject2(remainingTimeout);
      if (netWriteSucceeded) {
        netWrite = true;
        break;
      }
      if (remoteException != null) {
        Throwable cause;
        if (remoteException instanceof TryAgainException) {
          if (index < writeCandidates.length) {
            continue;
          } else {
            break;
          }
        }
        if (remoteException instanceof CacheWriterException
            && remoteException.getCause() != null) {
          cause = remoteException.getCause();
        } else {
          cause = remoteException;
        }
        throw new CacheWriterException(
            String.format("While invoking a remote netWrite: %s",
                cause),
            cause);
      }
    } while (index < writeCandidates.length);

    return netWriteSucceeded;
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
    if (remoteLoadInProgress) {
      if (logger.isDebugEnabled()) {
        logger.debug("Ignoring netsearch response from {} because we're now doing a netload",
            sender);
      }
      return;
    }

    if (pendingResponders.isEmpty()) {
      return;
    }
    if (!pendingResponders.remove(sender)) {
      return;
    } else {
      if (logger.isDebugEnabled()) {
        // only log the message if it's from a recipient we're waiting for.
        // it could have been multicast to all members, which would give us
        // one response per member
        logger.debug(
            "Processing response for processorId={}, isPresent is {}, sender is {}, key is {}, value is {}, version is {}",
            processorId, isPresent, sender, key, serialized, versionTag);
      }
    }

    // Another thread got a response and that contained the value.
    // Ignore this response.
    if (result != null) {
      return;
    }

    if (isPresent) {
      if (obj != null) {
        Assert.assertTrue(obj != Token.INVALID && obj != Token.LOCAL_INVALID);
        synchronized (this) {
          result = obj;
          lastModified = lastModifiedTime;
          isSerialized = serialized;
          this.versionTag = versionTag;
          signalDone();
          return;
        }
      } else {
        if (!remoteGetInProgress) {
          // send a message to this responder asking for the value
          // do this on the waiting pool in case the send blocks
          try {
            dm.getExecutors().getWaitingThreadPool().execute(new Runnable() {
              @Override
              public void run() {
                sendValueRequest(sender);
              }
            });
            // need to do this here before releasing sync to fix bug 37132
            requestInProgress = true;
            remoteGetInProgress = true;
            setSelectedNode(sender);
            return; // sendValueRequest does the rest of the work
          } catch (RejectedExecutionException ignore) {
            // just fall through since we must be shutting down.
          }
        }
        if (responseQueue == null) {
          responseQueue = new LinkedList();
        }
        if (logger.isDebugEnabled()) {
          logger.debug("Saving isPresent response, requestInProgress {}", sender);
        }
        responseQueue.add(sender);
      }
    }
    if (requestorTimedOut) {
      signalTimedOut();
    }

    boolean endRequest = pendingResponders.isEmpty() && (!remoteGetInProgress);
    if (endRequest) {
      signalDone();
    }
  }

  protected synchronized void sendValueRequest(final InternalDistributedMember sender) {
    // send a message to this responder asking for the value
    // do this on the waiting pool in case the send blocks
    // Always attempt to send the message to fix bug 37149
    RegionAttributes attrs = region.getAttributes();
    NetSearchRequestMessage.sendMessage(this, regionName, key, sender,
        remainingTimeout, attrs.getEntryTimeToLive().getTimeout(),
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
      result = obj;
      lastModified = lastModifiedTime;
      remoteException = e;
      aCallbackArgument = callbackArg;
      computeRemainingTimeout();
      isSerialized = serialized;
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

    if (requestInProgress) {
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
          result = value;
          lastModified = lastModifiedTime;
          isSerialized = serialized;
          remoteGetInProgress = false;
          this.authorative = authorative;
          this.versionTag = versionTag;
          if (isDebugEnabled) {
            logger.debug("incomingNetSearchReply() - got obj {}", this);
          }
          signalDone();
        }
      } else if (remainingTimeout <= 0) {
        remoteGetInProgress = false;
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
      RegionAttributes attrs = region.getAttributes();
      setSelectedNode(nextResponder);
      requestInProgress = true;
      remoteGetInProgress = true;
      NetSearchRequestMessage.sendMessage(this, regionName, key, nextResponder,
          remainingTimeout, attrs.getEntryTimeToLive().getTimeout(),
          attrs.getEntryIdleTimeout().getTimeout());

    } else {
      remoteGetInProgress = false;
      checkIfDone();

    }
  }

  /**
   * This is the response from the accepted responder.
   */
  protected void incomingNetWriteReply(boolean netWriteSuccessful, Exception e, boolean exe) {
    synchronized (this) {
      remoteException = e;
      netWriteSucceeded = netWriteSuccessful;
      computeRemainingTimeout();
      signalDone();
    }
  }

  private synchronized void initRemainingTimeout() {
    remainingTimeout = timeout * 1000;
    startTimeSnapShot = distributionManager.cacheTimeMillis();
  }

  private synchronized void computeRemainingTimeout() {
    if (startTimeSnapShot > 0) { // @todo this should be an assertion
      endTimeSnapShot = distributionManager.cacheTimeMillis();
      long delta = endTimeSnapShot - startTimeSnapShot;
      if (delta > 0) {
        remainingTimeout -= delta;
      }
      startTimeSnapShot = endTimeSnapShot;
    }
  }

  private synchronized void waitForObject2(final int timeoutMs) throws TimeoutException {
    if (requestInProgress) {
      try {
        final DistributionManager dm =
            region.getCache().getInternalDistributedSystem().getDistributionManager();
        long waitTimeMs = timeoutMs;
        final long endTime = System.currentTimeMillis() + waitTimeMs;
        for (;;) {
          if (!requestInProgress) {
            return;
          }
          if (waitTimeMs <= 0) {
            throw new TimeoutException(
                String.format(
                    "Timed out while doing netsearch/netload/netwrite processorId= %s Key is %s",
                    processorId, key));
          }

          boolean interrupted = Thread.interrupted();
          int lastNS = lastNotifySpot;
          try {
            {
              boolean done = (lastNS != 0);
              while (!done && waitTimeMs > 0) {
                region.getCancelCriterion().checkCancelInProgress(null);
                interrupted = Thread.interrupted() || interrupted;
                long wt = Math.min(RETRY_TIME, waitTimeMs);
                wait(wt); // spurious wakeup ok
                lastNS = lastNotifySpot;
                done = (lastNS != 0);
                if (!done) {
                  // calc remaing wait time to fix bug 37196
                  waitTimeMs = endTime - System.currentTimeMillis();
                }
              } // while
              if (done) {
                lastNotifySpot = 0;
              }
            }
            if (requestInProgress && !selectedNodeDead) {
              // added the test of "!this.selectedNodeDead" for bug 37196
              StringBuilder sb = new StringBuilder(200);
              sb.append("processorId=").append(processorId);
              sb.append(" Key is ").append(key);
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
    netSearch = false;
    netLoad = false;
    localLoad = false;
    localWrite = false;
    netWrite = false;
    lastModified = 0;
    isSerialized = false;
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
    return super.toString() + " processorId " + processorId;
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
      processorId = processor.processorId;
      regionName = theRegionName;
      setMulticast(multicast);
      key = theKey;
      timeoutMs = theTimeoutMs;
      ttl = theTtl;
      idleTime = theIdleTime;
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

    @Override
    public int getDSFID() {
      return QUERY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);

      short flags = 0;
      if (processorId != 0) {
        flags |= HAS_PROCESSOR_ID;
      }
      if (ttl != 0) {
        flags |= HAS_TTL;
      }
      if (idleTime != 0) {
        flags |= HAS_IDLE_TIME;
      }
      if (alwaysSendResult) {
        flags |= ALWAYS_SEND_RESULT;
      }
      out.writeShort(flags);

      if (processorId != 0) {
        out.writeInt(processorId);
      }
      out.writeUTF(regionName);
      DataSerializer.writeObject(key, out);
      out.writeInt(timeoutMs);
      if (ttl != 0) {
        InternalDataSerializer.writeSignedVL(ttl, out);
      }
      if (idleTime != 0) {
        InternalDataSerializer.writeSignedVL(idleTime, out);
      }
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      short flags = in.readShort();
      if ((flags & HAS_PROCESSOR_ID) != 0) {
        processorId = in.readInt();
        ReplyProcessor21.setMessageRPId(processorId);
      }
      regionName = in.readUTF();
      key = DataSerializer.readObject(in);
      timeoutMs = in.readInt();
      if ((flags & HAS_TTL) != 0) {
        ttl = (int) InternalDataSerializer.readSignedVL(in);
      }
      if ((flags & HAS_IDLE_TIME) != 0) {
        idleTime = (int) InternalDataSerializer.readSignedVL(in);
      }
      alwaysSendResult = (flags & ALWAYS_SEND_RESULT) != 0;
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.QueryMessage for \"" + key + "\" in region \""
          + regionName + "\", processorId " + processorId + ", timeoutMs=" + timeoutMs
          + ", ttl=" + ttl + ", idleTime=" + idleTime;
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

      final InitializationLevel oldLevel =
          LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE);
      try {
        // check to see if we would have to wait on initialization latch (if global)
        // if so abort and reply with null
        InternalCache cache = dm.getExistingCache();
        if (cache.isGlobalRegionInitializing(regionName)) {
          replyWithNull(dm);
          if (logger.isDebugEnabled()) {
            logger.debug("Global Region not initialized yet");
          }
          return;
        }

        LocalRegion region = (LocalRegion) dm.getExistingCache().getRegion(regionName);
        Object o = null;

        if (region != null) {
          setClearCountReference(region);
          try {
            RegionEntry entry = region.basicGetEntry(key);
            if (entry != null) {
              synchronized (entry) {
                assert region.isInitialized();
                if (dm.cacheTimeMillis() - startTime < timeoutMs) {
                  o = region.getNoLRU(key, false, true, true); // OFFHEAP: incrc, copy bytes,
                                                               // decrc
                  if (o != null && !Token.isInvalid(o) && !Token.isRemoved(o)
                      && !region.isExpiredWithRegardTo(key, ttl, idleTime)) {
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
                    if (isPresent && (alwaysSendResult
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
        ResponseMessage.sendMessage(key, getSender(), processorId,
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
      ResponseMessage.sendMessage(key, getSender(), processorId, null, 0, false, false,
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
      key = theKey;
      processorId = theProcessorId;
      result = theResult;
      lastModified = lastModifiedTime;
      isPresent = ispresent;
      isSerialized = isserialized;
      requestorTimedOut = requestorTimedOutFlag;
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
      processor = (SearchLoadAndWriteProcessor) getProcessorKeeper().retrieve(processorId);
      if (processor == null) {
        if (logger.isDebugEnabled()) {
          logger.debug("Response() SearchLoadAndWriteProcessor no longer exists");
        }
        return;
      }
      long lastModifiedSystemTime = 0;
      if (lastModified != 0) {
        lastModifiedSystemTime = lastModified;
      }
      if (versionTag != null) {
        versionTag.replaceNullIDs(getSender());
      }

      processor.incomingResponse(result, lastModifiedSystemTime, isPresent,
          isSerialized, requestorTimedOut, getSender(), dm, versionTag);
    }

    @Override
    public boolean getInlineProcess() { // optimization for bug 37075
      return true;
    }

    @Override
    public int getDSFID() {
      return RESPONSE_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      DataSerializer.writeObject(key, out);
      out.writeInt(processorId);
      DataSerializer.writeObject(result, out);
      out.writeLong(lastModified);
      out.writeBoolean(isPresent);
      out.writeBoolean(isSerialized);
      out.writeBoolean(requestorTimedOut);
      DataSerializer.writeObject(versionTag, out);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      key = DataSerializer.readObject(in);
      processorId = in.readInt();
      result = DataSerializer.readObject(in);
      lastModified = in.readLong();
      isPresent = in.readBoolean();
      isSerialized = in.readBoolean();
      requestorTimedOut = in.readBoolean();
      versionTag = DataSerializer.readObject(in);
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.ResponseMessage for processorId " + processorId
          + ", blob is " + result + ", isPresent is " + isPresent + ", requestorTimedOut is "
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
      processorId = processor.processorId;
      regionName = theRegionName;
      key = theKey;
      timeoutMs = timeoutMS;
      ttl = ttlMS;
      idleTime = idleTimeMS;
      Assert.assertTrue(processor.region.getScope().isDistributed());
    }

    /** Invoked on the node that has the object */
    @Override
    protected void process(ClusterDistributionManager dm) {
      doGet(dm);
    }

    @Override
    public int getDSFID() {
      return NET_SEARCH_REQUEST_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);

      short flags = 0;
      if (processorId != 0) {
        flags |= HAS_PROCESSOR_ID;
      }
      if (ttl != 0) {
        flags |= HAS_TTL;
      }
      if (idleTime != 0) {
        flags |= HAS_IDLE_TIME;
      }
      out.writeShort(flags);

      if (processorId != 0) {
        out.writeInt(processorId);
      }
      out.writeUTF(regionName);
      DataSerializer.writeObject(key, out);
      out.writeInt(timeoutMs);
      if (ttl != 0) {
        InternalDataSerializer.writeSignedVL(ttl, out);
      }
      if (idleTime != 0) {
        InternalDataSerializer.writeSignedVL(idleTime, out);
      }
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      short flags = in.readShort();
      if ((flags & HAS_PROCESSOR_ID) != 0) {
        processorId = in.readInt();
        ReplyProcessor21.setMessageRPId(processorId);
      }
      regionName = in.readUTF();
      key = DataSerializer.readObject(in);
      timeoutMs = in.readInt();
      if ((flags & HAS_TTL) != 0) {
        ttl = (int) InternalDataSerializer.readSignedVL(in);
      }
      if ((flags & HAS_IDLE_TIME) != 0) {
        idleTime = (int) InternalDataSerializer.readSignedVL(in);
      }
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.NetSearchRequestMessage for \"" + key
          + "\" in region \"" + regionName + "\", processorId " + processorId;
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

      InternalCache cache = dm.getExistingCache();

      final InitializationLevel oldLevel =
          LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE);
      LocalRegion region = (LocalRegion) cache.getRegion(regionName);
      CachePerfStats stats =
          region == null ? cache.getCachePerfStats() : region.getRegionPerfStats();
      long startHandlingTime = stats.startHandlingNetsearch();
      boolean handlingSuccess = false;
      try {
        if (region != null) {
          setClearCountReference(region);
          try {
            boolean initialized = region.isInitialized();
            RegionEntry entry = region.basicGetEntry(key);
            if (entry != null) {
              synchronized (entry) {
                // get the value and version under synchronization so they don't change
                VersionStamp versionStamp = entry.getVersionStamp();
                if (versionStamp != null) {
                  versionTag = versionStamp.asVersionTag();
                }
                Object eov = region.getNoLRU(key, false, true, true); // OFFHEAP: incrc, copy
                // bytes, decrc
                if (eov != null) {
                  if (eov == Token.INVALID || eov == Token.LOCAL_INVALID) {
                    // nothing?
                  } else if (dm.cacheTimeMillis() - startTime < timeoutMs) {
                    if (!region.isExpiredWithRegardTo(key, ttl, idleTime)) {
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
                    handlingSuccess = true;
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
        NetSearchReplyMessage.sendMessage(getSender(), processorId,
            key, ebv, ebvObj, ebvLen, lastModifiedCacheTime, isSer, requestorTimedOut,
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
        stats.endHandlingNetsearch(startHandlingTime, handlingSuccess);
      }
    }

    private void replyWithNull(ClusterDistributionManager dm) {
      NetSearchReplyMessage.sendMessage(getSender(), processorId,
          key, null, null, 0, 0, false, false, false, dm, null);
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
      processorId = procId;
      value = theValue;
      valueObj = theValueObj;
      valueLen = valueObjLen;
      lastModified = lastModifiedTime;
      isSerialized = isserialized;
      requestorTimedOut = requestorTimedout;
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
      if (lastModified != 0) {
        lastModifiedSystemTime = lastModified;
      }
      if (versionTag != null) {
        versionTag.replaceNullIDs(getSender());
      }
      processor.incomingNetSearchReply(value, lastModifiedSystemTime, isSerialized,
          requestorTimedOut, authoritative, versionTag, getSender());
    }

    @Override
    public int getDSFID() {
      return NET_SEARCH_REPLY_MESSAGE;
    }


    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(processorId);
      if (valueObj != null) {
        DataSerializer.writeObjectAsByteArray(valueObj, out);
      } else {
        DataSerializer.writeByteArray(value, valueLen, out);
      }
      out.writeLong(lastModified);
      byte booleans = 0;
      if (isSerialized) {
        booleans |= SERIALIZED;
      }
      if (requestorTimedOut) {
        booleans |= REQUESTOR_TIMEOUT;
      }
      if (authoritative) {
        booleans |= AUTHORATIVE;
      }
      if (versionTag != null) {
        booleans |= VERSIONED;
      }
      if (versionTag instanceof DiskVersionTag) {
        booleans |= PERSISTENT;
      }
      out.writeByte(booleans);
      if (versionTag != null) {
        InternalDataSerializer.invokeToData(versionTag, out);
      }
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      processorId = in.readInt();
      value = DataSerializer.readByteArray(in);
      if (value != null) {
        valueLen = value.length;
      }
      lastModified = in.readLong();
      byte booleans = in.readByte();

      isSerialized = (booleans & SERIALIZED) != 0;
      requestorTimedOut = (booleans & REQUESTOR_TIMEOUT) != 0;
      authoritative = (booleans & AUTHORATIVE) != 0;
      if ((booleans & VERSIONED) != 0) {
        boolean persistentTag = (booleans & PERSISTENT) != 0;
        versionTag = VersionTag.create(persistentTag, in);
      }
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.NetSearchReplyMessage for processorId " + processorId
          + ", blob is " +
          // this.value
          (value == null ? "null" : "(" + value.length + " bytes)") + " authorative="
          + authoritative + " versionTag=" + versionTag;
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
      processorId = processor.processorId;
      regionName = theRegionName;
      key = theKey;
      aCallbackArgument = callbackArgument;
      timeoutMs = timeoutMS;
      ttl = ttlMS;
      idleTime = idleTimeMS;
      Assert.assertTrue(processor.region.getScope().isDistributed());
    }

    /** Invoked on the node that has the object */
    @Override
    protected void process(ClusterDistributionManager dm) {
      doLoad(dm);
    }

    @Override
    public int getDSFID() {
      return NET_LOAD_REQUEST_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(processorId);
      out.writeUTF(regionName);
      DataSerializer.writeObject(key, out);
      DataSerializer.writeObject(aCallbackArgument, out);
      out.writeInt(timeoutMs);
      out.writeInt(ttl);
      out.writeInt(idleTime);

    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      processorId = in.readInt();
      regionName = in.readUTF();
      key = DataSerializer.readObject(in);
      aCallbackArgument = DataSerializer.readObject(in);
      timeoutMs = in.readInt();
      ttl = in.readInt();
      idleTime = in.readInt();
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.NetLoadRequestMessage for \"" + key
          + "\" in region \"" + regionName + "\", processorId " + processorId;
    }

    private void doLoad(ClusterDistributionManager dm) {
      long startTime = dm.cacheTimeMillis();
      final InitializationLevel oldLevel =
          LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE);
      try {
        InternalCache gfc = dm.getExistingCache();
        LocalRegion region = (LocalRegion) gfc.getRegion(regionName);
        if (region != null && region.isInitialized()
            && (dm.cacheTimeMillis() - startTime < timeoutMs)) {
          CacheLoader loader = region.basicGetLoader();
          if (loader != null) {
            LoaderHelper loaderHelper = region.loaderHelperFactory.createLoaderHelper(key,
                aCallbackArgument, false, false, null);
            CachePerfStats stats = region.getCachePerfStats();
            long start = stats.startLoad();
            try {
              Object o = loader.load(loaderHelper);
              // no need to call convertPdxInstanceIfNeeded since we are serializing
              // this into the NetLoadRequestMessage. The loaded object will be deserialized
              // on the other side and have the correct form in that member.
              Assert.assertTrue(o != Token.INVALID && o != Token.LOCAL_INVALID);
              NetLoadReplyMessage.sendMessage(getSender(), processorId,
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
      NetLoadReplyMessage.sendMessage(getSender(), processorId, null, dm,
          aCallbackArgument, e, false, false);
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
      processorId = procId;
      result = obj;
      e = exe;
      aCallbackArgument = callbackArgument;
      isSerialized = isserialized;
      requestorTimedOut = requestorTimedout;
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
      processor.incomingNetLoadReply(result, 0, aCallbackArgument, e,
          isSerialized, requestorTimedOut);
    }

    @Override
    public int getDSFID() {
      return NET_LOAD_REPLY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(processorId);
      boolean isSerialized = this.isSerialized;
      if (result instanceof byte[]) {
        DataSerializer.writeByteArray((byte[]) result, out);
      } else {
        DataSerializer.writeObjectAsByteArray(result, out);
        isSerialized = true;
      }
      DataSerializer.writeObject(aCallbackArgument, out);
      DataSerializer.writeObject(e, out);
      out.writeBoolean(isSerialized);
      out.writeBoolean(requestorTimedOut);

    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      processorId = in.readInt();
      result = DataSerializer.readByteArray(in);
      aCallbackArgument = DataSerializer.readObject(in);
      e = DataSerializer.readObject(in);
      isSerialized = in.readBoolean();
      requestorTimedOut = in.readBoolean();

    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.NetLoadReplyMessage for processorId " + processorId
          + ", blob is " + result;
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
      processorId = processor.processorId;
      regionName = theRegionName;
      timeoutMs = timeoutMS;
      event = theEvent;
      action = actionType;
      Assert.assertTrue(processor.region.getScope().isDistributed());
    }

    @Override
    public int getDSFID() {
      return NET_WRITE_REQUEST_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(processorId);
      out.writeUTF(regionName);
      out.writeInt(timeoutMs);
      DataSerializer.writeObject(event, out);
      out.writeInt(action);

    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      processorId = in.readInt();
      regionName = in.readUTF();
      timeoutMs = in.readInt();
      event = DataSerializer.readObject(in);
      action = in.readInt();
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.NetWriteRequestMessage " + " for region \""
          + regionName + "\", processorId " + processorId;
    }

    /** Invoked on the node that has the object */
    @Override
    protected void process(ClusterDistributionManager dm) {
      long startTime = dm.cacheTimeMillis();
      final InitializationLevel oldLevel =
          LocalRegion.setThreadInitLevelRequirement(BEFORE_INITIAL_IMAGE);
      try {
        InternalCache gfc = dm.getExistingCache();
        LocalRegion region = (LocalRegion) gfc.getRegion(regionName);
        if (region != null && region.isInitialized()
            && (dm.cacheTimeMillis() - startTime < timeoutMs)) {
          CacheWriter writer = region.basicGetWriter();
          EntryEventImpl entryEvtImpl = null;
          RegionEventImpl regionEvtImpl = null;
          if (event instanceof EntryEventImpl) {
            entryEvtImpl = (EntryEventImpl) event;
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
          } else if (event instanceof RegionEventImpl) {
            regionEvtImpl = (RegionEventImpl) event;
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
              NetWriteReplyMessage.sendMessage(getSender(), processorId,
                  dm, true, null, false);
            } catch (CacheWriterException cwe) {
              NetWriteReplyMessage.sendMessage(getSender(), processorId,
                  dm, false, cwe, true);
            } catch (Exception e) {
              NetWriteReplyMessage.sendMessage(getSender(), processorId,
                  dm, false, e, false);
            } finally {
              if (entryEvtImpl != null) {
                entryEvtImpl.setReadOldValueFromDisk(false);
              }
            }

          } else {
            NetWriteReplyMessage.sendMessage(getSender(), processorId,
                dm, false,
                new TryAgainException(
                    "No cachewriter defined"),
                true);
          }

        } else {
          NetWriteReplyMessage.sendMessage(getSender(), processorId, dm,
              false,
              new TryAgainException("Timeout expired or region not ready"),
              true);

        }
      } catch (RegionDestroyedException ignore) {
        NetWriteReplyMessage.sendMessage(getSender(), processorId, dm,
            false, null, false);

      } catch (DistributedSystemDisconnectedException e) {
        // shutdown condition
        throw e;
      } catch (CancelException cce) {
        dm.getCancelCriterion().checkCancelInProgress(cce); // TODO anyway to find the region or
                                                            // cache here?
        NetWriteReplyMessage.sendMessage(getSender(), processorId, dm,
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
        NetWriteReplyMessage.sendMessage(getSender(), processorId, dm,
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
      processorId = procId;
      netWriteSucceeded = netwriteSucceeded;
      e = except;
      cacheWriterException = cacheWriterExcept;
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
      processor.incomingNetWriteReply(netWriteSucceeded, e, cacheWriterException);
    }

    @Override
    public int getDSFID() {
      return NET_WRITE_REPLY_MESSAGE;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      super.toData(out, context);
      out.writeInt(processorId);
      out.writeBoolean(netWriteSucceeded);
      DataSerializer.writeObject(e, out);
      out.writeBoolean(cacheWriterException);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      super.fromData(in, context);
      processorId = in.readInt();
      netWriteSucceeded = in.readBoolean();
      e = DataSerializer.readObject(in);
      cacheWriterException = in.readBoolean();
    }

    @Override
    public String toString() {
      return "SearchLoadAndWriteProcessor.NetWriteReplyMessage for processorId " + processorId;
    }
  }
}
