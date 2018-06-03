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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.geode.CancelCriterion;
import org.apache.geode.Statistics;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionExistsException;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.client.internal.ServerRegionProxy;
import org.apache.geode.cache.query.internal.index.IndexManager;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.eviction.EvictionController;
import org.apache.geode.internal.cache.persistence.DiskExceptionHandler;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.versions.RegionVersionVector;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.util.concurrent.StoppableCountDownLatch;

/**
 * Interface to be used instead of type-casting to LocalRegion.
 *
 * <p>
 * The following interfaces are implemented by LocalRegion and may need to be extended by
 * InternalRegion to completely allow code to move to using InternalRegion:
 * <ul>
 * <li>RegionAttributes
 * <li>AttributesMutator
 * <li>CacheStatistics
 * <li>DataSerializableFixedID
 * <li>RegionEntryContext
 * <li>Extensible
 * </ul>
 */
@SuppressWarnings("rawtypes")
public interface InternalRegion extends Region, HasCachePerfStats, RegionEntryContext,
    RegionAttributes, HasDiskRegion, RegionMapOwner, DiskExceptionHandler {

  CachePerfStats getCachePerfStats();

  DiskRegion getDiskRegion();

  RegionEntry getRegionEntry(Object key);

  RegionVersionVector getVersionVector();

  long cacheTimeMillis();

  Object getValueInVM(Object key) throws EntryNotFoundException;

  Object getValueOnDisk(Object key) throws EntryNotFoundException;

  void dispatchListenerEvent(EnumListenerEvent op, InternalCacheEvent event);

  boolean isUsedForPartitionedRegionAdmin();

  ImageState getImageState();

  VersionSource getVersionMember();

  long updateStatsForPut(RegionEntry entry, long lastModified, boolean lruRecentUse);

  FilterProfile getFilterProfile();

  ServerRegionProxy getServerProxy();

  void unscheduleTombstone(RegionEntry entry);

  void scheduleTombstone(RegionEntry entry, VersionTag destroyedVersion);

  boolean isEntryExpiryPossible();

  void addExpiryTaskIfAbsent(RegionEntry entry);

  /**
   * Used by unit tests to get access to the EntryExpiryTask of the given key. Returns null if the
   * entry exists but does not have an expiry task.
   *
   * @throws EntryNotFoundException if no entry exists key.
   */
  EntryExpiryTask getEntryExpiryTask(Object key);

  DistributionManager getDistributionManager();

  void generateAndSetVersionTag(InternalCacheEvent event, RegionEntry entry);

  boolean cacheWriteBeforeDestroy(EntryEventImpl event, Object expectedOldValue)
      throws CacheWriterException, EntryNotFoundException, TimeoutException;

  void recordEvent(InternalCacheEvent event);

  boolean isProxy();

  Lock getClientMetaDataLock();

  IndexManager getIndexManager();

  boolean isThisRegionBeingClosedOrDestroyed();

  CancelCriterion getCancelCriterion();

  boolean isIndexCreationThread();

  int updateSizeOnEvict(Object key, int oldSize);

  RegionEntry basicGetEntry(Object key);

  void invokePutCallbacks(final EnumListenerEvent eventType, final EntryEventImpl event,
      final boolean callDispatchListenerEvent, boolean notifyGateways);

  void invokeDestroyCallbacks(final EnumListenerEvent eventType, final EntryEventImpl event,
      final boolean callDispatchListenerEvent, boolean notifyGateways);

  void invokeInvalidateCallbacks(final EnumListenerEvent eventType, final EntryEventImpl event,
      final boolean callDispatchListenerEvent);

  long getTotalEvictions();

  Region createSubregion(String subregionName, RegionAttributes attrs,
      InternalRegionArguments internalRegionArgs)
      throws RegionExistsException, TimeoutException, IOException, ClassNotFoundException;

  void addCacheServiceProfile(CacheServiceProfile profile);

  void setEvictionMaximum(int maximum);

  /**
   * Returns null if the region is not configured for eviction otherwise returns the Statistics used
   * to measure eviction activity.
   */
  Statistics getEvictionStatistics();

  long getEvictionCounter();

  RegionMap getRegionMap();

  InternalDistributedSystem getSystem();

  int getRegionSize();

  void basicDestroyBeforeRemoval(RegionEntry entry, EntryEventImpl event);

  void basicDestroyPart2(RegionEntry re, EntryEventImpl event, boolean inTokenMode,
      boolean conflictWithClear, boolean duringRI, boolean invokeCallbacks);

  void notifyTimestampsToGateways(EntryEventImpl event);

  boolean bridgeWriteBeforeDestroy(EntryEventImpl event, Object expectedOldValue)
      throws CacheWriterException, EntryNotFoundException, TimeoutException;

  void checkEntryNotFound(Object entryKey);

  void rescheduleTombstone(RegionEntry entry, VersionTag version);

  /**
   * Throws CacheClosedException or RegionDestroyedException
   */
  void checkReadiness();

  void basicDestroyPart3(RegionEntry re, EntryEventImpl event, boolean inTokenMode,
      boolean duringRI, boolean invokeCallbacks, Object expectedOldValue);

  void cancelExpiryTask(RegionEntry regionEntry);

  boolean hasServerProxy();

  int calculateRegionEntryValueSize(RegionEntry re);

  void updateSizeOnRemove(Object key, int oldSize);

  boolean isEntryEvictionPossible();

  KeyInfo getKeyInfo(Object key);

  void waitOnInitialization();

  void waitOnInitialization(StoppableCountDownLatch latch);

  Set basicSubregions(boolean recursive);

  boolean isSecret();

  boolean isUsedForMetaRegion();

  boolean isInternalRegion();

  void handleCacheClose(Operation op);

  /**
   * Execute any validation required prior to initializing the region.
   * This method should throw an exception whenever an invalid configuration is detected.
   */
  default void preInitialize() {
    // Do nothing by default.
  }

  void initialize(InputStream snapshotInputStream, InternalDistributedMember imageTarget,
      InternalRegionArguments internalRegionArgs)
      throws TimeoutException, IOException, ClassNotFoundException;

  void cleanupFailedInitialization();

  void postCreateRegion();

  Region getSubregion(String string, boolean b);

  boolean checkForInitialization();

  boolean isUsedForPartitionedRegionBucket();

  Set<String> getAllGatewaySenderIds();

  void senderCreated();

  boolean isInitialized();

  void cleanupForClient(CacheClientNotifier ccn, ClientProxyMembershipID client);

  InternalDistributedMember getMyId();

  /**
   * The default Region implementation will generate EvenTID in the EntryEvent object. This method
   * is overridden in special Region objects like HARegion or
   * SingleWriteSingleReadRegionQueue.SingleReadWriteMetaRegion
   * to return false as the event propagation from those regions do not need EventID objects. This
   * method is made abstract to directly use it in clear operations. (clear and localclear)
   *
   * @return boolean indicating whether to generate eventID or not
   */
  boolean generateEventID();

  boolean containsTombstone(Object key);

  CacheWriter basicGetWriter();

  void basicPutPart3(EntryEventImpl event, RegionEntry regionEntry, boolean isInitialized,
      long lastModifiedTime, boolean invokeListeners, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue);

  long basicPutPart2(EntryEventImpl event, RegionEntry re, boolean isInitialized,
      long lastModifiedTime, boolean clearOccured);

  int calculateValueSize(Object v);

  void cacheWriteBeforePut(EntryEventImpl event, Set netWriteRecipients, CacheWriter cacheWriter,
      boolean requireOldValue, Object expectedOldValue);

  void updateSizeOnPut(Object key, int oldSize, int newBucketSize);

  void updateSizeOnCreate(Object key, int newBucketSize);

  boolean isCopyOnRead();

  Object getValueInVMOrDiskWithoutFaultIn(Object key);

  boolean isRegionInvalid();

  void setRegionInvalid(boolean b);

  ObjectSizer getObjectSizer();

  boolean hasSeenEvent(EntryEventImpl entryEvent);

  TXId getTXId();

  KeyInfo getKeyInfo(Object key, Object newVal, Object callbackArgument);

  void invokeTXCallbacks(EnumListenerEvent afterDestroy, EntryEventImpl ee, boolean b);

  LocalRegion getPartitionedRegion();

  void checkIfAboveThreshold(EntryEventImpl event);

  LocalRegion getDataRegionForRead(KeyInfo keyInfo);

  InternalRegion getDataRegionForWrite(KeyInfo keyInfo);

  TXEntryState createReadEntry(TXRegionState txr, KeyInfo keyInfo, boolean createIfAbsent);

  void syncBulkOp(Runnable task, EventID eventId);

  Object getDataView();

  boolean basicPut(EntryEventImpl ev, boolean b, boolean b1, Object o, boolean b2);

  void basicDestroy(EntryEventImpl ev, boolean b, Object o);

  DistributedMember getOwnerForKey(KeyInfo key);

  boolean isMetaRegionWithTransactions();

  void setInUseByTransaction(boolean b);

  boolean txLRUStart();

  void txLRUEnd();

  void txDecRefCount(RegionEntry refCountEntry);

  Object getDisplayName();

  Object basicGetEntryUserAttribute(Object key);

  boolean requiresReliabilityCheck();

  boolean lockGII();

  void unlockGII();

  boolean hasSeenEvent(EventID eventID);

  void txApplyDestroy(Object key, TransactionId rmtOrigin, TXRmtEvent event,
      boolean needTokensForGII, Operation op, EventID eventId, Object aCallbackArgument,
      List<EntryEventImpl> pendingCallbacks, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, boolean isOriginRemote, TXEntryState txEntryState,
      VersionTag versionTag, long tailKey);


  void txApplyInvalidate(Object key, Object newValue, boolean didDestroy,
      TransactionId transactionId, TXRmtEvent event, boolean localOp, EventID eventId,
      Object aCallbackArgument, List<EntryEventImpl> pendingCallbacks,
      FilterRoutingInfo filterRoutingInfo, ClientProxyMembershipID bridgeContext,
      TXEntryState txEntryState, VersionTag versionTag, long tailKey);

  void txApplyPut(Operation putOp, Object key, Object newValue, boolean didDestroy,
      TransactionId transactionId, TXRmtEvent event, EventID eventId, Object aCallbackArgument,
      List<EntryEventImpl> pendingCallbacks, FilterRoutingInfo filterRoutingInfo,
      ClientProxyMembershipID bridgeContext, TXEntryState txEntryState, VersionTag versionTag,
      long tailKey);

  void txApplyPutPart2(RegionEntry regionEntry, Object key, long lastModified, boolean isCreate,
      boolean didDestroy, boolean clearConflict);

  void txApplyPutHandleDidDestroy(Object key);

  void handleReliableDistribution(Set successfulRecipients);

  StoppableCountDownLatch getInitializationLatchBeforeGetInitialImage();

  StoppableCountDownLatch getInitializationLatchAfterGetInitialImage();

  boolean mapDestroy(EntryEventImpl event, boolean cacheWrite, boolean b, Object expectedOldValue);

  boolean virtualPut(EntryEventImpl event, boolean ifNew, boolean ifOld, Object expectedOldValue,
      boolean requireOldValue, long lastModified, boolean overwriteDestroyed);

  long postPutAllSend(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts);

  void postPutAllFireEvents(DistributedPutAllOperation putallOp,
      VersionedObjectList successfulPuts);

  long postRemoveAllSend(DistributedRemoveAllOperation op, VersionedObjectList successfulOps);

  void postRemoveAllFireEvents(DistributedRemoveAllOperation op, VersionedObjectList successfulOps);

  VersionTag findVersionTagForEvent(EventID eventId);

  Object getIMSync();

  IndexManager setIndexManager(IndexManager idxMgr);

  RegionTTLExpiryTask getRegionTTLExpiryTask();

  RegionIdleExpiryTask getRegionIdleExpiryTask();

  boolean isAllEvents();

  boolean shouldDispatchListenerEvent();

  boolean shouldNotifyBridgeClients();

  default Set adviseNetWrite() {
    return null;
  }

  EvictionController getEvictionController();

  default void handleWANEvent(EntryEventImpl event) {}

}
