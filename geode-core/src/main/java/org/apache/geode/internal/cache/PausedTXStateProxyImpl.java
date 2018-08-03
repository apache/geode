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

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.geode.cache.CommitConflictException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.TransactionId;
import org.apache.geode.cache.UnsupportedOperationInTransactionException;
import org.apache.geode.cache.client.internal.ServerRegionDataAccess;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.tier.sockets.ClientProxyMembershipID;
import org.apache.geode.internal.cache.tier.sockets.VersionedObjectList;
import org.apache.geode.internal.cache.tx.TransactionalOperation.ServerRegionOperation;

public class PausedTXStateProxyImpl implements TXStateProxy {

  @Override
  public TransactionId getTransactionId() {
    return null;
  }

  @Override
  public TXRegionState readRegion(InternalRegion r) {
    return null;
  }

  @Override
  public TXRegionState writeRegion(InternalRegion r) {
    return null;
  }

  @Override
  public long getBeginTime() {
    return 0;
  }

  @Override
  public int getChanges() {
    return 0;
  }

  @Override
  public boolean isInProgress() {
    return false;
  }

  @Override
  public int nextModSerialNum() {
    return 0;
  }

  @Override
  public boolean needsLargeModCount() {
    return false;
  }

  @Override
  public void precommit()
      throws CommitConflictException, UnsupportedOperationInTransactionException {}

  @Override
  public void commit() throws CommitConflictException {}

  @Override
  public void rollback() {}

  @Override
  public List getEvents() {
    return null;
  }

  @Override
  public InternalCache getCache() {
    return null;
  }

  @Override
  public Collection<InternalRegion> getRegions() {
    return null;
  }

  @Override
  public void invalidateExistingEntry(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) {}

  @Override
  public Entry getEntry(KeyInfo keyInfo, LocalRegion region, boolean allowTombstones) {
    return null;
  }

  @Override
  public Object getDeserializedValue(KeyInfo keyInfo, LocalRegion localRegion, boolean updateStats,
      boolean disableCopyOnRead, boolean preferCD, EntryEventImpl clientEvent,
      boolean returnTombstones, boolean retainResult) {
    return null;
  }

  @Override
  public TXEvent getEvent() {
    return null;
  }

  @Override
  public TXRegionState txWriteRegion(InternalRegion internalRegion, KeyInfo entryKey) {
    return null;
  }

  @Override
  public TXRegionState txReadRegion(InternalRegion internalRegion) {
    return null;
  }

  @Override
  public boolean txPutEntry(EntryEventImpl event, boolean ifNew, boolean requireOldValue,
      boolean checkResources, Object expectedOldValue) {
    return false;
  }

  @Override
  public TXEntryState txReadEntry(KeyInfo entryKey, LocalRegion localRegion, boolean rememberRead,
      boolean createTxEntryIfAbsent) {
    return null;
  }

  @Override
  public void rmRegion(LocalRegion r) {

  }

  @Override
  public boolean isInProgressAndSameAs(TXStateInterface state) {
    return false;
  }

  @Override
  public boolean isFireCallbacks() {
    return false;
  }

  @Override
  public ReentrantLock getLock() {
    return null;
  }

  @Override
  public boolean isRealDealLocal() {
    return false;
  }

  @Override
  public boolean isMemberIdForwardingRequired() {
    return false;
  }

  @Override
  public InternalDistributedMember getOriginatingMember() {
    return null;
  }

  @Override
  public TXCommitMessage getCommitMessage() {
    return null;
  }

  @Override
  public void close() {}

  @Override
  public boolean isTxState() {
    return false;
  }

  @Override
  public boolean isTxStateStub() {
    return false;
  }

  @Override
  public boolean isTxStateProxy() {
    return false;
  }

  @Override
  public boolean isDistTx() {
    return false;
  }

  @Override
  public boolean isCreatedOnDistTxCoordinator() {
    return false;
  }

  @Override
  public void beforeCompletion() {}

  @Override
  public void afterCompletion(int status) {}

  @Override
  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) throws EntryNotFoundException {}

  @Override
  public int entryCount(LocalRegion localRegion) {
    return 0;
  }

  @Override
  public Object getValueInVM(KeyInfo keyInfo, LocalRegion localRegion, boolean rememberRead) {
    return null;
  }

  @Override
  public boolean containsKey(KeyInfo keyInfo, LocalRegion localRegion) {
    return false;
  }

  @Override
  public boolean containsValueForKey(KeyInfo keyInfo, LocalRegion localRegion) {
    return false;
  }

  @Override
  public Entry getEntryOnRemote(KeyInfo key, LocalRegion localRegion, boolean allowTombstones)
      throws DataLocationException {
    return null;
  }

  @Override
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
    return false;
  }

  @Override
  public boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) throws DataLocationException {
    return false;
  }

  @Override
  public void destroyOnRemote(EntryEventImpl event, boolean cacheWrite, Object expectedOldValue)
      throws DataLocationException {}

  @Override
  public void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) throws DataLocationException {}

  @Override
  public boolean isDeferredStats() {
    return false;
  }

  @Override
  public Object findObject(KeyInfo key, LocalRegion r, boolean isCreate, boolean generateCallbacks,
      Object value, boolean disableCopyOnRead, boolean preferCD,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) {
    return null;
  }

  @Override
  public Object getEntryForIterator(KeyInfo key, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones) {
    return null;
  }

  @Override
  public Object getKeyForIterator(KeyInfo keyInfo, LocalRegion currRgn, boolean rememberReads,
      boolean allowTombstones) {
    return null;
  }

  @Override
  public Set getAdditionalKeysForIterator(LocalRegion currRgn) {
    return null;
  }

  @Override
  public Collection<?> getRegionKeysForIteration(LocalRegion currRegion) {
    return null;
  }

  @Override
  public Object getSerializedValue(LocalRegion localRegion, KeyInfo key, boolean doNotLockEntry,
      ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent,
      boolean returnTombstones) throws DataLocationException {
    return null;
  }

  @Override
  public void checkSupportsRegionDestroy() throws UnsupportedOperationInTransactionException {}

  @Override
  public void checkSupportsRegionInvalidate() throws UnsupportedOperationInTransactionException {}

  @Override
  public void checkSupportsRegionClear() throws UnsupportedOperationInTransactionException {}

  @Override
  public Set getBucketKeys(LocalRegion localRegion, int bucketId, boolean allowTombstones) {
    return null;
  }

  @Override
  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,
      InternalRegion reg) {}

  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps,
      InternalRegion reg) {}

  @Override
  public Entry accessEntry(KeyInfo keyInfo, LocalRegion localRegion) {
    return null;
  }

  @Override
  public void updateEntryVersion(EntryEventImpl event) throws EntryNotFoundException {}

  @Override
  public void checkJTA(String errmsg) throws IllegalStateException {}

  @Override
  public void setIsJTA(boolean isJTA) {}

  @Override
  public TXId getTxId() {
    return null;
  }

  @Override
  public TXManagerImpl getTxMgr() {
    return null;
  }

  @Override
  public void setLocalTXState(TXStateInterface state) {}

  @Override
  public void setTarget(DistributedMember target) {}

  @Override
  public DistributedMember getTarget() {
    return null;
  }

  @Override
  public boolean isCommitOnBehalfOfRemoteStub() {
    return false;
  }

  @Override
  public boolean setCommitOnBehalfOfRemoteStub(boolean requestedByOwner) {
    return false;
  }

  @Override
  public boolean isOnBehalfOfClient() {
    return false;
  }

  @Override
  public boolean isJCATransaction() {
    return false;
  }

  @Override
  public void setJCATransaction() {}

  @Override
  public void suspend() {}

  @Override
  public void resume() {}

  @Override
  public void recordTXOperation(ServerRegionDataAccess proxy, ServerRegionOperation op, Object key,
      Object[] arguments) {}

  @Override
  public int operationCount() {
    return 0;
  }

  @Override
  public void setInProgress(boolean progress) {}

  @Override
  public void updateProxyServer(InternalDistributedMember proxy) {}

  @Override
  public InternalDistributedMember getOnBehalfOfClientMember() {
    return null;
  }
}
