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
/**
 * File comment
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CommitConflictException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.SynchronizationCommitConflictException;
import com.gemstone.gemfire.cache.TransactionDataNodeHasDepartedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.TransactionException;
import com.gemstone.gemfire.cache.TransactionId;
import com.gemstone.gemfire.cache.UnsupportedOperationInTransactionException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.ReliableReplyException;
import com.gemstone.gemfire.distributed.internal.ReliableReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import com.gemstone.gemfire.internal.cache.tx.TXRegionStub;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * TXStateStub lives on the accessor node when we are remoting
 * a transaction. It is a stub for {@link TXState}. 
 * 
 *
 */
public abstract class TXStateStub implements TXStateInterface {
  protected final DistributedMember target;
  protected final TXStateProxy proxy;
  protected Runnable internalAfterSendRollback;
  protected Runnable internalAfterSendCommit;
  
  Map<Region<?,?>,TXRegionStub> regionStubs = new HashMap<Region<?,?>,TXRegionStub>();
  

  /**
   * @param stateProxy
   * @param target
   */
  protected TXStateStub(TXStateProxy stateProxy, DistributedMember target) {
    this.target = target;
    this.proxy = stateProxy;
    this.internalAfterSendRollback = null;
    this.internalAfterSendCommit = null;
  }
  
  @Override
  public void precommit() throws CommitConflictException,
      UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(
        LocalizedStrings.Dist_TX_PRECOMMIT_NOT_SUPPORTED_IN_A_TRANSACTION
            .toLocalizedString("precommit"));
  }
  
  /**
   * Implemented in subclasses for Peer vs. Client
   */
  public abstract void commit() throws CommitConflictException;
  protected abstract void validateRegionCanJoinTransaction(LocalRegion region) throws TransactionException;
  protected abstract TXRegionStub generateRegionStub(LocalRegion region);
  public abstract void rollback();
  public abstract void afterCompletion(int status);
  public void beforeCompletion() {
    // note that this class must do distribution as it is used as the stub class in some situations
    ReliableReplyProcessor21 response = JtaBeforeCompletionMessage.send(proxy.getCache(),
        proxy.getTxId().getUniqId(),getOriginatingMember(), target);
    try {
      try {
        response.waitForReliableDelivery();
      } catch (ReliableReplyException e) {
        throw new TransactionDataNodeHasDepartedException(e);
      } catch (ReplyException e) {
        e.handleAsUnexpected();
      } catch (InterruptedException e) {
      }
    } catch (SynchronizationCommitConflictException e) {
      throw e;
    } catch (CommitConflictException cce) {
      throw cce;
    } catch(TransactionException te) {
      throw te;
    }
  }

  
  
  
  /**
   * Get or create a TXRegionStub for the given region. 
   * For regions that are new to the tx, we validate their eligibility.
   * 
   * @param region The region to involve in the tx.
   * @return existing or new stub for region
   */
  protected final TXRegionStub getTXRegionStub(LocalRegion region) {
    TXRegionStub stub = regionStubs.get(region);
    if(stub==null) {
      /*
       * validate whether this region is legit or not
       */
      validateRegionCanJoinTransaction(region);
      stub = generateRegionStub(region);
      regionStubs.put(region,stub);
    }
    return stub;
  }
  

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(this.getClass())
                    .append("@")
                    .append(System.identityHashCode(this))
                    .append(" target node: ")
                    .append(target);
    return builder.toString();
  }


  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#destroyExistingEntry(com.gemstone.gemfire.internal.cache.EntryEventImpl, boolean, java.lang.Object)
   */
  public void destroyExistingEntry(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) throws EntryNotFoundException {
    if (event.getOperation().isLocal()) {
      throw new UnsupportedOperationInTransactionException(
          LocalizedStrings.TXStateStub_LOCAL_DESTROY_NOT_ALLOWED_IN_TRANSACTION.toLocalizedString());
    }
    TXRegionStub rs = getTXRegionStub(event.getRegion());
    rs.destroyExistingEntry(event,cacheWrite,expectedOldValue);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#getBeginTime()
   */
  public long getBeginTime() {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#getCache()
   */
  public Cache getCache() {
    return this.proxy.getTxMgr().getCache();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#getChanges()
   */
  public int getChanges() {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#getDeserializedValue(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion, boolean)
   */
  public Object getDeserializedValue(KeyInfo keyInfo, LocalRegion localRegion,
      boolean updateStats, boolean disableCopyOnRead, boolean preferCD, EntryEventImpl clientEvent, boolean returnTombstones, boolean allowReadFromHDFS,  boolean retainResult) {
    // We never have a local value if we are a stub...
    return null;
  }

  public Object getDeserializedValue(KeyInfo keyInfo, LocalRegion localRegion,
      boolean updateStats, boolean disableCopyOnRead, boolean preferCD, EntryEventImpl clientEvent, boolean returnTombstones) {
    // We never have a local value if we are a stub...
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#getEntry(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public Entry getEntry(KeyInfo keyInfo, LocalRegion r, boolean allowTombstones) {
    return getTXRegionStub(r).getEntry(keyInfo, allowTombstones);
//    Entry retVal = null;
//    if (r.getPartitionAttributes() != null) {
//      PartitionedRegion pr = (PartitionedRegion)r;
//      try {
//        retVal = pr.getEntryRemotely((InternalDistributedMember)target,
//                                keyInfo.getBucketId(), keyInfo.getKey(), allowTombstones);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#getEvent()
   */
  public TXEvent getEvent() {
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#getEvents()
   */
  public List getEvents() {
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#getRegions()
   */
  public Collection<LocalRegion> getRegions() {
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#getTransactionId()
   */
  public TransactionId getTransactionId() {
    return this.proxy.getTxId();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#invalidateExistingEntry(com.gemstone.gemfire.internal.cache.EntryEventImpl, boolean, boolean)
   */
  public void invalidateExistingEntry(EntryEventImpl event,
      boolean invokeCallbacks, boolean forceNewEntry) {
    if (event.getOperation().isLocal()) {
      throw new UnsupportedOperationInTransactionException(
          LocalizedStrings.TXStateStub_LOCAL_INVALIDATE_NOT_ALLOWED_IN_TRANSACTION.toLocalizedString());
    }
    getTXRegionStub(event.getRegion()).invalidateExistingEntry(event,invokeCallbacks,forceNewEntry);
 
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#isInProgress()
   */
  public boolean isInProgress() {
    return this.proxy.isInProgress();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#isInProgressAndSameAs(com.gemstone.gemfire.internal.cache.TXStateInterface)
   */
  public boolean isInProgressAndSameAs(TXStateInterface state) {
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#needsLargeModCount()
   */
  public boolean needsLargeModCount() {
    // TODO Auto-generated method stub
    return false;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#nextModSerialNum()
   */
  public int nextModSerialNum() {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#readRegion(com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public TXRegionState readRegion(LocalRegion r) {
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#rmRegion(com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public void rmRegion(LocalRegion r) {
    throw new UnsupportedOperationException();
  }




  public void setAfterSendRollback(Runnable afterSend) {
    // TODO Auto-generated method stub
    internalAfterSendRollback = afterSend;
  }
  
  public void setAfterSendCommit(Runnable afterSend) {
    // TODO Auto-generated method stub
    internalAfterSendCommit = afterSend;
  }
  

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#txPutEntry(com.gemstone.gemfire.internal.cache.EntryEventImpl, boolean, boolean, boolean)
   */
  public boolean txPutEntry(EntryEventImpl event, boolean ifNew,
      boolean requireOldValue, boolean checkResources, Object expectedOldValue) {
    return false;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#txReadEntry(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion, boolean)
   */
  public TXEntryState txReadEntry(KeyInfo entryKey, LocalRegion localRegion,
      boolean rememberRead,boolean createTxEntryIfAbsent) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#txReadRegion(com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public TXRegionState txReadRegion(LocalRegion localRegion) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#txWriteRegion(com.gemstone.gemfire.internal.cache.LocalRegion, java.lang.Object)
   */
  public TXRegionState txWriteRegion(LocalRegion localRegion, KeyInfo entryKey) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#writeRegion(com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public TXRegionState writeRegion(LocalRegion r) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#containsKey(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public boolean containsKey(KeyInfo keyInfo, LocalRegion localRegion) {
    return getTXRegionStub(localRegion).containsKey(keyInfo);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#containsValueForKey(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public boolean containsValueForKey(KeyInfo keyInfo, LocalRegion localRegion) {
    return getTXRegionStub(localRegion).containsValueForKey( keyInfo);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#entryCount(com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public int entryCount(LocalRegion localRegion) {
    return getTXRegionStub(localRegion).entryCount();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#findObject(com.gemstone.gemfire.internal.cache.LocalRegion, java.lang.Object, java.lang.Object, boolean, boolean, java.lang.Object)
   */
  public Object findObject(KeyInfo keyInfo, LocalRegion r, boolean isCreate,
      boolean generateCallbacks, Object value, boolean disableCopyOnRead, boolean preferCD, ClientProxyMembershipID requestingClient,
      EntryEventImpl clientEvent, boolean returnTombstones, boolean allowReadFromHDFS) {
    return getTXRegionStub(r).findObject(keyInfo,isCreate,generateCallbacks,value, preferCD, requestingClient, clientEvent, allowReadFromHDFS);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getAdditionalKeysForIterator(com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public Set getAdditionalKeysForIterator(LocalRegion currRgn) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getEntryForIterator(com.gemstone.gemfire.internal.cache.LocalRegion, java.lang.Object, boolean)
   */
  public Object getEntryForIterator(KeyInfo keyInfo, LocalRegion currRgn,
      boolean rememberReads, boolean allowTombstones) {
    return getTXRegionStub(currRgn).getEntryForIterator(keyInfo, allowTombstones);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getKeyForIterator(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion, boolean)
   */
  public Object getKeyForIterator(KeyInfo keyInfo, LocalRegion currRgn,
      boolean rememberReads, boolean allowTombstones) {
    return keyInfo.getKey();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getValueInVM(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion, boolean)
   */
  public Object getValueInVM(KeyInfo keyInfo, LocalRegion localRegion,
      boolean rememberRead) {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#isDeferredStats()
   */
  public boolean isDeferredStats() {
    return true;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#putEntry(com.gemstone.gemfire.internal.cache.EntryEventImpl, boolean, boolean, java.lang.Object, boolean, long, boolean)
   */
  public boolean putEntry(EntryEventImpl event, boolean ifNew, boolean ifOld,
      Object expectedOldValue, boolean requireOldValue, long lastModified,
      boolean overwriteDestroyed) {
   return getTXRegionStub(event.getRegion()).putEntry(event,ifNew,ifOld,expectedOldValue,requireOldValue,lastModified,overwriteDestroyed);
  }

  /*
   * (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getSerializedValue(com.gemstone.gemfire.internal.cache.LocalRegion, java.lang.Object, java.lang.Object)
   */
  public Object getSerializedValue(LocalRegion localRegion, KeyInfo key, boolean doNotLockEntry, ClientProxyMembershipID requestingClient, EntryEventImpl clientEvent, boolean returnTombstones, boolean allowReadFromHDFS) {
    throw new UnsupportedOperationException();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#putEntryOnRemote(com.gemstone.gemfire.internal.cache.EntryEventImpl, boolean, boolean, java.lang.Object, boolean, long, boolean)
   */
  public boolean putEntryOnRemote(EntryEventImpl event, boolean ifNew,
      boolean ifOld, Object expectedOldValue, boolean requireOldValue,
      long lastModified, boolean overwriteDestroyed)
      throws DataLocationException {
    throw new IllegalStateException();
  }
  
  public boolean isFireCallbacks() {
    return false;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#destroyOnRemote(java.lang.Integer, com.gemstone.gemfire.internal.cache.EntryEventImpl, java.lang.Object)
   */
  public void destroyOnRemote(EntryEventImpl event, boolean cacheWrite,
      Object expectedOldValue) throws DataLocationException {
    throw new IllegalStateException();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#invalidateOnRemote(com.gemstone.gemfire.internal.cache.EntryEventImpl, boolean, boolean)
   */
  public void invalidateOnRemote(EntryEventImpl event, boolean invokeCallbacks,
      boolean forceNewEntry) throws DataLocationException {
    throw new IllegalStateException();
  }

  public void checkSupportsRegionDestroy()
    throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(LocalizedStrings.TXState_REGION_DESTROY_NOT_SUPPORTED_IN_A_TRANSACTION.toLocalizedString());
  }
  
  public void checkSupportsRegionInvalidate()
    throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(LocalizedStrings.TXState_REGION_INVALIDATE_NOT_SUPPORTED_IN_A_TRANSACTION.toLocalizedString());
  }

  @Override
  public void checkSupportsRegionClear()
      throws UnsupportedOperationInTransactionException {
    throw new UnsupportedOperationInTransactionException(LocalizedStrings.TXState_REGION_CLEAR_NOT_SUPPORTED_IN_A_TRANSACTION.toLocalizedString());
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getBucketKeys(com.gemstone.gemfire.internal.cache.LocalRegion, int)
   */
  public Set getBucketKeys(LocalRegion localRegion, int bucketId, boolean allowTombstones) {
    PartitionedRegion pr = (PartitionedRegion)localRegion;
    /*
     * txtodo: what does this mean for c/s
     */
    return pr.getBucketKeys(bucketId, allowTombstones);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getEntryOnRemote(java.lang.Object, com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public Entry getEntryOnRemote(KeyInfo key, LocalRegion localRegion, boolean allowTombstones)
      throws DataLocationException {
    throw new IllegalStateException();
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.TXStateInterface#getSemaphore()
   */
  public ReentrantLock getLock() {
    return proxy.getLock();
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.InternalDataView#getRegionKeysForIteration(com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  public Set getRegionKeysForIteration(LocalRegion currRegion) {
    return getTXRegionStub(currRegion).getRegionKeysForIteration(currRegion);
  }


  public boolean isRealDealLocal()
  {    
    return false;
  }

  public DistributedMember getTarget() {
    return target;
  }
  
  public void postPutAll(DistributedPutAllOperation putallOp, VersionedObjectList successfulPuts,LocalRegion region) {
	  getTXRegionStub(region).postPutAll(putallOp,successfulPuts,region);
  }
  @Override
  public void postRemoveAll(DistributedRemoveAllOperation op, VersionedObjectList successfulOps, LocalRegion region) {
    getTXRegionStub(region).postRemoveAll(op, successfulOps, region);
  }

  
  public Entry accessEntry(KeyInfo keyInfo, LocalRegion localRegion) {
    return getEntry(keyInfo, localRegion, false);
  }

  @Override
  public void updateEntryVersion(EntryEventImpl event)
      throws EntryNotFoundException {
    throw new UnsupportedOperationException();
  }
  
  @Override
  public void close() {
    // nothing needed
  }
  
  @Override
  public boolean isTxState() {
    return false;
  }
  
  @Override
  public boolean isTxStateStub() {
    return true;
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
}
