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
package com.gemstone.gemfire.cache.query.internal.cq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.client.internal.UserAttributes;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesMutator;
import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqResults;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.internal.CompiledBindArgument;
import com.gemstone.gemfire.cache.query.internal.CompiledIteratorDef;
import com.gemstone.gemfire.cache.query.internal.CompiledRegion;
import com.gemstone.gemfire.cache.query.internal.CompiledSelect;
import com.gemstone.gemfire.cache.query.internal.CqStateImpl;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;
import com.gemstone.gemfire.i18n.StringId;

public class ServerCQImpl extends CqQueryImpl implements DataSerializable, ServerCQ {
  private static final Logger logger = LogService.getLogger();

  /** 
   * This holds the keys that are part of the CQ query results.
   * Using this CQ engine can determine whether to execute 
   * query on old value from EntryEvent, which is an expensive
   * operation. 
   */
  private volatile HashMap<Object, Object> cqResultKeys;

  /** 
   * This maintains the keys that are destroyed while the Results
   * Cache is getting constructed. This avoids any keys that are
   * destroyed (after query execution) but is still part of the 
   * CQs result.
   */
  private HashSet<Object> destroysWhileCqResultsInProgress;
  
  /**
   * To indicate if the CQ results key cache is initialized.
   */
  public volatile boolean cqResultKeysInitialized = false;
  
  /** Boolean flag to see if the CQ is on Partitioned Region */
  public volatile boolean isPR = false;
  
  private ClientProxyMembershipID clientProxyId = null;
  
  private CacheClientNotifier ccn = null;
  
  private String serverCqName;
  
  
  /** identifier assigned to this query for FilterRoutingInfos */
  private Long filterID;
  
  public ServerCQImpl(CqServiceImpl cqService, String cqName, String queryString, boolean isDurable, String serverCqName)  {
    super(cqService, cqName, queryString, isDurable);
    this.serverCqName = serverCqName; // On Client Side serverCqName and cqName will be same.
  }
  
  public ServerCQImpl() {
    //For deserialization
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.query.internal.InternalCqQuery2#getFilterID()
   */
  @Override
  public Long getFilterID() {
    return this.filterID;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.query.internal.InternalCqQuery2#setFilterID(java.lang.Long)
   */
  @Override
  public void setFilterID(Long filterID) {
    this.filterID = filterID;
  }
  
  @Override
  public void setName(String cqName) {
    this.cqName = this.serverCqName = cqName;
  }
  
  @Override
  public String getServerCqName() {
    return this.serverCqName;
  }
  
  @Override
  public void registerCq(ClientProxyMembershipID p_clientProxyId, 
      CacheClientNotifier p_ccn, int p_cqState) 
      throws CqException, RegionNotFoundException {      
    
    CacheClientProxy clientProxy = null;    
    this.clientProxyId = p_clientProxyId; 
    //servConnection = serverSideConnection;

    if (p_ccn != null) {
      this.ccn = p_ccn;
      clientProxy = p_ccn.getClientProxy(p_clientProxyId, true);
    }
    
    /*
    try {
      initCq();
    } catch (CqExistsException cqe) {
      // Should not happen.
      throw new CqException(LocalizedStrings.CqQueryImpl_UNABLE_TO_CREATE_CQ_0_ERROR__1.toLocalizedString(new Object[] { cqName, cqe.getMessage()}));
    }
    */
    
    validateCq();
    
    final boolean isDebugEnabled = logger.isDebugEnabled();
    StringId msg = LocalizedStrings.ONE_ARG;
    Throwable t = null;
    try {
      this.query = constructServerSideQuery();
      if (isDebugEnabled) {
        logger.debug("Server side query for the cq: {} is: {}", cqName, this.query.getQueryString());
      }
    } catch (Exception ex) {
      t = ex;
      if (ex instanceof ClassNotFoundException) {
        msg = LocalizedStrings.CqQueryImpl_CLASS_NOT_FOUND_EXCEPTION_THE_ANTLRJAR_OR_THE_SPCIFIED_CLASS_MAY_BE_MISSING_FROM_SERVER_SIDE_CLASSPATH_ERROR_0;
      } else {
        msg = LocalizedStrings.CqQueryImpl_ERROR_WHILE_PARSING_THE_QUERY_ERROR_0;
      }
    } finally {
      if (t != null) {
        String s = msg.toLocalizedString(t);
        if (isDebugEnabled) {
          logger.debug(s, t);
        }
        throw new CqException(s);
      }
    }
    
    // Update Regions Book keeping.
    // TODO replace getRegion() with getRegionByPathForProcessing() so this doesn't block
    // if the region is still being initialized
    this.cqBaseRegion = (LocalRegion)cqService.getCache().getRegion(regionName); 
    if (this.cqBaseRegion == null) {
      throw new RegionNotFoundException(LocalizedStrings.CqQueryImpl_REGION__0_SPECIFIED_WITH_CQ_NOT_FOUND_CQNAME_1
          .toLocalizedString(new Object[] {regionName, this.cqName}));
    }
    
    // Make sure that the region is partitioned or 
    // replicated with distributed ack or global.
    DataPolicy dp = this.cqBaseRegion.getDataPolicy();
    this.isPR = dp.withPartitioning();
    if (!(this.isPR || dp.withReplication())) {
      String errMsg = null;
      //replicated regions with eviction set to local destroy get turned into preloaded
      if (dp.withPreloaded() && cqBaseRegion.getAttributes().getEvictionAttributes() != null && cqBaseRegion.getAttributes().getEvictionAttributes().getAction().equals(EvictionAction.LOCAL_DESTROY)) {
        errMsg = LocalizedStrings.CqQueryImpl_CQ_NOT_SUPPORTED_FOR_REPLICATE_WITH_LOCAL_DESTROY.toString(this.regionName, cqBaseRegion.getAttributes().getEvictionAttributes().getAction());
      }
      else {
        errMsg = "The region " + this.regionName + 
            "  specified in CQ creation is neither replicated nor partitioned; " +
        "only replicated or partitioned regions are allowed in CQ creation.";
      }
      if (isDebugEnabled){
        logger.debug(errMsg);
      }
      throw new CqException(errMsg);
    }
    if ((dp.withReplication() && 
        (!(cqBaseRegion.getAttributes().getScope().isDistributedAck() || 
        cqBaseRegion.getAttributes().getScope().isGlobal())))) {
      String errMsg = "The replicated region " + this.regionName + 
          " specified in CQ creation does not have scope supported by CQ." +
          " The CQ supported scopes are DISTRIBUTED_ACK and GLOBAL.";
      if (isDebugEnabled){
        logger.debug(errMsg);
      }
      throw new CqException(errMsg);
    }
    
    //checkAndSetCqOnRegion();
    
    //Can be null by the time we are here
    if (clientProxy != null) {
      clientProxy.incCqCount();
      if (clientProxy.hasOneCq()) {
        cqService.stats.incClientsWithCqs();
      }
      if (isDebugEnabled) {
        logger.debug("Added CQ to the base region: {} With key as: {}", cqBaseRegion.getFullPath(), serverCqName);
      }
    }
    
    // this.cqService.addToCqEventKeysMap(this);
    this.updateCqCreateStats();
    
    // Initialize the state of CQ.
    if(this.cqState.getState() != p_cqState) {
      setCqState(p_cqState);
    }
    
    //Register is called from both filter profile and cqService
    //In either case, if we are trying to start/run the cq, we need to add
    //it to other matching cqs for performance reasons
    if (p_cqState == CqStateImpl.RUNNING) {
         // Add to the matchedCqMap.
        cqService.addToMatchingCqMap(this);    
    }

    // Initialize CQ results (key) cache.
    if(CqServiceProvider.MAINTAIN_KEYS) {
      this.cqResultKeys = new HashMap <Object, Object>();
      // Currently the CQ Result keys are not cached for the Partitioned 
      // Regions. Supporting this with PR needs more work like forcing 
      // query execution on primary buckets only; and handling the bucket
      // re-balancing. Once this is added remove the check with PR region.
      // Only the events which are seen during event processing is 
      // added to the results cache (not from the CQ Results).
      if (this.isPR){
        this.setCqResultsCacheInitialized();
      } else {
        this.destroysWhileCqResultsInProgress = new HashSet <Object>();
      }
    } 

    if (p_ccn != null) {
      try {
        cqService.addToCqMap(this);
      } catch (CqExistsException cqe) {
        // Should not happen.
        throw new CqException(LocalizedStrings.CqQueryImpl_UNABLE_TO_CREATE_CQ_0_ERROR__1.toLocalizedString(new Object[] { cqName, cqe.getMessage()}));
      }
      this.cqBaseRegion.getFilterProfile().registerCq(this);
    }
  }
  
  /**
   * For Test use only.
   * @return CQ Results Cache.
   */
  public Set<Object> getCqResultKeyCache() {
    if (this.cqResultKeys != null){
      synchronized (this.cqResultKeys) {
        return Collections.synchronizedSet(new HashSet<Object>(this.cqResultKeys.keySet()));
      }
    } else {
      return null;
    }
  }
  
  /**
   * Returns parameterized query used by the server.
   * This method replaces Region name with $1 and if type is not specified
   * in the query, looks for type from cqattributes and appends into the
   * query.
   * @return String modified query.
   * @throws CqException
   */
  private Query constructServerSideQuery() throws QueryException {
    GemFireCacheImpl cache = (GemFireCacheImpl)cqService.getCache();
    DefaultQuery locQuery = (DefaultQuery)cache.getLocalQueryService().newQuery(this.queryString);      
    CompiledSelect select = locQuery.getSimpleSelect();
    CompiledIteratorDef from = (CompiledIteratorDef)select.getIterators().get(0);
    // WARNING: ASSUMES QUERY WAS ALREADY VALIDATED FOR PROPER "FORM" ON CLIENT;
    // THIS VALIDATION WILL NEED TO BE DONE ON THE SERVER FOR NATIVE CLIENTS,
    // BUT IS NOT DONE HERE FOR JAVA CLIENTS.
    // The query was already checked on the client that the sole iterator is a
    // CompiledRegion
    this.regionName = ((CompiledRegion)from.getCollectionExpr()).getRegionPath();
    from.setCollectionExpr(new CompiledBindArgument(1));
    return locQuery;    
  }
  
  /**
   * Returns if the passed key is part of the CQs result set.
   * This method needs to be called once the CQ result key caching
   * is completed (cqResultsCacheInitialized is true).
   * @param key
   * @return true if key is in the Results Cache.
   */
  public boolean isPartOfCqResult(Object key) {
    // Handle events that may have been deleted,
    // but added by result caching.
    if (this.cqResultKeys == null) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.CqQueryImpl_Null_CQ_Result_Key_Cache_0));
      return false;
    }

    synchronized (this.cqResultKeys) {
      if (this.destroysWhileCqResultsInProgress != null) {
        //this.logger.fine("Removing keys from Destroy Cache  For CQ :" + 
        //this.cqName + " Keys :" + this.destroysWhileCqResultsInProgress);
        for (Object k : this.destroysWhileCqResultsInProgress){
          this.cqResultKeys.remove(k);  
        }
        this.destroysWhileCqResultsInProgress = null;
      }
      return this.cqResultKeys.containsKey(key);
    }
  }
    
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.query.internal.InternalCqQuery2#addToCqResultKeys(java.lang.Object)
   */
  @Override
  public void addToCqResultKeys(Object key) {
    if (!CqServiceProvider.MAINTAIN_KEYS){
      return;
    }
    
    //this.logger.fine("Adding key to Results Cache For CQ :" + 
    //this.cqName + " key :" + key);
    if (this.cqResultKeys != null) {
      synchronized (this.cqResultKeys) { 
        this.cqResultKeys.put(key, TOKEN);
        if (!this.cqResultKeysInitialized){
          // This key could be coming after add, destroy.
          // Remove this from destroy queue.
          //this.logger.fine("Removing key from Destroy Cache For CQ :" + 
          //this.cqName + " key :" + key);
          if (this.destroysWhileCqResultsInProgress != null){
            this.destroysWhileCqResultsInProgress.remove(key);
          }
        }
      }
    }
  }

  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.query.internal.InternalCqQuery2#removeFromCqResultKeys(java.lang.Object, boolean)
   */
  @Override
  public void removeFromCqResultKeys(Object key, boolean isTokenMode) {
    if (!CqServiceProvider.MAINTAIN_KEYS){
      return;
    }
    //this.logger.fine("Removing key from Results Cache For CQ :" + 
    //this.cqName + " key :" + key);
    if (this.cqResultKeys != null) {
      synchronized (this.cqResultKeys) { 
        if (isTokenMode && this.cqResultKeys.get(key) != Token.DESTROYED){
          return;
        }
        this.cqResultKeys.remove(key);
        if (!this.cqResultKeysInitialized){
          //this.logger.fine("Adding key to Destroy Cache For CQ :" + 
          //this.cqName + " key :" + key);
          if (this.destroysWhileCqResultsInProgress != null){
            this.destroysWhileCqResultsInProgress.add(key);
          }
        }
      }
    }
  }
  
  /**
   * Marks the key as destroyed in the CQ Results key cache.
   * @param key
   */
  public void markAsDestroyedInCqResultKeys(Object key){
    if (!CqServiceProvider.MAINTAIN_KEYS){
      return;
    }
    //this.logger.fine("Marking key in Results Cache For CQ :" + 
    //    this.cqName + " key :" + key);

    if (this.cqResultKeys != null) {
      synchronized (this.cqResultKeys) { 
        this.cqResultKeys.put(key, Token.DESTROYED);
        if (!this.cqResultKeysInitialized){
          //this.logger.fine("Adding key to Destroy Cache For CQ :" + 
          //this.cqName + " key :" + key);
          if (this.destroysWhileCqResultsInProgress != null){
            this.destroysWhileCqResultsInProgress.add(key);
          }
        }
      }
    }    
  }
  
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.query.internal.InternalCqQuery2#setCqResultsCacheInitialized()
   */
  @Override
  public void setCqResultsCacheInitialized() {
    if (CqServiceProvider.MAINTAIN_KEYS) {
      this.cqResultKeysInitialized = true;
    }
  }
   
  /**
   * Returns the size of the CQ Result key cache.
   * @return size of CQ Result key cache.
   */
  public int getCqResultKeysSize() {
    if (this.cqResultKeys == null) {
      return 0;
    }
    synchronized (this.cqResultKeys) { 
      return this.cqResultKeys.size();
    }
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.query.internal.InternalCqQuery2#isOldValueRequiredForQueryProcessing(java.lang.Object)
   */
  @Override
  public boolean isOldValueRequiredForQueryProcessing(Object key){
    if (this.cqResultKeysInitialized && this.isPartOfCqResult(key)) {
      return false;
    }
    return true;
  }
  
  /**
   * Closes the Query.
   *        On Client side, sends the cq close request to server.
   *        On Server side, takes care of repository cleanup.
   * @throws CqException
   */
  public void close() throws CqClosedException, CqException {
    close(true);
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.query.internal.InternalCqQuery2#close(boolean)
   */
  @Override
  public void close(boolean sendRequestToServer) throws CqClosedException, CqException {
    final boolean isDebugEnabled = logger.isDebugEnabled();
    if (isDebugEnabled) {
      logger.debug("Started closing CQ CqName: {} SendRequestToServer: {}", cqName, sendRequestToServer);
    }   
    // Synchronize with stop and execute CQ commands
    synchronized(this.cqState) {
      // Check if the cq is already closed.
      if (this.isClosed()) {
        //throw new CqClosedException("CQ is already closed, CqName : " + this.cqName);
        if (isDebugEnabled){
          logger.debug("CQ is already closed, CqName: {}", this.cqName);
        }
        return;
      }

      int stateBeforeClosing = this.cqState.getState();
      this.cqState.setState(CqStateImpl.CLOSING);      
      boolean isClosed = false;

      // Cleanup the resource used by cq.
      this.removeFromCqMap(); 

      // Stat update.
      if (stateBeforeClosing == CqStateImpl.RUNNING) {
        cqService.stats.decCqsActive();
      } else if (stateBeforeClosing == CqStateImpl.STOPPED) {
        cqService.stats.decCqsStopped();
      }

      // Clean-up the CQ Results Cache.
      if (this.cqResultKeys != null) {
        synchronized (this.cqResultKeys){
          this.cqResultKeys.clear();
        }
      }

      // Set the state to close, and update stats
      this.cqState.setState(CqStateImpl.CLOSED);
      cqService.stats.incCqsClosed();
      cqService.stats.decCqsOnClient();
      if(this.stats != null)
        this.stats.close();
    }

    if (isDebugEnabled) {
      logger.debug("Successfully closed the CQ. {}", cqName);
    }
  }
  
  @Override
  public ClientProxyMembershipID getClientProxyId() {
    return this.clientProxyId;
  }
  
  /**
   * Get CacheClientNotifier of this CqQuery.
   * @return CacheClientNotifier 
   */
  public CacheClientNotifier getCacheClientNotifier() {
    return this.ccn;
  }
  
  /**
   * Clears the resource used by CQ.
   * @throws CqException
   */
  protected void cleanup() throws CqException {
    // CqBaseRegion 
    try {
      if (this.cqBaseRegion != null && !this.cqBaseRegion.isDestroyed()) {
        this.cqBaseRegion.getFilterProfile().closeCq(this);
        CacheClientProxy clientProxy = ccn.getClientProxy(clientProxyId);
        clientProxy.decCqCount();
        if (clientProxy.hasNoCq()) {
          cqService.stats.decClientsWithCqs();
        }
      } 
    }catch (Exception ex){
      // May be cache is being shutdown
      if (logger.isDebugEnabled()) {
        logger.debug("Failed to remove CQ from the base region. CqName :{}", cqName);
      }
    }
  }
  
  /**
   * @param serverCqName The serverCqName to set.
   */
  public void setServerCqName(String serverCqName) {
    
    this.serverCqName = serverCqName;
  }
  
  /**
   * Stop or pause executing the query.
   */
  public void stop()throws CqClosedException, CqException {
    boolean isStopped = false;
    synchronized (this.cqState) {
      if (this.isClosed()) {
        throw new CqClosedException(LocalizedStrings.CqQueryImpl_CQ_IS_CLOSED_CQNAME_0
            .toLocalizedString(this.cqName));
      }
      
      if (!(this.isRunning())) {
        throw new IllegalStateException(LocalizedStrings.CqQueryImpl_CQ_IS_NOT_IN_RUNNING_STATE_STOP_CQ_DOES_NOT_APPLY_CQNAME_0
            .toLocalizedString(this.cqName));
      }
      
      // Change state and stats on the client side
      this.cqState.setState(CqStateImpl.STOPPED);
      this.cqService.stats.incCqsStopped();
      this.cqService.stats.decCqsActive();
      if (logger.isDebugEnabled()) {
        logger.debug("Successfully stopped the CQ. {}", cqName);
      }
    }
  }

  /* DataSerializableFixedID methods ---------------------------------------- */

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    //this.cqName = DataSerializer.readString(in);
    synchronized(cqState) {
      this.cqState.setState(DataSerializer.readInteger(in));
    }
    this.isDurable = DataSerializer.readBoolean(in);
    this.queryString = DataSerializer.readString(in);
    this.filterID = in.readLong();
  }
  
  /*
  public int getDSFID() {
    return CQ_QUERY;
  }
  */
  
  public void toData(DataOutput out) throws IOException {
    //DataSerializer.writeString(this.cqName, out);
    DataSerializer.writeInteger(this.cqState.getState(), out);
    DataSerializer.writeBoolean(this.isDurable, out);
    DataSerializer.writeString(this.queryString, out);
    out.writeLong(this.filterID);
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.query.internal.InternalCqQuery2#isPR()
   */
  @Override
  public boolean isPR() {
    return isPR;
  }

  @Override
  public CqAttributes getCqAttributes() {
    throw new IllegalStateException("CQ attributes are not available on the server");
  }

  @Override
  public CqAttributesMutator getCqAttributesMutator() {
    throw new IllegalStateException("CQ attributes are not available on the server");
  }

  @Override
  public <E> CqResults<E> executeWithInitialResults() throws CqClosedException,
      RegionNotFoundException, CqException {
    throw new IllegalStateException("Execute cannot be called on a CQ on the server");
  }

  @Override
  public void execute() throws CqClosedException, RegionNotFoundException,
      CqException {
    throw new IllegalStateException("Execute cannot be called on a CQ on the server");
  }
  
  
}
