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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.CqState;
import com.gemstone.gemfire.cache.query.CqStatistics;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.internal.CompiledIteratorDef;
import com.gemstone.gemfire.cache.query.internal.CompiledRegion;
import com.gemstone.gemfire.cache.query.internal.CompiledSelect;
import com.gemstone.gemfire.cache.query.internal.CompiledValue;
import com.gemstone.gemfire.cache.query.internal.CqQueryVsdStats;
import com.gemstone.gemfire.cache.query.internal.CqStateImpl;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.query.internal.ExecutionContext;
import com.gemstone.gemfire.cache.query.internal.QueryExecutionContext;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.i18n.StringId;

/**
 * @author rmadduri
 * @author anil
 * @since 5.5
 * Represents the CqQuery object. Implements CqQuery API and CqAttributeMutator. 
 *  
 */
public abstract class CqQueryImpl implements InternalCqQuery {
  private static final Logger logger = LogService.getLogger();
  
  protected String cqName;
  
  protected String queryString;
  
  protected static final Object TOKEN = new Object();

  protected LocalRegion cqBaseRegion;
  
  protected Query query = null;
  
  protected InternalLogWriter securityLogWriter;
  
  protected CqServiceImpl cqService;
  
  protected String regionName;
  
  protected boolean isDurable = false ;
  
  // Stats counters
  protected CqStatisticsImpl cqStats;
  
  protected CqQueryVsdStats stats;
    
  protected final CqStateImpl cqState = new CqStateImpl();
  
  protected ExecutionContext queryExecutionContext = null;

  public static TestHook testHook = null;
  
  /**
   * Constructor.
   */
  public CqQueryImpl(){
  }
  
  public CqQueryImpl(CqServiceImpl cqService, String cqName, String queryString, boolean isDurable)  {
    this.cqName = cqName;
    this.queryString = queryString;
    this.securityLogWriter = (InternalLogWriter) cqService.getCache().getSecurityLoggerI18n();
    this.cqService = cqService;
    this.isDurable = isDurable ;
  }
  
  /** 
   * returns CQ name
   */
  public String getName() {
    return this.cqName;
  }
  
  @Override
  public void setName(String cqName) {
    this.cqName = cqName;
  }

  public void setCqService(CqService cqService) {
    this.cqService = (CqServiceImpl) cqService;
  }
  
  /**
   * get the region name in CQ query
   */
  @Override
  public String getRegionName() {
    return this.regionName;
  }

  public void updateCqCreateStats() {
    // Initialize the VSD statistics
    StatisticsFactory factory = cqService.getCache().getDistributedSystem();
    this.stats = new CqQueryVsdStats(factory, getServerCqName());
    this.cqStats = new CqStatisticsImpl(this);

    // Update statistics with CQ creation.
    this.cqService.stats.incCqsStopped();
    this.cqService.stats.incCqsCreated();
    this.cqService.stats.incCqsOnClient();
  }

  /**
   * Validates the CQ. Checks for cq constraints. 
   * Also sets the base region name.
   */
  public void validateCq() {
    Cache cache = cqService.getCache();
    DefaultQuery locQuery = (DefaultQuery)((GemFireCacheImpl)cache).getLocalQueryService().newQuery(this.queryString);
    this.query = locQuery;
//    assert locQuery != null;
    
    // validate Query.
    Object[] parameters = new Object[0]; // parameters are not permitted
    
    // check that it is only a SELECT statement (possibly with IMPORTs)
    CompiledSelect select = locQuery.getSimpleSelect();
    if (select == null) {
      throw new UnsupportedOperationException(LocalizedStrings.CqQueryImpl_CQ_QUERIES_MUST_BE_A_SELECT_STATEMENT_ONLY.toLocalizedString());
    }    
    
    // must not be a DISTINCT select
    if (select.isDistinct()) {
      throw new UnsupportedOperationException(LocalizedStrings.CqQueryImpl_SELECT_DISTINCT_QUERIES_NOT_SUPPORTED_IN_CQ.toLocalizedString());
    }
    
    // get the regions referenced in this query
    Set regionsInQuery = locQuery.getRegionsInQuery(parameters);
    // check for more than one region is referenced in the query
    // (though it could still be one region referenced multiple times)
    if (regionsInQuery.size() > 1 || regionsInQuery.size() < 1) {
      throw new UnsupportedOperationException(LocalizedStrings.CqQueryImpl_CQ_QUERIES_MUST_REFERENCE_ONE_AND_ONLY_ONE_REGION.toLocalizedString());
    }
    this.regionName = (String)regionsInQuery.iterator().next();
    
    // make sure the where clause references no regions
    Set regions = new HashSet();
    CompiledValue whereClause = select.getWhereClause();
    if (whereClause != null) {
      whereClause.getRegionsInQuery(regions, parameters);
      if (!regions.isEmpty()) {
        throw new UnsupportedOperationException(LocalizedStrings.CqQueryImpl_THE_WHERE_CLAUSE_IN_CQ_QUERIES_CANNOT_REFER_TO_A_REGION.toLocalizedString());
      }
    }
    List fromClause = select.getIterators();
    // cannot have more than one iterator in FROM clause
    if (fromClause.size() > 1) {
      throw new UnsupportedOperationException(LocalizedStrings.CqQueryImpl_CQ_QUERIES_CANNOT_HAVE_MORE_THAN_ONE_ITERATOR_IN_THE_FROM_CLAUSE.toLocalizedString());
    }
    
    // the first iterator in the FROM clause must be just a CompiledRegion
    CompiledIteratorDef itrDef = (CompiledIteratorDef)fromClause.get(0);
    // By process of elimination, we know that the first iterator contains a reference
    // to the region. Check to make sure it is only a CompiledRegion
    if (!(itrDef.getCollectionExpr() instanceof CompiledRegion)) {
      throw new UnsupportedOperationException(LocalizedStrings.CqQueryImpl_CQ_QUERIES_MUST_HAVE_A_REGION_PATH_ONLY_AS_THE_FIRST_ITERATOR_IN_THE_FROM_CLAUSE.toLocalizedString());
    }
    
    // must not have any projections
    List projs = select.getProjectionAttributes();
    if (projs != null) {
      throw new UnsupportedOperationException(LocalizedStrings.CqQueryImpl_CQ_QUERIES_DO_NOT_SUPPORT_PROJECTIONS.toLocalizedString());
    }
    
    // check the orderByAttrs, not supported
    List orderBys = select.getOrderByAttrs();
    if (orderBys != null) {
      throw new UnsupportedOperationException(LocalizedStrings.CqQueryImpl_CQ_QUERIES_DO_NOT_SUPPORT_ORDER_BY.toLocalizedString());
    }   
    
    // Set Query ExecutionContext, that will be used in later execution.
    this.setQueryExecutionContext(new QueryExecutionContext(null, this.cqService.getCache()));
  }
  
  /**
   * Removes the CQ from CQ repository.
   * @throws CqException
   */
  protected void removeFromCqMap() throws CqException {
    try {
      cqService.removeCq(this.getServerCqName());
    } catch (Exception ex){
      StringId errMsg = LocalizedStrings.CqQueryImpl_FAILED_TO_REMOVE_CONTINUOUS_QUERY_FROM_THE_REPOSITORY_CQNAME_0_ERROR_1;
      Object[] errMsgArgs = new Object[] {cqName, ex.getLocalizedMessage()};
      String msg = errMsg.toLocalizedString(errMsgArgs);
      logger.error(msg);
      throw new CqException(msg, ex);
    }    
    if (logger.isDebugEnabled()){
      logger.debug("Removed CQ from the CQ repository. CQ Name: {}", this.cqName);
    }
  }

  /** 
   * Returns the QueryString of this CQ.
   */
  public String getQueryString() {
    return queryString;
  }
  
  /**
   * Return the query after replacing region names with parameters
   * @return the Query for the query string
   */
  public Query getQuery(){
    return query;
  }
  
  
  /**
   * @see com.gemstone.gemfire.cache.query.CqQuery#getStatistics()
   */
  public CqStatistics getStatistics() {
    return cqStats;
  }
  
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.query.internal.InternalCqQuery2#getCqBaseRegion()
   */
  @Override
  public LocalRegion getCqBaseRegion() {
    return this.cqBaseRegion;
  }
  
  protected abstract void cleanup() throws CqException;
  
  /**
   * @return Returns the Region name on which this cq is created.
   */
  public String getBaseRegionName() {
    
    return this.regionName;
  }

  public abstract String getServerCqName();

  /**
   * Return the state of this query.
   * Should not modify this state without first locking it.
   * @return STOPPED RUNNING or CLOSED
   */
  public CqState getState() {
    return this.cqState;
  }

 /* (non-Javadoc)
 * @see com.gemstone.gemfire.cache.query.internal.InternalCqQuery2#setCqState(int)
 */
  @Override
  public void setCqState(int state) {
    if (this.isClosed()) {
      throw new CqClosedException(LocalizedStrings.CqQueryImpl_CQ_IS_CLOSED_CQNAME_0
          .toLocalizedString(this.cqName));
    }

    synchronized (cqState) {
      if (state == CqStateImpl.RUNNING){
        if (this.isRunning()) {
          //throw new IllegalStateException(LocalizedStrings.CqQueryImpl_CQ_IS_NOT_IN_RUNNING_STATE_STOP_CQ_DOES_NOT_APPLY_CQNAME_0
          //  .toLocalizedString(this.cqName));
        }
        this.cqState.setState(CqStateImpl.RUNNING);
        this.cqService.stats.incCqsActive();
        this.cqService.stats.decCqsStopped();
      } else if(state == CqStateImpl.STOPPED) {
        this.cqState.setState(CqStateImpl.STOPPED);
        this.cqService.stats.incCqsStopped();
        this.cqService.stats.decCqsActive();
      } else if(state == CqStateImpl.CLOSING) {
        this.cqState.setState(state);
      }
    }
  }

  /**
   * Update CQ stats
   * @param cqEvent object
   */
  public void updateStats(CqEvent cqEvent) {
    this.stats.updateStats(cqEvent);  // Stats for VSD
  }
  
  /**
   * Return true if the CQ is in running state
   * @return true if running, false otherwise
   */
  public boolean isRunning() {
    return this.cqState.isRunning();
  }
  
  /**
   * Return true if the CQ is in Sstopped state
   * @return true if stopped, false otherwise
   */ 
  public boolean isStopped() {
    return this.cqState.isStopped();
  }
  
  /**
   * Return true if the CQ is closed
   * @return true if closed, false otherwise
   */
  public boolean isClosed() {
    return this.cqState.isClosed();
  }

  /**
   * Return true if the CQ is in closing state.
   * @return true if close in progress, false otherwise
   */
  public boolean isClosing() {
    return this.cqState.isClosing();
  }
  
  /**
   * Return true if the CQ is durable
   * @return true if durable, false otherwise
   */
  public boolean isDurable() {
    return this.isDurable;
  }

  /**
   * Returns a reference to VSD stats of the CQ
   * @return VSD stats of the CQ
   */
  @Override
  public CqQueryVsdStats getVsdStats() {
    return stats;
  }

  public ExecutionContext getQueryExecutionContext() {
    return queryExecutionContext;
  }

  public void setQueryExecutionContext(ExecutionContext queryExecutionContext) {
    this.queryExecutionContext = queryExecutionContext;
  }

  /** Test Hook */
  public interface TestHook {
    public void pauseUntilReady();
    public void ready();
    public int numQueuedEvents();
    public void setEventCount(int count);
  }
}
