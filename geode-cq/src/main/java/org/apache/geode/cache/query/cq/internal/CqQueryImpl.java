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
package org.apache.geode.cache.query.cq.internal;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.query.CqClosedException;
import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqException;
import org.apache.geode.cache.query.CqState;
import org.apache.geode.cache.query.CqStatistics;
import org.apache.geode.cache.query.Query;
import org.apache.geode.cache.query.internal.CompiledIteratorDef;
import org.apache.geode.cache.query.internal.CompiledRegion;
import org.apache.geode.cache.query.internal.CompiledSelect;
import org.apache.geode.cache.query.internal.CompiledValue;
import org.apache.geode.cache.query.internal.CqQueryVsdStats;
import org.apache.geode.cache.query.internal.CqStateImpl;
import org.apache.geode.cache.query.internal.DefaultQuery;
import org.apache.geode.cache.query.internal.ExecutionContext;
import org.apache.geode.cache.query.internal.QueryExecutionContext;
import org.apache.geode.cache.query.internal.cq.CqService;
import org.apache.geode.cache.query.internal.cq.InternalCqQuery;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.logging.InternalLogWriter;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * Represents the CqQuery object. Implements CqQuery API and CqAttributeMutator.
 *
 * @since GemFire 5.5
 */
@SuppressWarnings("deprecation")
public abstract class CqQueryImpl implements InternalCqQuery {
  private static final Logger logger = LogService.getLogger();

  protected String cqName;

  protected String queryString;

  LocalRegion cqBaseRegion;

  protected Query query = null;

  InternalLogWriter securityLogWriter;

  protected CqServiceImpl cqService;

  protected String regionName;

  protected boolean isDurable = false;

  /** Stats counters */
  private CqStatisticsImpl cqStats;

  protected CqQueryVsdStats stats;

  protected final CqStateImpl cqState = new CqStateImpl();

  private ExecutionContext queryExecutionContext = null;

  public static TestHook testHook = null;

  /**
   * Constructor.
   */
  public CqQueryImpl() {}

  public CqQueryImpl(CqServiceImpl cqService, String cqName, String queryString,
      boolean isDurable) {
    this.cqName = cqName;
    this.queryString = queryString;
    this.securityLogWriter = (InternalLogWriter) cqService.getCache().getSecurityLoggerI18n();
    this.cqService = cqService;
    this.isDurable = isDurable;
  }

  /**
   * returns CQ name
   */
  @Override
  public String getName() {
    return this.cqName;
  }

  @Override
  public void setName(String cqName) {
    this.cqName = cqName;
  }

  @Override
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

  void updateCqCreateStats() {
    // Initialize the VSD statistics
    StatisticsFactory factory = cqService.getCache().getDistributedSystem();
    this.stats = new CqQueryVsdStats(factory, getServerCqName());
    this.cqStats = new CqStatisticsImpl(this);

    // Update statistics with CQ creation.
    this.cqService.stats().incCqsStopped();
    this.cqService.stats().incCqsCreated();
    this.cqService.stats().incCqsOnClient();
  }

  /**
   * Validates the CQ. Checks for cq constraints. Also sets the base region name.
   */
  void validateCq() {
    InternalCache cache = cqService.getInternalCache();
    DefaultQuery locQuery = (DefaultQuery) cache.getLocalQueryService().newQuery(this.queryString);
    this.query = locQuery;
    // assert locQuery != null;

    // validate Query.
    Object[] parameters = new Object[0]; // parameters are not permitted

    // check that it is only a SELECT statement (possibly with IMPORTs)
    CompiledSelect select = locQuery.getSimpleSelect();
    if (select == null) {
      throw new UnsupportedOperationException(
          "CQ queries must be a select statement only");
    }

    // must not be a DISTINCT select
    if (select.isDistinct()) {
      throw new UnsupportedOperationException(
          "select DISTINCT queries not supported in CQ");
    }

    // get the regions referenced in this query
    Set regionsInQuery = locQuery.getRegionsInQuery(parameters);
    // check for more than one region is referenced in the query
    // (though it could still be one region referenced multiple times)
    if (regionsInQuery.size() > 1 || regionsInQuery.size() < 1) {
      throw new UnsupportedOperationException(
          "CQ queries must reference one and only one region");
    }
    this.regionName = (String) regionsInQuery.iterator().next();

    // make sure the where clause references no regions
    Set regions = new HashSet();
    CompiledValue whereClause = select.getWhereClause();
    if (whereClause != null) {
      whereClause.getRegionsInQuery(regions, parameters);
      if (!regions.isEmpty()) {
        throw new UnsupportedOperationException(
            "The WHERE clause in CQ queries cannot refer to a region");
      }
    }
    List fromClause = select.getIterators();
    // cannot have more than one iterator in FROM clause
    if (fromClause.size() > 1) {
      throw new UnsupportedOperationException(
          "CQ queries cannot have more than one iterator in the FROM clause");
    }

    // the first iterator in the FROM clause must be just a CompiledRegion
    CompiledIteratorDef itrDef = (CompiledIteratorDef) fromClause.get(0);
    // By process of elimination, we know that the first iterator contains a reference
    // to the region. Check to make sure it is only a CompiledRegion
    if (!(itrDef.getCollectionExpr() instanceof CompiledRegion)) {
      throw new UnsupportedOperationException(
          "CQ queries must have a region path only as the first iterator in the FROM clause");
    }

    // must not have any projections
    List projs = select.getProjectionAttributes();
    if (projs != null) {
      throw new UnsupportedOperationException(
          "CQ queries do not support projections");
    }

    // check the orderByAttrs, not supported
    List orderBys = select.getOrderByAttrs();
    if (orderBys != null) {
      throw new UnsupportedOperationException(
          "CQ queries do not support ORDER BY");
    }

    // Set Query ExecutionContext, that will be used in later execution.
    this.setQueryExecutionContext(
        new QueryExecutionContext(null, (InternalCache) this.cqService.getCache(), true));
  }

  /**
   * Removes the CQ from CQ repository.
   */
  void removeFromCqMap() throws CqException {
    try {
      cqService.removeCq(this.getServerCqName());
    } catch (Exception ex) {
      String errMsg =
          "Failed to remove Continuous Query From the repository. CqName: %s Error : %s";
      Object[] errMsgArgs = new Object[] {cqName, ex.getLocalizedMessage()};
      String msg = String.format(errMsg, errMsgArgs);
      logger.error(msg);
      throw new CqException(msg, ex);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Removed CQ from the CQ repository. CQ Name: {}", this.cqName);
    }
  }

  /**
   * Returns the QueryString of this CQ.
   */
  @Override
  public String getQueryString() {
    return queryString;
  }

  /**
   * Return the query after replacing region names with parameters
   *
   * @return the Query for the query string
   */
  @Override
  public Query getQuery() {
    return query;
  }

  @Override
  public CqStatistics getStatistics() {
    return cqStats;
  }

  @Override
  public LocalRegion getCqBaseRegion() {
    return this.cqBaseRegion;
  }

  protected abstract void cleanup() throws CqException;

  /**
   * @return Returns the Region name on which this cq is created.
   */
  String getBaseRegionName() {

    return this.regionName;
  }

  @Override
  public abstract String getServerCqName();

  /**
   * Return the state of this query. Should not modify this state without first locking it.
   *
   * @return STOPPED RUNNING or CLOSED
   */
  @Override
  public CqState getState() {
    return this.cqState;
  }

  @Override
  public void setCqState(int state) {
    if (this.isClosed()) {
      throw new CqClosedException(
          String.format("CQ is closed, CqName : %s", this.cqName));
    }

    synchronized (cqState) {
      if (state == CqStateImpl.RUNNING) {
        this.cqState.setState(CqStateImpl.RUNNING);
        this.cqService.stats().incCqsActive();
        this.cqService.stats().decCqsStopped();
      } else if (state == CqStateImpl.STOPPED) {
        this.cqState.setState(CqStateImpl.STOPPED);
        this.cqService.stats().incCqsStopped();
        this.cqService.stats().decCqsActive();
      } else if (state == CqStateImpl.CLOSING) {
        this.cqState.setState(state);
      }
    }
  }

  /**
   * Update CQ stats
   *
   * @param cqEvent object
   */
  void updateStats(CqEvent cqEvent) {
    this.stats.updateStats(cqEvent); // Stats for VSD
  }

  /**
   * Return true if the CQ is in running state
   *
   * @return true if running, false otherwise
   */
  @Override
  public boolean isRunning() {
    return this.cqState.isRunning();
  }

  /**
   * Return true if the CQ is in stopped state
   *
   * @return true if stopped, false otherwise
   */
  @Override
  public boolean isStopped() {
    return this.cqState.isStopped();
  }

  /**
   * Return true if the CQ is closed
   *
   * @return true if closed, false otherwise
   */
  @Override
  public boolean isClosed() {
    return this.cqState.isClosed();
  }

  /**
   * Return true if the CQ is in closing state.
   *
   * @return true if close in progress, false otherwise
   */
  public boolean isClosing() {
    return this.cqState.isClosing();
  }

  /**
   * Return true if the CQ is durable
   *
   * @return true if durable, false otherwise
   */
  @Override
  public boolean isDurable() {
    return this.isDurable;
  }

  /**
   * Returns a reference to VSD stats of the CQ
   *
   * @return VSD stats of the CQ
   */
  @Override
  public CqQueryVsdStats getVsdStats() {
    return stats;
  }

  ExecutionContext getQueryExecutionContext() {
    return queryExecutionContext;
  }

  private void setQueryExecutionContext(ExecutionContext queryExecutionContext) {
    this.queryExecutionContext = queryExecutionContext;
  }

  /** Test Hook */
  public interface TestHook {
    void pauseUntilReady();

    void ready();

    int numQueuedEvents();

    void setEventCount(int count);
  }
}
