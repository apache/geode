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

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.cache.FilterProfile;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.logging.LogService;

/**
 * This class tracks GemFire statistics related to CqService.
 * Specifically the following statistics are tracked:
 *      Number of CQs created
 *      Number of active CQs
 *      Number of CQs suspended or stopped
 *      Number of CQs closed
 *      Number of CQs on a client 
 * 
 * @author Rao Madduri
 * @since 5.5 
 */
public class CqServiceVsdStats
{
  private static final Logger logger = LogService.getLogger();
  
  /** The <code>StatisticsType</code> of the statistics */
  private static final StatisticsType _type;

  /** Name of the created CQs statistic */
  protected static final String CQS_CREATED = "numCqsCreated";

  /** Name of the active CQs statistic */
  protected static final String CQS_ACTIVE = "numCqsActive";

  /** Name of the stopped CQs statistic */
  protected static final String CQS_STOPPED = "numCqsStopped";

  /** Name of the closed CQs statistic */
  protected static final String CQS_CLOSED = "numCqsClosed";

  /** Name of the client's CQs statistic */
  protected static final String CQS_ON_CLIENT = "numCqsOnClient";
  
  /** Number of clients with CQs statistic */
  protected static final String CLIENTS_WITH_CQS = "numClientsWithCqs";

  
  /** CQ query execution time. */
  protected static final String CQ_QUERY_EXECUTION_TIME = "cqQueryExecutionTime";

  /** CQ query execution in progress */
  protected static final String CQ_QUERY_EXECUTION_IN_PROGRESS = "cqQueryExecutionInProgress";

  /** Completed CQ query executions */
  protected static final String CQ_QUERY_EXECUTIONS_COMPLETED = "cqQueryExecutionsCompleted";

  /** Unique CQs, number of different CQ queries */
  protected static final String UNIQUE_CQ_QUERY = "numUniqueCqQuery";
  
  /** Id of the CQs created statistic */
  private static final int _numCqsCreatedId;

  /** Id of the active CQs statistic */
  private static final int _numCqsActiveId;

  /** Id of the stopped CQs statistic */
  private static final int _numCqsStoppedId;

  /** Id of the closed CQs statistic */
  private static final int _numCqsClosedId;

  /** Id of the CQs on client statistic */
  private static final int _numCqsOnClientId;
  
  /** Id of the Clients with Cqs statistic */
  private static final int _numClientsWithCqsId;

  /** Id for the CQ query execution time. */
  private static final int _cqQueryExecutionTimeId;

  /** Id for the CQ query execution in progress */
  private static final int _cqQueryExecutionInProgressId;

  /** Id for completed CQ query executions */
  private static final int _cqQueryExecutionsCompletedId;

  /** Id for unique CQs, difference in CQ queries */
  private static final int _numUniqueCqQuery;

  /**
   * Static initializer to create and initialize the <code>StatisticsType</code>
   */
  static {
    String statName = "CqServiceStats";
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();

    _type = f.createType(statName, statName, new StatisticDescriptor[] {
        f.createLongCounter(CQS_CREATED, "Number of CQs created.", "operations"),
        f.createLongCounter(CQS_ACTIVE, "Number of CQS actively executing.", "operations"),
        f.createLongCounter(CQS_STOPPED, "Number of CQs stopped.", "operations"),
        f.createLongCounter(CQS_CLOSED, "Number of CQs closed.", "operations"),
        f.createLongCounter(CQS_ON_CLIENT, "Number of CQs on the client.", "operations"),
        f.createLongCounter(CLIENTS_WITH_CQS, "Number of Clients with CQs.", "operations"),
        f.createLongCounter(CQ_QUERY_EXECUTION_TIME, "Time taken for CQ Query Execution.", "nanoseconds"),
        f.createLongCounter(CQ_QUERY_EXECUTIONS_COMPLETED, "Number of CQ Query Executions.", "operations"),
        f.createIntGauge(CQ_QUERY_EXECUTION_IN_PROGRESS, "CQ Query Execution In Progress.", "operations"),
        f.createIntGauge(UNIQUE_CQ_QUERY, "Number of Unique CQ Querys.", "Queries"),

    });

    // Initialize id fields
    _numCqsCreatedId = _type.nameToId(CQS_CREATED);
    _numCqsActiveId = _type.nameToId(CQS_ACTIVE);
    _numCqsStoppedId = _type.nameToId(CQS_STOPPED);
    _numCqsClosedId = _type.nameToId(CQS_CLOSED);
    _numCqsOnClientId = _type.nameToId(CQS_ON_CLIENT);
    _numClientsWithCqsId = _type.nameToId(CLIENTS_WITH_CQS);
    _cqQueryExecutionTimeId = _type.nameToId(CQ_QUERY_EXECUTION_TIME);    
    _cqQueryExecutionsCompletedId = _type.nameToId(CQ_QUERY_EXECUTIONS_COMPLETED);
    _cqQueryExecutionInProgressId = _type.nameToId(CQ_QUERY_EXECUTION_IN_PROGRESS);
    _numUniqueCqQuery = _type.nameToId(UNIQUE_CQ_QUERY);
    
  }

  /** The <code>Statistics</code> instance to which most behavior is delegated */
  private final Statistics _stats;

  /**
   * Constructor.
   * 
   * @param factory
   *          The <code>StatisticsFactory</code> which creates the
   *          <code>Statistics</code> instance
   */
  public CqServiceVsdStats(StatisticsFactory factory) {
    this._stats = factory.createAtomicStatistics(_type, "CqServiceStats");
  }

  // /////////////////// Instance Methods /////////////////////

  /**
   * Closes the <code>HARegionQueueStats</code>.
   */
  public void close()
  {
    this._stats.close();
  }

  /**
   * Returns the current value of the "numCqsCreated" stat.
   * 
   * @return the current value of the "numCqsCreated" stat
   */
  public long getNumCqsCreated()
  {
    return this._stats.getLong(_numCqsCreatedId);
  }

  /**
   * Increments the "numCqsCreated" stat by 1.
   */
  public void incCqsCreated()
  {
    this._stats.incLong(_numCqsCreatedId, 1);
  }

  /**
   * Returns the current value of the "numCqsActive" stat.
   * 
   * @return the current value of the "numCqsActive" stat
   */
  public long getNumCqsActive()
  {
    return this._stats.getLong(_numCqsActiveId);
  }

  /**
   * Increments the "numCqsActive" stat by 1.
   */
  public void incCqsActive()
  {
    this._stats.incLong(_numCqsActiveId, 1);
  }
  
  /**
   * Decrements the "numCqsActive" stat by 1.
   */
  public void decCqsActive()
  {
    this._stats.incLong(_numCqsActiveId, -1);
  }
  
  /**
   * Returns the current value of the "numCqsStopped" stat.
   * 
   * @return the current value of the "numCqsStopped" stat
   */
  public long getNumCqsStopped()
  {
    return this._stats.getLong(_numCqsStoppedId);
  }

  /**
   * Increments the "numCqsStopped" stat by 1.
   */
  public void incCqsStopped()
  {
    this._stats.incLong(_numCqsStoppedId, 1);
  }

  /**
   * Decrements the "numCqsStopped" stat by 1.
   */
  public void decCqsStopped()
  {
    this._stats.incLong(_numCqsStoppedId, -1);
  }

  /**
   * Returns the current value of the "numCqsClosed" stat.
   * 
   * @return the current value of the "numCqsClosed" stat
   */
  public long getNumCqsClosed()
  {
    return this._stats.getLong(_numCqsClosedId);
  }

  /**
   * Increments the "numCqsClosed" stat by 1.
   */
  public void incCqsClosed()
  {
    this._stats.incLong(_numCqsClosedId, 1);
  }

  /**
   * Returns the current value of the "numCqsOnClient" stat.
   * 
   * @return the current value of the "numCqsOnClient" stat
   */
  public long getNumCqsOnClient()
  {
    return this._stats.getLong(_numCqsOnClientId);
  }

  /**
   * Increments the "numCqsOnClient" stat by 1.
   */
  public void incCqsOnClient()
  {
    this._stats.incLong(_numCqsOnClientId, 1);
  }
  
  /**
   * Decrements the "numCqsOnClient" stat by 1.
   */
  public void decCqsOnClient()
  {
    this._stats.incLong(_numCqsOnClientId, -1);
  }
  
  /**
   * Returns the current value of the "numClientsWithCqs" stat.
   * 
   * @return the current value of the "numClientsWithCqs" stat
   */
  public long getNumClientsWithCqs()
  {
    return this._stats.getLong(_numClientsWithCqsId);
  }

  /**
   * Increments the "numClientsWithCqs" stat by 1.
   */
  public void incClientsWithCqs()
  {
    this._stats.incLong(_numClientsWithCqsId, 1);
  }
  
  /**
   * Decrements the "numCqsOnClient" stat by 1.
   */
  public void decClientsWithCqs()
  {
    this._stats.incLong(_numClientsWithCqsId, -1);
  }
  
  /**
   * Start the CQ Query Execution time.
   */
  public long startCqQueryExecution() {
    this._stats.incInt(_cqQueryExecutionInProgressId, 1);
    return NanoTimer.getTime(); 
  }

  /**
   * End CQ Query Execution Time.
   * @param start long time value.
   */
  public void endCqQueryExecution(long start) {
    long ts = NanoTimer.getTime(); 
    this._stats.incLong(_cqQueryExecutionTimeId, ts-start);
    this._stats.incInt(_cqQueryExecutionInProgressId, -1);
    this._stats.incLong(_cqQueryExecutionsCompletedId, 1);
  }
  
  /**
   * Returns the total time spent executing the CQ Queries.
   * @return long time spent.
   */
  public long getCqQueryExecutionTime(){
    return this._stats.getLong(_cqQueryExecutionTimeId);
  }

  /**
   * Increments number of Unique queries.
   */
  public void incUniqueCqQuery()
  {
    this._stats.incInt(_numUniqueCqQuery, 1);
  }

  /**
   * Decrements number of unique Queries.
   */
  public void decUniqueCqQuery()
  {
    this._stats.incInt(_numUniqueCqQuery, -1);
  }
  
  
  /**
   * This is a test method.  It silently ignores exceptions and should not be
   * used outside of unit tests.<p>
   * Returns the number of CQs (active + suspended) on the given region.
   * @param regionName
   */
  public long numCqsOnRegion(String regionName){
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache == null) {
      return 0;
    }
    DefaultQueryService queryService = (DefaultQueryService)cache.getQueryService();
    CqService cqService = null;
    try {
      cqService = queryService.getCqService();
    }
    catch (CqException e) {
      if (logger.isDebugEnabled()) {
        logger.debug("Failed to get CqService {}", e.getLocalizedMessage());
      }
      e.printStackTrace();
      return -1; // We're confused
    }
    if (((CqServiceImpl) cqService).isServer()) {
      //If we are on the server, look at the number of CQs in the filter profile.
      try {
        FilterProfile fp = cache.getFilterProfile(regionName);
        if (fp == null) {
          return 0;
        }
        return fp.getCqCount();
      }
      catch (Exception ex) {
        if (logger.isDebugEnabled()) {
          logger.debug("Failed to get serverside CQ count for region: {} {}", regionName, ex.getLocalizedMessage());
        }
      }
    }
    else {
      try {
        CqQuery[] cqs = queryService.getCqs(regionName);
        
        if (cqs != null) {
          return cqs.length;
        }
      } catch(Exception ex) {
        // Dont do anything.
      }
    }
    return 0;
  } 
}
