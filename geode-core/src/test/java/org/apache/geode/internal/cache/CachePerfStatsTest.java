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

import static org.apache.geode.internal.cache.CachePerfStats.cacheListenerCallsCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.cacheWriterCallsCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.clearsId;
import static org.apache.geode.internal.cache.CachePerfStats.createsId;
import static org.apache.geode.internal.cache.CachePerfStats.deltaFailedUpdatesId;
import static org.apache.geode.internal.cache.CachePerfStats.deltaFullValuesRequestedId;
import static org.apache.geode.internal.cache.CachePerfStats.deltaFullValuesSentId;
import static org.apache.geode.internal.cache.CachePerfStats.deltaGetInitialImagesCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.deltaUpdatesId;
import static org.apache.geode.internal.cache.CachePerfStats.deltasPreparedId;
import static org.apache.geode.internal.cache.CachePerfStats.deltasSentId;
import static org.apache.geode.internal.cache.CachePerfStats.destroysId;
import static org.apache.geode.internal.cache.CachePerfStats.entryCountId;
import static org.apache.geode.internal.cache.CachePerfStats.evictorJobsCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.evictorJobsStartedId;
import static org.apache.geode.internal.cache.CachePerfStats.getInitialImagesCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.getTimeId;
import static org.apache.geode.internal.cache.CachePerfStats.getsId;
import static org.apache.geode.internal.cache.CachePerfStats.handlingNetsearchesCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.handlingNetsearchesFailedId;
import static org.apache.geode.internal.cache.CachePerfStats.handlingNetsearchesFailedTimeId;
import static org.apache.geode.internal.cache.CachePerfStats.handlingNetsearchesInProgressId;
import static org.apache.geode.internal.cache.CachePerfStats.handlingNetsearchesTimeId;
import static org.apache.geode.internal.cache.CachePerfStats.indexUpdateCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.invalidatesId;
import static org.apache.geode.internal.cache.CachePerfStats.loadsCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.missesId;
import static org.apache.geode.internal.cache.CachePerfStats.netloadsCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.netsearchesCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.putAllsId;
import static org.apache.geode.internal.cache.CachePerfStats.putTimeId;
import static org.apache.geode.internal.cache.CachePerfStats.putsId;
import static org.apache.geode.internal.cache.CachePerfStats.queryExecutionsId;
import static org.apache.geode.internal.cache.CachePerfStats.removeAllsId;
import static org.apache.geode.internal.cache.CachePerfStats.retriesId;
import static org.apache.geode.internal.cache.CachePerfStats.txCommitChangesId;
import static org.apache.geode.internal.cache.CachePerfStats.txCommitsId;
import static org.apache.geode.internal.cache.CachePerfStats.txFailureChangesId;
import static org.apache.geode.internal.cache.CachePerfStats.txFailuresId;
import static org.apache.geode.internal.cache.CachePerfStats.txRollbackChangesId;
import static org.apache.geode.internal.cache.CachePerfStats.txRollbacksId;
import static org.apache.geode.internal.cache.CachePerfStats.updatesId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.statistics.StatisticsClock;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.internal.statistics.StripedStatisticsImpl;

/**
 * Unit tests for {@link CachePerfStats}.
 */
public class CachePerfStatsTest {

  private static final String TEXT_ID = "cachePerfStats";
  private static final long CLOCK_TIME = 10;

  private Statistics statistics;
  private StatisticsClock statisticsClock;
  private CachePerfStats cachePerfStats;

  @Before
  public void setUp() {
    StatisticsType statisticsType = CachePerfStats.getStatisticsType();

    StatisticsManager statisticsManager = mock(StatisticsManager.class);
    StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);

    statistics = new StripedStatisticsImpl(statisticsType, TEXT_ID, 1, 1, statisticsManager);
    statisticsClock = mock(StatisticsClock.class);

    when(statisticsClock.isEnabled())
        .thenReturn(true);
    when(statisticsClock.getTime())
        .thenReturn(CLOCK_TIME);
    when(statisticsFactory.createAtomicStatistics(eq(statisticsType), eq(TEXT_ID)))
        .thenReturn(statistics);

    cachePerfStats = new CachePerfStats(statisticsFactory, TEXT_ID, statisticsClock);
  }

  @Test
  public void getPutsDelegatesToStatistics() {
    statistics.incLong(putsId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getPuts()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code puts} is to invoke {@code
   * endPut}.
   */
  @Test
  public void endPutIncrementsPuts() {
    cachePerfStats.endPut(0, false);

    assertThat(statistics.getLong(putsId)).isEqualTo(1L);
  }

  /**
   * Characterization test: {@code puts} currently wraps to negative from max long value.
   */
  @Test
  public void putsWrapsFromMaxLongToNegativeValue() {
    statistics.incLong(putsId, Long.MAX_VALUE);

    cachePerfStats.endPut(0, false);

    assertThat(cachePerfStats.getPuts()).isNegative();
  }

  @Test
  public void getGetsDelegatesToStatistics() {
    statistics.incLong(getsId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getGets()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code gets} is to invoke {@code
   * endGet}.
   */
  @Test
  public void endGetIncrementsGets() {
    cachePerfStats.endGet(0, false);

    assertThat(statistics.getLong(getsId)).isEqualTo(1L);
  }

  /**
   * Characterization test: {@code gets} currently wraps to negative from max long value.
   */
  @Test
  public void getsWrapsFromMaxLongToNegativeValue() {
    statistics.incLong(getsId, Long.MAX_VALUE);

    cachePerfStats.endGet(0, false);

    assertThat(cachePerfStats.getGets()).isNegative();
  }

  @Test
  public void getPutTimeDelegatesToStatistics() {
    statistics.incLong(putTimeId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getPutTime()).isEqualTo(Long.MAX_VALUE);

  }

  /**
   * Characterization test: Note that the only way to increment {@code putTime} is to invoke {@code
   * endPut}.
   */
  @Test
  public void endPutIncrementsPutTime() {
    cachePerfStats.endPut(0, false);

    assertThat(statistics.getLong(putTimeId)).isEqualTo(CLOCK_TIME);
  }

  @Test
  public void getGetTimeDelegatesToStatistics() {
    statistics.incLong(getTimeId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getGetTime()).isEqualTo(Long.MAX_VALUE);

  }

  /**
   * Characterization test: Note that the only way to increment {@code getTime} is to invoke {@code
   * endGet}.
   */
  @Test
  public void endGetIncrementsGetTime() {
    cachePerfStats.endGet(0, false);

    assertThat(statistics.getLong(getTimeId)).isEqualTo(CLOCK_TIME);
  }

  @Test
  public void getDestroysDelegatesToStatistics() {
    statistics.incLong(destroysId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getDestroys()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void incDestroysIncrementsDestroys() {
    cachePerfStats.incDestroys();

    assertThat(statistics.getLong(destroysId)).isEqualTo(1L);
  }

  /**
   * Characterization test: {@code destroys} currently wraps to negative from max long value.
   */
  @Test
  public void destroysWrapsFromMaxLongToNegativeValue() {
    statistics.incLong(destroysId, Long.MAX_VALUE);

    cachePerfStats.incDestroys();

    assertThat(cachePerfStats.getDestroys()).isNegative();
  }

  @Test
  public void getCreatesDelegatesToStatistics() {
    statistics.incLong(createsId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getCreates()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void incCreatesIncrementsCreates() {
    cachePerfStats.incCreates();

    assertThat(statistics.getLong(createsId)).isEqualTo(1L);
  }

  /**
   * Characterization test: {@code creates} currently wraps to negative from max long value.
   */
  @Test
  public void createsWrapsFromMaxLongToNegativeValue() {
    statistics.incLong(createsId, Long.MAX_VALUE);

    cachePerfStats.incCreates();

    assertThat(cachePerfStats.getCreates()).isNegative();
  }

  @Test
  public void getPutAllsDelegatesToStatistics() {
    statistics.incLong(putAllsId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getPutAlls()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code putalls} is to invoke {@code
   * endPutAll}.
   */
  @Test
  public void endPutAllIncrementsPutAlls() {
    cachePerfStats.endPutAll(0);

    assertThat(statistics.getLong(putAllsId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code putAlls} currently wraps to negative from max integer value.
   */
  @Test
  public void putAllsWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(putAllsId, Long.MAX_VALUE);

    cachePerfStats.endPutAll(0);

    assertThat(cachePerfStats.getPutAlls()).isNegative();
  }

  @Test
  public void getRemoveAllsDelegatesToStatistics() {
    statistics.incLong(removeAllsId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getRemoveAlls()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code removeAlls} is to invoke
   * {@code endRemoveAll}.
   */
  @Test
  public void endRemoveAllIncrementsRemoveAll() {
    cachePerfStats.endRemoveAll(0);

    assertThat(statistics.getLong(removeAllsId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code removeAlls} currently wraps to negative from max integer value.
   */
  @Test
  public void removeAllsWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(removeAllsId, Long.MAX_VALUE);

    cachePerfStats.endRemoveAll(0);

    assertThat(cachePerfStats.getRemoveAlls()).isNegative();
  }

  @Test
  public void getUpdatesDelegatesToStatistics() {
    statistics.incLong(updatesId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getUpdates()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code updates} is to invoke {@code
   * endPut}.
   */
  @Test
  public void endPutIncrementsUpdates() {
    cachePerfStats.endPut(0, true);

    assertThat(statistics.getLong(updatesId)).isEqualTo(1L);
  }

  /**
   * Characterization test: {@code updates} currently wraps to negative from max long value.
   */
  @Test
  public void updatesWrapsFromMaxLongToNegativeValue() {
    statistics.incLong(updatesId, Long.MAX_VALUE);

    cachePerfStats.endPut(0, true);

    assertThat(cachePerfStats.getUpdates()).isNegative();
  }

  @Test
  public void getInvalidatesDelegatesToStatistics() {
    statistics.incLong(invalidatesId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getInvalidates()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void incInvalidatesIncrementsInvalidates() {
    cachePerfStats.incInvalidates();

    assertThat(statistics.getLong(invalidatesId)).isEqualTo(1L);
  }

  /**
   * Characterization test: {@code invalidates} currently wraps to negative from max long value.
   */
  @Test
  public void invalidatesWrapsFromMaxLongToNegativeValue() {
    statistics.incLong(invalidatesId, Long.MAX_VALUE);

    cachePerfStats.incInvalidates();

    assertThat(cachePerfStats.getInvalidates()).isNegative();
  }

  @Test
  public void getMissesDelegatesToStatistics() {
    statistics.incLong(missesId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getMisses()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code misses} is to invoke {@code
   * endGet}.
   */
  @Test
  public void endGetIncrementsMisses() {
    cachePerfStats.endGet(0, true);

    assertThat(statistics.getLong(missesId)).isEqualTo(1L);
  }

  /**
   * Characterization test: {@code misses} currently wraps to negative from max long value.
   */
  @Test
  public void missesWrapsFromMaxLongToNegativeValue() {
    statistics.incLong(missesId, Long.MAX_VALUE);

    cachePerfStats.endGet(0, true);

    assertThat(cachePerfStats.getMisses()).isNegative();
  }

  @Test
  public void getRetriesDelegatesToStatistics() {
    statistics.incLong(retriesId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getRetries()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void incRetriesIncrementsRetries() {
    cachePerfStats.incRetries();

    assertThat(statistics.getLong(retriesId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code retries} currently wraps to negative from max integer value.
   */
  @Test
  public void retriesWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(retriesId, Long.MAX_VALUE);

    cachePerfStats.incRetries();

    assertThat(cachePerfStats.getRetries()).isNegative();
  }

  @Test
  public void getClearsDelegatesToStatistics() {
    statistics.incLong(clearsId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getClearCount()).isEqualTo(Long.MAX_VALUE);
  }



  @Test
  public void incClearCountIncrementsClears() {
    cachePerfStats.incClearCount();

    assertThat(statistics.getLong(clearsId)).isEqualTo(1L);
  }

  /**
   * Characterization test: {@code clears} currently wraps to negative from max long value.
   */
  @Test
  public void clearsWrapsFromMaxLongToNegativeValue() {
    statistics.incLong(clearsId, Long.MAX_VALUE);

    cachePerfStats.incClearCount();

    assertThat(cachePerfStats.getClearCount()).isNegative();
  }

  @Test
  public void getLoadsCompletedDelegatesToStatistics() {
    statistics.incLong(loadsCompletedId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getLoadsCompleted()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code loadsCompleted} is to invoke
   * {@code endLoad}.
   */
  @Test
  public void endLoadIncrementsMisses() {
    cachePerfStats.endLoad(0);

    assertThat(statistics.getLong(loadsCompletedId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code loads} currently wraps to negative from max integer value.
   */
  @Test
  public void loadsCompletedWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(loadsCompletedId, Long.MAX_VALUE);

    cachePerfStats.endLoad(0);

    assertThat(cachePerfStats.getLoadsCompleted()).isNegative();
  }

  @Test
  public void getNetloadsCompletedDelegatesToStatistics() {
    statistics.incLong(netloadsCompletedId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getNetloadsCompleted()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code netloadsCompleted} is to
   * invoke {@code endNetload}.
   */
  @Test
  public void endNetloadIncrementsNetloadsCompleted() {
    cachePerfStats.endNetload(0);

    assertThat(statistics.getLong(netloadsCompletedId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code netloadsComplete} currently wraps to negative from max integer
   * value.
   */
  @Test
  public void netloadsCompletedWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(netloadsCompletedId, Long.MAX_VALUE);

    cachePerfStats.endNetload(0);

    assertThat(cachePerfStats.getNetloadsCompleted()).isNegative();
  }

  @Test
  public void getNetsearchesCompletedDelegatesToStatistics() {
    statistics.incLong(netsearchesCompletedId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getNetsearchesCompleted()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code netsearchesCompleted} is to
   * invoke {@code endNetsearch}.
   */
  @Test
  public void endLoadIncrementsNetsearchesCompleted() {
    cachePerfStats.endNetsearch(0);

    assertThat(statistics.getLong(netsearchesCompletedId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code netsearchesCompleted} currently wraps to negative from max
   * integer value.
   */
  @Test
  public void netsearchesCompletedWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(netsearchesCompletedId, Long.MAX_VALUE);

    cachePerfStats.endNetsearch(0);

    assertThat(cachePerfStats.getNetsearchesCompleted()).isNegative();
  }

  @Test
  public void getCacheWriterCallsCompletedDelegatesToStatistics() {
    statistics.incLong(cacheWriterCallsCompletedId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getCacheWriterCallsCompleted()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code cacheWriterCallsCompleted} is
   * to invoke {@code endCacheWriterCall}.
   */
  @Test
  public void endCacheWriterCallIncrementsCacheWriterCallsCompleted() {
    cachePerfStats.endCacheWriterCall(0);

    assertThat(statistics.getLong(cacheWriterCallsCompletedId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code cacheWriterCallsCompleted} currently wraps to negative from max
   * integer value.
   */
  @Test
  public void cacheWriterCallsCompletedWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(cacheWriterCallsCompletedId, Long.MAX_VALUE);

    cachePerfStats.endCacheWriterCall(0);

    assertThat(cachePerfStats.getCacheWriterCallsCompleted()).isNegative();
  }

  @Test
  public void getCacheListenerCallsCompletedDelegatesToStatistics() {
    statistics.incLong(cacheListenerCallsCompletedId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getCacheListenerCallsCompleted()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code cacheListenerCallsCompleted}
   * is to invoke {@code endCacheListenerCall}.
   */
  @Test
  public void endCacheWriterCallIncrementsCacheListenerCallsCompleted() {
    cachePerfStats.endCacheListenerCall(0);

    assertThat(statistics.getLong(cacheListenerCallsCompletedId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code cacheListenerCallsCompleted} currently wraps to negative from max
   * integer value.
   */
  @Test
  public void cacheListenerCallsCompletedWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(cacheListenerCallsCompletedId, Long.MAX_VALUE);

    cachePerfStats.endCacheListenerCall(0);

    assertThat(cachePerfStats.getCacheListenerCallsCompleted()).isNegative();
  }

  @Test
  public void getGetInitialImagesCompletedDelegatesToStatistics() {
    statistics.incLong(getInitialImagesCompletedId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getGetInitialImagesCompleted()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code getInitialImagesCompleted} is
   * to invoke {@code endGetInitialImage}.
   */
  @Test
  public void endCacheWriterCallIncrementsGetInitialImagesCompleted() {
    cachePerfStats.endGetInitialImage(0);

    assertThat(statistics.getLong(getInitialImagesCompletedId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code getInitialImagesCompleted} currently wraps to negative from max
   * integer value.
   */
  @Test
  public void getInitialImagesCompletedCallsCompletedWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(getInitialImagesCompletedId, Long.MAX_VALUE);

    cachePerfStats.endGetInitialImage(0);

    assertThat(cachePerfStats.getGetInitialImagesCompleted()).isNegative();
  }

  @Test
  public void getDeltaGetInitialImagesCompletedDelegatesToStatistics() {
    statistics.incLong(deltaGetInitialImagesCompletedId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getDeltaGetInitialImagesCompleted()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void incDeltaGIICompletedIncrementsDeltaGetInitialImagesCompleted() {
    cachePerfStats.incDeltaGIICompleted();

    assertThat(statistics.getLong(deltaGetInitialImagesCompletedId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code deltaGetInitialImagesCompleted} currently wraps to negative from
   * max integer value.
   */
  @Test
  public void deltaGetInitialImagesCompletedWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(deltaGetInitialImagesCompletedId, Long.MAX_VALUE);

    cachePerfStats.incDeltaGIICompleted();

    assertThat(cachePerfStats.getDeltaGetInitialImagesCompleted()).isNegative();
  }

  @Test
  public void getQueryExecutionsDelegatesToStatistics() {
    statistics.incLong(queryExecutionsId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getQueryExecutions()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code queryExecutions} is to invoke
   * {@code endQueryExecution}.
   */
  @Test
  public void endQueryExecutionIncrementsQueryExecutions() {
    cachePerfStats.endQueryExecution(1);

    assertThat(statistics.getLong(queryExecutionsId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code queryExecutions} currently wraps to negative from max integer
   * value.
   */
  @Test
  public void queryExecutionsWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(queryExecutionsId, Long.MAX_VALUE);

    cachePerfStats.endQueryExecution(1);

    assertThat(cachePerfStats.getQueryExecutions()).isNegative();
  }

  @Test
  public void getTxCommitsDelegatesToStatistics() {
    statistics.incLong(txCommitsId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getTxCommits()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code txCommits} is to invoke
   * {@code txSuccess}.
   */
  @Test
  public void txSuccessIncrementsTxCommits() {
    cachePerfStats.txSuccess(1, 1, 1);

    assertThat(statistics.getLong(txCommitsId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code txCommits} currently wraps to negative from max integer value.
   */
  @Test
  public void txCommitsWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(txCommitsId, Long.MAX_VALUE);

    cachePerfStats.txSuccess(1, 1, 1);

    assertThat(cachePerfStats.getTxCommits()).isNegative();
  }

  @Test
  public void getTxFailuresDelegatesToStatistics() {
    statistics.incLong(txFailuresId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getTxFailures()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code txFailures} is to invoke
   * {@code txFailure}.
   */
  @Test
  public void txFailureIncrementsTxFailures() {
    cachePerfStats.txFailure(1, 1, 1);

    assertThat(statistics.getLong(txFailuresId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code txFailures} currently wraps to negative from max integer value.
   */
  @Test
  public void txFailuresWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(txFailuresId, Long.MAX_VALUE);

    cachePerfStats.txFailure(1, 1, 1);

    assertThat(cachePerfStats.getTxFailures()).isNegative();
  }

  @Test
  public void getTxRollbacksDelegatesToStatistics() {
    statistics.incLong(txRollbacksId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getTxRollbacks()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code txRollbacks} is to invoke
   * {@code txRollback}.
   */
  @Test
  public void txRollbackIncrementsTxRollbacks() {
    cachePerfStats.txRollback(1, 1, 1);

    assertThat(statistics.getLong(txRollbacksId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code txRollbacks} currently wraps to negative from max integer value.
   */
  @Test
  public void txRollbacksWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(txRollbacksId, Long.MAX_VALUE);

    cachePerfStats.txRollback(1, 1, 1);

    assertThat(cachePerfStats.getTxRollbacks()).isNegative();
  }

  @Test
  public void getTxCommitChangesDelegatesToStatistics() {
    statistics.incLong(txCommitChangesId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getTxCommitChanges()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code txCommitChanges} is to invoke
   * {@code txSuccess}.
   */
  @Test
  public void txSuccessIncrementsTxCommitChanges() {
    cachePerfStats.txSuccess(1, 1, 1);

    assertThat(statistics.getLong(txCommitChangesId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code txCommitChanges} currently wraps to negative from max integer
   * value.
   */
  @Test
  public void txCommitChangesWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(txCommitChangesId, Long.MAX_VALUE);

    cachePerfStats.txSuccess(1, 1, 1);

    assertThat(cachePerfStats.getTxCommitChanges()).isNegative();
  }

  @Test
  public void getTxFailureChangesDelegatesToStatistics() {
    statistics.incLong(txFailureChangesId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getTxFailureChanges()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code txFailureChanges} is to
   * invoke {@code txFailure}.
   */
  @Test
  public void txFailureIncrementsTxFailureChanges() {
    cachePerfStats.txFailure(1, 1, 1);

    assertThat(statistics.getLong(txFailureChangesId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code txFailureChanges} currently wraps to negative from max integer
   * value.
   */
  @Test
  public void txFailureChangesWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(txFailureChangesId, Long.MAX_VALUE);

    cachePerfStats.txFailure(1, 1, 1);

    assertThat(cachePerfStats.getTxFailureChanges()).isNegative();
  }

  @Test
  public void getTxRollbackChangesDelegatesToStatistics() {
    statistics.incLong(txRollbackChangesId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getTxRollbackChanges()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code txRollbackChanges} is to
   * invoke {@code txRollback}.
   */
  @Test
  public void txRollbackIncrementsTxRollbackChanges() {
    cachePerfStats.txRollback(1, 1, 1);

    assertThat(statistics.getLong(txRollbackChangesId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code txRollbackChanges} currently wraps to negative from max integer
   * value.
   */
  @Test
  public void txRollbackChangesWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(txRollbackChangesId, Long.MAX_VALUE);

    cachePerfStats.txRollback(1, 1, 1);

    assertThat(cachePerfStats.getTxRollbackChanges()).isNegative();
  }

  @Test
  public void getEvictorJobsStartedChangesDelegatesToStatistics() {
    statistics.incLong(evictorJobsStartedId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getEvictorJobsStarted()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void incEvictorJobsStartedIncrementsEvictorJobsStarted() {
    cachePerfStats.incEvictorJobsStarted();

    assertThat(statistics.getLong(evictorJobsStartedId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code evictorJobsStarted} currently wraps to negative from max integer
   * value.
   */
  @Test
  public void evictorJobsStartedWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(evictorJobsStartedId, Long.MAX_VALUE);

    cachePerfStats.incEvictorJobsStarted();

    assertThat(cachePerfStats.getEvictorJobsStarted()).isNegative();
  }

  @Test
  public void getEvictorJobsCompletedChangesDelegatesToStatistics() {
    statistics.incLong(evictorJobsCompletedId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getEvictorJobsCompleted()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void incEvictorJobsCompletedIncrementsEvictorJobsCompleted() {
    cachePerfStats.incEvictorJobsCompleted();

    assertThat(statistics.getLong(evictorJobsCompletedId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code evictorJobsCompleted} currently wraps to negative from max
   * integer value.
   */
  @Test
  public void evictorJobsCompletedWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(evictorJobsCompletedId, Long.MAX_VALUE);

    cachePerfStats.incEvictorJobsCompleted();

    assertThat(cachePerfStats.getEvictorJobsCompleted()).isNegative();
  }

  @Test
  public void getIndexUpdateCompletedChangesDelegatesToStatistics() {
    statistics.incLong(indexUpdateCompletedId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getIndexUpdateCompleted()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code indexUpdateCompleted} is to
   * invoke {@code endIndexUpdate}.
   */
  @Test
  public void endIndexUpdateIncrementsEvictorJobsCompleted() {
    cachePerfStats.endIndexUpdate(1);

    assertThat(statistics.getLong(indexUpdateCompletedId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code indexUpdateCompleted} currently wraps to negative from max
   * integer value.
   */
  @Test
  public void indexUpdateCompletedWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(indexUpdateCompletedId, Long.MAX_VALUE);

    cachePerfStats.endIndexUpdate(1);

    assertThat(cachePerfStats.getIndexUpdateCompleted()).isNegative();
  }

  @Test
  public void getDeltaUpdatesDelegatesToStatistics() {
    statistics.incLong(deltaUpdatesId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getDeltaUpdates()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code deltaUpdates} is to invoke
   * {@code endDeltaUpdate}.
   */
  @Test
  public void endDeltaUpdateIncrementsDeltaUpdates() {
    cachePerfStats.endDeltaUpdate(1);

    assertThat(statistics.getLong(deltaUpdatesId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code deltaUpdatesId} currently wraps to negative from max integer
   * value.
   */
  @Test
  public void deltaUpdatesWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(deltaUpdatesId, Long.MAX_VALUE);

    cachePerfStats.endDeltaUpdate(1);

    assertThat(cachePerfStats.getDeltaUpdates()).isNegative();
  }

  @Test
  public void getDeltaFailedUpdatesDelegatesToStatistics() {
    statistics.incLong(deltaFailedUpdatesId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getDeltaFailedUpdates()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void incDeltaFailedUpdatesIncrementsDeltaFailedUpdates() {
    cachePerfStats.incDeltaFailedUpdates();

    assertThat(statistics.getLong(deltaFailedUpdatesId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code deltaFailedUpdates} currently wraps to negative from max integer
   * value.
   */
  @Test
  public void deltaFailedUpdatesWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(deltaFailedUpdatesId, Long.MAX_VALUE);

    cachePerfStats.incDeltaFailedUpdates();

    assertThat(cachePerfStats.getDeltaFailedUpdates()).isNegative();
  }

  @Test
  public void getDeltasPreparedUpdatesDelegatesToStatistics() {
    statistics.incLong(deltasPreparedId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getDeltasPrepared()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code deltasPrepared} is to invoke
   * {@code endDeltaPrepared}.
   */
  @Test
  public void endDeltaPreparedIncrementsDeltasPrepared() {
    cachePerfStats.endDeltaPrepared(1);

    assertThat(statistics.getLong(deltasPreparedId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code deltasPrepared} currently wraps to negative from max integer
   * value.
   */
  @Test
  public void deltasPreparedWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(deltasPreparedId, Long.MAX_VALUE);

    cachePerfStats.endDeltaPrepared(1);

    assertThat(cachePerfStats.getDeltasPrepared()).isNegative();
  }

  @Test
  public void getDeltasSentDelegatesToStatistics() {
    statistics.incLong(deltasSentId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getDeltasSent()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void incDeltasSentPreparedIncrementsDeltasSent() {
    cachePerfStats.incDeltasSent();

    assertThat(statistics.getLong(deltasSentId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code deltasSent} currently wraps to negative from max integer value.
   */
  @Test
  public void deltasSentWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(deltasSentId, Long.MAX_VALUE);

    cachePerfStats.incDeltasSent();

    assertThat(cachePerfStats.getDeltasSent()).isNegative();
  }

  @Test
  public void getDeltaFullValuesSentDelegatesToStatistics() {
    statistics.incLong(deltaFullValuesSentId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getDeltaFullValuesSent()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void incDeltaFullValuesSentIncrementsDeltaFullValuesSent() {
    cachePerfStats.incDeltaFullValuesSent();

    assertThat(statistics.getLong(deltaFullValuesSentId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code deltaFullValuesSent} currently wraps to negative from max integer
   * value.
   */
  @Test
  public void deltaFullValuesSentWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(deltaFullValuesSentId, Long.MAX_VALUE);

    cachePerfStats.incDeltaFullValuesSent();

    assertThat(cachePerfStats.getDeltaFullValuesSent()).isNegative();
  }

  @Test
  public void getDeltaFullValuesRequestedDelegatesToStatistics() {
    statistics.incLong(deltaFullValuesRequestedId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getDeltaFullValuesRequested()).isEqualTo(Long.MAX_VALUE);
  }

  @Test
  public void incDeltaFullValuesRequestedIncrementsDeltaFullValuesRequested() {
    cachePerfStats.incDeltaFullValuesRequested();

    assertThat(statistics.getLong(deltaFullValuesRequestedId)).isEqualTo(1);
  }

  /**
   * Characterization test: {@code deltaFullValuesRequested} currently wraps to negative from max
   * integer value.
   */
  @Test
  public void deltaFullValuesRequestedWrapsFromMaxIntegerToNegativeValue() {
    statistics.incLong(deltaFullValuesRequestedId, Long.MAX_VALUE);

    cachePerfStats.incDeltaFullValuesRequested();

    assertThat(cachePerfStats.getDeltaFullValuesRequested()).isNegative();
  }

  @Test
  public void incEntryCount_whenDeltaIsPositive_increasesTheEntryCountStat() {
    cachePerfStats.incEntryCount(2);

    assertThat(statistics.getLong(entryCountId)).isEqualTo(2);
  }

  @Test
  public void incEntryCount_whenDeltaIsNegative_decreasesTheEntryCountStat() {
    cachePerfStats.incEntryCount(-2);

    assertThat(statistics.getLong(entryCountId)).isEqualTo(-2);
  }

  @Test
  public void handlingNetsearchesInProgressIsZeroByDefault() {
    assertThat(statistics.getLong(handlingNetsearchesInProgressId)).isZero();
  }

  @Test
  public void handlingNetsearchesCompletedIsZeroByDefault() {
    assertThat(statistics.getLong(handlingNetsearchesCompletedId)).isZero();
  }

  @Test
  public void handlingNetsearchesTimeIsZeroByDefault() {
    assertThat(statistics.getLong(handlingNetsearchesTimeId)).isZero();
  }

  @Test
  public void handlingNetsearchesFailedIsZeroByDefault() {
    assertThat(statistics.getLong(handlingNetsearchesFailedId)).isZero();
  }

  @Test
  public void handlingNetsearchesFailedTimeIsZeroByDefault() {
    assertThat(statistics.getLong(handlingNetsearchesFailedTimeId)).isZero();
  }

  @Test
  public void startHandlingNetsearchIncreasesHandlingNetsearchesInProgress() {
    doReturn(1L, 10L).when(statisticsClock).getTime();

    cachePerfStats.startHandlingNetsearch();

    assertThat(statistics.getLong(handlingNetsearchesInProgressId)).isOne();
  }

  @Test
  public void endHandlingNetsearchIncreasesHandlingNetsearchesCompletedIfSuccess() {
    doReturn(1L, 10L).when(statisticsClock).getTime();
    long startTime = cachePerfStats.startHandlingNetsearch();

    cachePerfStats.endHandlingNetsearch(startTime, true);

    assertThat(statistics.getLong(handlingNetsearchesCompletedId)).isOne();
  }

  @Test
  public void endHandlingNetsearchIncreasesHandlingNetsearchesTimeIfSuccess() {
    doReturn(1L, 10L).when(statisticsClock).getTime();
    long startTime = cachePerfStats.startHandlingNetsearch();

    cachePerfStats.endHandlingNetsearch(startTime, true);

    assertThat(statistics.getLong(handlingNetsearchesTimeId)).isEqualTo(9);
  }

  @Test
  public void endHandlingNetsearchIncreasesHandlingNetsearchesFailedIfNotSuccess() {
    doReturn(1L, 10L).when(statisticsClock).getTime();
    long startTime = cachePerfStats.startHandlingNetsearch();

    cachePerfStats.endHandlingNetsearch(startTime, false);

    assertThat(statistics.getLong(handlingNetsearchesFailedId)).isOne();
  }

  @Test
  public void endHandlingNetsearchIncreasesHandlingNetsearchesFailedTimeIfNotSuccess() {
    doReturn(1L, 10L).when(statisticsClock).getTime();
    long startTime = cachePerfStats.startHandlingNetsearch();

    cachePerfStats.endHandlingNetsearch(startTime, false);

    assertThat(statistics.getLong(handlingNetsearchesFailedTimeId)).isEqualTo(9);
  }

  @Test
  public void endHandlingNetsearchDecreasesHandlingNetsearchesInProgressIfSuccess() {
    doReturn(1L, 10L).when(statisticsClock).getTime();
    long startTime = cachePerfStats.startHandlingNetsearch();

    cachePerfStats.endHandlingNetsearch(startTime, true);

    assertThat(statistics.getLong(handlingNetsearchesInProgressId)).isZero();
  }

  @Test
  public void endHandlingNetsearchDecreasesHandlingNetsearchesInProgressIfNotSuccess() {
    doReturn(1L, 10L).when(statisticsClock).getTime();
    long startTime = cachePerfStats.startHandlingNetsearch();

    cachePerfStats.endHandlingNetsearch(startTime, false);

    assertThat(statistics.getLong(handlingNetsearchesInProgressId)).isZero();
  }
}
