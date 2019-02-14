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
import static org.apache.geode.internal.cache.CachePerfStats.evictorJobsCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.evictorJobsStartedId;
import static org.apache.geode.internal.cache.CachePerfStats.getInitialImagesCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.getTimeId;
import static org.apache.geode.internal.cache.CachePerfStats.getsId;
import static org.apache.geode.internal.cache.CachePerfStats.indexUpdateCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.invalidatesId;
import static org.apache.geode.internal.cache.CachePerfStats.loadsCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.missesId;
import static org.apache.geode.internal.cache.CachePerfStats.netloadsCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.netsearchesCompletedId;
import static org.apache.geode.internal.cache.CachePerfStats.putTimeId;
import static org.apache.geode.internal.cache.CachePerfStats.putallsId;
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
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.cache.CachePerfStats.Clock;
import org.apache.geode.internal.statistics.StatisticsManager;
import org.apache.geode.internal.stats50.Atomic50StatisticsImpl;

/**
 * Unit tests for {@link CachePerfStats}.
 */
public class CachePerfStatsTest {

  private static final String TEXT_ID = "cachePerfStats";
  private static final long CLOCK_TIME = 10;

  private Statistics statistics;
  private CachePerfStats cachePerfStats;

  @Before
  public void setUp() {
    StatisticsType statisticsType = CachePerfStats.getStatisticsType();

    StatisticsManager statisticsManager = mock(StatisticsManager.class);
    StatisticsFactory statisticsFactory = mock(StatisticsFactory.class);
    Clock clock = mock(Clock.class);

    statistics = spy(new Atomic50StatisticsImpl(statisticsType, TEXT_ID, 1, 1,
        statisticsManager));

    when(clock.getTime()).thenReturn(CLOCK_TIME);
    when(statisticsFactory.createAtomicStatistics(eq(statisticsType), eq(TEXT_ID)))
        .thenReturn(statistics);

    CachePerfStats.enableClockStats = true;
    cachePerfStats = new CachePerfStats(statisticsFactory, TEXT_ID, clock);
  }

  @After
  public void tearDown() {
    CachePerfStats.enableClockStats = false;
  }

  @Test
  public void getPutsDelegatesToStatistics() {
    statistics.incLong(putsId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getPuts()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code puts} is to invoke
   * {@code endPut}.
   */
  @Test
  public void endPutIncrementsPuts() {
    cachePerfStats.endPut(0, false);

    assertThat(statistics.getLong(putsId)).isEqualTo(1);
  }

  @Test
  public void putsWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(putsId, Integer.MAX_VALUE);

    cachePerfStats.endPut(0, false);

    assertThat(cachePerfStats.getPuts()).isPositive();
  }

  @Test
  public void getGetsDelegatesToStatistics() {
    statistics.incLong(getsId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getGets()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code gets} is to invoke
   * {@code endGet}.
   */
  @Test
  public void endGetIncrementsGets() {
    cachePerfStats.endGet(0, false);

    assertThat(statistics.getLong(getsId)).isEqualTo(1);
  }

  @Test
  public void getsWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(getsId, Integer.MAX_VALUE);

    cachePerfStats.endGet(0, false);

    assertThat(cachePerfStats.getGets()).isPositive();
  }

  @Test
  public void getPutTimeDelegatesToStatistics() {
    statistics.incLong(putTimeId, Long.MAX_VALUE);

    assertThat(cachePerfStats.getPutTime()).isEqualTo(Long.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code putTime} is to invoke
   * {@code endPut}.
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
   * Characterization test: Note that the only way to increment {@code getTime} is to invoke
   * {@code endGet}.
   */
  @Test
  public void endGetIncrementsGetTime() {
    cachePerfStats.endGet(0, false);

    assertThat(statistics.getLong(getTimeId)).isEqualTo(CLOCK_TIME);
  }

  @Test
  public void getDestroysDelegatesToStatistics() {
    statistics.incLong(destroysId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getDestroys()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incDestroysIncrementsDestroys() {
    cachePerfStats.incDestroys();

    assertThat(statistics.getLong(destroysId)).isEqualTo(1);
  }

  @Test
  public void destroysWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(destroysId, Integer.MAX_VALUE);

    cachePerfStats.incDestroys();

    assertThat(cachePerfStats.getDestroys()).isPositive();
  }

  @Test
  public void getCreatesDelegatesToStatistics() {
    statistics.incLong(createsId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getCreates()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incCreatesIncrementsDestroys() {
    cachePerfStats.incCreates();

    assertThat(statistics.getLong(createsId)).isEqualTo(1);
  }

  @Test
  public void createsWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(createsId, Integer.MAX_VALUE);

    cachePerfStats.incCreates();

    assertThat(cachePerfStats.getCreates()).isPositive();
  }

  @Test
  public void getPutAllsDelegatesToStatistics() {
    statistics.incLong(putallsId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getPutAlls()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code putalls} is to invoke
   * {@code endPutAll}.
   */
  @Test
  public void endPutAllIncrementsDestroys() {
    cachePerfStats.endPutAll(0);

    assertThat(statistics.getLong(putallsId)).isEqualTo(1);
  }

  @Test
  public void putAllsWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(putallsId, Integer.MAX_VALUE);

    cachePerfStats.endPutAll(0);

    assertThat(cachePerfStats.getPutAlls()).isPositive();
  }

  @Test
  public void getRemoveAllsDelegatesToStatistics() {
    statistics.incLong(removeAllsId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getRemoveAlls()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code removeAlls} is to invoke
   * {@code endRemoveAll}.
   */
  @Test
  public void endRemoveAllIncrementsDestroys() {
    cachePerfStats.endRemoveAll(0);

    assertThat(statistics.getLong(removeAllsId)).isEqualTo(1);
  }

  @Test
  public void removeAllsWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(removeAllsId, Integer.MAX_VALUE);

    cachePerfStats.endRemoveAll(0);

    assertThat(cachePerfStats.getRemoveAlls()).isPositive();
  }

  @Test
  public void getUpdatesDelegatesToStatistics() {
    statistics.incLong(updatesId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getUpdates()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code updates} is to invoke
   * {@code endPut}.
   */
  @Test
  public void endPutIncrementsUpdates() {
    cachePerfStats.endPut(0, true);

    assertThat(statistics.getLong(updatesId)).isEqualTo(1);
  }

  @Test
  public void updatesWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(updatesId, Integer.MAX_VALUE);

    cachePerfStats.endPut(0, true);

    assertThat(cachePerfStats.getUpdates()).isPositive();
  }

  @Test
  public void getInvalidatesDelegatesToStatistics() {
    statistics.incLong(invalidatesId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getInvalidates()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incInvalidatesIncrementsInvalidates() {
    cachePerfStats.incInvalidates();

    assertThat(statistics.getLong(invalidatesId)).isEqualTo(1);
  }

  @Test
  public void invalidatesWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(invalidatesId, Integer.MAX_VALUE);

    cachePerfStats.incInvalidates();

    assertThat(cachePerfStats.getInvalidates()).isPositive();
  }

  @Test
  public void getMissesDelegatesToStatistics() {
    statistics.incLong(missesId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getMisses()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code misses} is to invoke
   * {@code endGet}.
   */
  @Test
  public void endGetIncrementsMisses() {
    cachePerfStats.endGet(0, true);

    assertThat(statistics.getLong(missesId)).isEqualTo(1);
  }

  @Test
  public void missesWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(missesId, Integer.MAX_VALUE);

    cachePerfStats.endGet(0, true);

    assertThat(cachePerfStats.getMisses()).isPositive();
  }

  @Test
  public void getRetriesDelegatesToStatistics() {
    statistics.incLong(retriesId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getRetries()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incRetriesIncrementsRetries() {
    cachePerfStats.incRetries();

    assertThat(statistics.getLong(retriesId)).isEqualTo(1);
  }

  @Test
  public void retriesWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(retriesId, Integer.MAX_VALUE);

    cachePerfStats.incRetries();

    assertThat(cachePerfStats.getRetries()).isPositive();
  }

  @Test
  public void getClearsDelegatesToStatistics() {
    statistics.incLong(clearsId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getClearCount()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incClearCountIncrementsClears() {
    cachePerfStats.incClearCount();

    assertThat(statistics.getLong(clearsId)).isEqualTo(1);
  }

  @Test
  public void clearsWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(clearsId, Integer.MAX_VALUE);

    cachePerfStats.incClearCount();

    assertThat(cachePerfStats.getClearCount()).isPositive();
  }

  @Test
  public void getLoadsCompletedDelegatesToStatistics() {
    statistics.incLong(loadsCompletedId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getLoadsCompleted()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code loadsCompleted} is to invoke
   * {@code endLoad}.
   */
  @Test
  public void endLoadIncrementsLoadsCompleted() {
    cachePerfStats.endLoad(0);

    assertThat(statistics.getLong(loadsCompletedId)).isEqualTo(1);
  }

  @Test
  public void loadsCompletedWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(loadsCompletedId, Integer.MAX_VALUE);

    cachePerfStats.endLoad(0);

    assertThat(cachePerfStats.getLoadsCompleted()).isPositive();
  }

  @Test
  public void getNetloadsCompletedDelegatesToStatistics() {
    statistics.incLong(netloadsCompletedId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getNetloadsCompleted()).isEqualTo(Integer.MAX_VALUE);
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

  @Test
  public void netloadsCompletedWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(netloadsCompletedId, Integer.MAX_VALUE);

    cachePerfStats.endNetload(0);

    assertThat(cachePerfStats.getNetloadsCompleted()).isPositive();
  }

  @Test
  public void getNetsearchesCompletedDelegatesToStatistics() {
    statistics.incLong(netsearchesCompletedId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getNetsearchesCompleted()).isEqualTo(Integer.MAX_VALUE);
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

  @Test
  public void netsearchesCompletedWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(netsearchesCompletedId, Integer.MAX_VALUE);

    cachePerfStats.endNetsearch(0);

    assertThat(cachePerfStats.getNetsearchesCompleted()).isPositive();
  }

  @Test
  public void getCacheWriterCallsCompletedDelegatesToStatistics() {
    statistics.incLong(cacheWriterCallsCompletedId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getCacheWriterCallsCompleted()).isEqualTo(Integer.MAX_VALUE);
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

  @Test
  public void cacheWriterCallsCompletedWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(cacheWriterCallsCompletedId, Integer.MAX_VALUE);

    cachePerfStats.endCacheWriterCall(0);

    assertThat(cachePerfStats.getCacheWriterCallsCompleted()).isPositive();
  }

  @Test
  public void getCacheListenerCallsCompletedDelegatesToStatistics() {
    statistics.incLong(cacheListenerCallsCompletedId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getCacheListenerCallsCompleted()).isEqualTo(Integer.MAX_VALUE);
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

  @Test
  public void cacheListenerCallsCompletedWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(cacheListenerCallsCompletedId, Integer.MAX_VALUE);

    cachePerfStats.endCacheListenerCall(0);

    assertThat(cachePerfStats.getCacheListenerCallsCompleted()).isPositive();
  }

  @Test
  public void getGetInitialImagesCompletedDelegatesToStatistics() {
    statistics.incLong(getInitialImagesCompletedId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getGetInitialImagesCompleted()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code getInitialImagesCompleted}
   * is to invoke {@code endGetInitialImage}.
   */
  @Test
  public void endGetInitialImageIncrementsGetInitialImagesCompleted() {
    cachePerfStats.endGetInitialImage(0);

    assertThat(statistics.getLong(getInitialImagesCompletedId)).isEqualTo(1);
  }

  @Test
  public void getInitialImagesCompletedWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(getInitialImagesCompletedId, Integer.MAX_VALUE);

    cachePerfStats.endGetInitialImage(0);

    assertThat(cachePerfStats.getGetInitialImagesCompleted()).isPositive();
  }

  @Test
  public void getDeltaGetInitialImagesCompletedDelegatesToStatistics() {
    statistics.incLong(deltaGetInitialImagesCompletedId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getDeltaGetInitialImagesCompleted()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incDeltaGIICompletedIncrementsDeltaGetInitialImagesCompleted() {
    cachePerfStats.incDeltaGIICompleted();

    assertThat(statistics.getLong(deltaGetInitialImagesCompletedId)).isEqualTo(1);
  }

  @Test
  public void deltaGetInitialImagesCompletedWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(deltaGetInitialImagesCompletedId, Integer.MAX_VALUE);

    cachePerfStats.incDeltaGIICompleted();

    assertThat(cachePerfStats.getDeltaGetInitialImagesCompleted()).isPositive();
  }

  @Test
  public void getQueryExecutionsDelegatesToStatistics() {
    statistics.incLong(queryExecutionsId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getQueryExecutions()).isEqualTo(Integer.MAX_VALUE);
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

  @Test
  public void queryExecutionsWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(queryExecutionsId, Integer.MAX_VALUE);

    cachePerfStats.endQueryExecution(1);

    assertThat(cachePerfStats.getQueryExecutions()).isPositive();
  }

  @Test
  public void getTxCommitsDelegatesToStatistics() {
    statistics.incLong(txCommitsId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getTxCommits()).isEqualTo(Integer.MAX_VALUE);
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

  @Test
  public void txCommitsWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(txCommitsId, Integer.MAX_VALUE);

    cachePerfStats.txSuccess(1, 1, 1);

    assertThat(cachePerfStats.getTxCommits()).isPositive();
  }

  @Test
  public void getTxFailuresDelegatesToStatistics() {
    statistics.incLong(txFailuresId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getTxFailures()).isEqualTo(Integer.MAX_VALUE);
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

  @Test
  public void txFailuresWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(txFailuresId, Integer.MAX_VALUE);

    cachePerfStats.txFailure(1, 1, 1);

    assertThat(cachePerfStats.getTxFailures()).isPositive();
  }

  @Test
  public void getTxRollbacksDelegatesToStatistics() {
    statistics.incLong(txRollbacksId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getTxRollbacks()).isEqualTo(Integer.MAX_VALUE);
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

  @Test
  public void txRollbacksWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(txRollbacksId, Integer.MAX_VALUE);

    cachePerfStats.txRollback(1, 1, 1);

    assertThat(cachePerfStats.getTxRollbacks()).isPositive();
  }

  @Test
  public void getTxCommitChangesDelegatesToStatistics() {
    statistics.incLong(txCommitChangesId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getTxCommitChanges()).isEqualTo(Integer.MAX_VALUE);
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

  @Test
  public void txCommitChangesWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(txCommitChangesId, Integer.MAX_VALUE);

    cachePerfStats.txSuccess(1, 1, 1);

    assertThat(cachePerfStats.getTxCommitChanges()).isPositive();
  }

  @Test
  public void getTxFailureChangesDelegatesToStatistics() {
    statistics.incLong(txFailureChangesId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getTxFailureChanges()).isEqualTo(Integer.MAX_VALUE);
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

  @Test
  public void txFailureChangesWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(txFailureChangesId, Integer.MAX_VALUE);

    cachePerfStats.txFailure(1, 1, 1);

    assertThat(cachePerfStats.getTxFailureChanges()).isPositive();
  }

  @Test
  public void getTxRollbackChangesDelegatesToStatistics() {
    statistics.incLong(txRollbackChangesId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getTxRollbackChanges()).isEqualTo(Integer.MAX_VALUE);
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

  @Test
  public void txRollbackChangesWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(txRollbackChangesId, Integer.MAX_VALUE);

    cachePerfStats.txRollback(1, 1, 1);

    assertThat(cachePerfStats.getTxRollbackChanges()).isPositive();
  }

  @Test
  public void getEvictorJobsStartedChangesDelegatesToStatistics() {
    statistics.incLong(evictorJobsStartedId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getEvictorJobsStarted()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incEvictorJobsStartedIncrementsEvictorJobsStarted() {
    cachePerfStats.incEvictorJobsStarted();

    assertThat(statistics.getLong(evictorJobsStartedId)).isEqualTo(1);
  }

  @Test
  public void evictorJobsStartedWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(evictorJobsStartedId, Integer.MAX_VALUE);

    cachePerfStats.incEvictorJobsStarted();

    assertThat(cachePerfStats.getEvictorJobsStarted()).isPositive();
  }

  @Test
  public void getEvictorJobsCompletedChangesDelegatesToStatistics() {
    statistics.incLong(evictorJobsCompletedId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getEvictorJobsCompleted()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incEvictorJobsCompletedIncrementsEvictorJobsCompleted() {
    cachePerfStats.incEvictorJobsCompleted();

    assertThat(statistics.getLong(evictorJobsCompletedId)).isEqualTo(1);
  }

  @Test
  public void evictorJobsCompletedWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(evictorJobsCompletedId, Integer.MAX_VALUE);

    cachePerfStats.incEvictorJobsCompleted();

    assertThat(cachePerfStats.getEvictorJobsCompleted()).isPositive();
  }

  @Test
  public void getIndexUpdateCompletedChangesDelegatesToStatistics() {
    statistics.incLong(indexUpdateCompletedId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getIndexUpdateCompleted()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code indexUpdateCompleted} is to
   * invoke {@code endIndexUpdate}.
   */
  @Test
  public void endIndexUpdateIncrementsIndexUpdateCompleted() {
    cachePerfStats.endIndexUpdate(1);

    assertThat(statistics.getLong(indexUpdateCompletedId)).isEqualTo(1);
  }

  @Test
  public void indexUpdateCompletedWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(indexUpdateCompletedId, Integer.MAX_VALUE);

    cachePerfStats.endIndexUpdate(1);

    assertThat(cachePerfStats.getIndexUpdateCompleted()).isPositive();
  }

  @Test
  public void getDeltaUpdatesDelegatesToStatistics() {
    statistics.incLong(deltaUpdatesId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getDeltaUpdates()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code deltaUpdates} is to
   * invoke {@code endDeltaUpdate}.
   */
  @Test
  public void endDeltaUpdateIncrementsDeltaUpdates() {
    cachePerfStats.endDeltaUpdate(1);

    assertThat(statistics.getLong(deltaUpdatesId)).isEqualTo(1);
  }

  @Test
  public void deltaUpdatesWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(deltaUpdatesId, Integer.MAX_VALUE);

    cachePerfStats.endDeltaUpdate(1);

    assertThat(cachePerfStats.getDeltaUpdates()).isPositive();
  }

  @Test
  public void getDeltaFailedUpdatesDelegatesToStatistics() {
    statistics.incLong(deltaFailedUpdatesId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getDeltaFailedUpdates()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incDeltaFailedUpdatesIncrementsDeltaFailedUpdates() {
    cachePerfStats.incDeltaFailedUpdates();

    assertThat(statistics.getLong(deltaFailedUpdatesId)).isEqualTo(1);
  }

  @Test
  public void deltaFailedUpdatesWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(deltaFailedUpdatesId, Integer.MAX_VALUE);

    cachePerfStats.incDeltaFailedUpdates();

    assertThat(cachePerfStats.getDeltaFailedUpdates()).isPositive();
  }

  @Test
  public void getDeltasPreparedUpdatesDelegatesToStatistics() {
    statistics.incLong(deltasPreparedId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getDeltasPrepared()).isEqualTo(Integer.MAX_VALUE);
  }

  /**
   * Characterization test: Note that the only way to increment {@code deltasPrepared} is to
   * invoke {@code endDeltaPrepared}.
   */
  @Test
  public void endDeltaPreparedIncrementsDeltasPrepared() {
    cachePerfStats.endDeltaPrepared(1);

    assertThat(statistics.getLong(deltasPreparedId)).isEqualTo(1);
  }

  @Test
  public void deltasPreparedWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(deltasPreparedId, Integer.MAX_VALUE);

    cachePerfStats.endDeltaPrepared(1);

    assertThat(cachePerfStats.getDeltasPrepared()).isPositive();
  }

  @Test
  public void getDeltasSentDelegatesToStatistics() {
    statistics.incLong(deltasSentId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getDeltasSent()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incDeltasSentPreparedIncrementsDeltasSent() {
    cachePerfStats.incDeltasSent();

    assertThat(statistics.getLong(deltasSentId)).isEqualTo(1);
  }

  @Test
  public void deltasSentWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(deltasSentId, Integer.MAX_VALUE);

    cachePerfStats.incDeltasSent();

    assertThat(cachePerfStats.getDeltasSent()).isPositive();
  }

  @Test
  public void getDeltaFullValuesSentDelegatesToStatistics() {
    statistics.incLong(deltaFullValuesSentId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getDeltaFullValuesSent()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incDeltaFullValuesSentIncrementsDeltaFullValuesSent() {
    cachePerfStats.incDeltaFullValuesSent();

    assertThat(statistics.getLong(deltaFullValuesSentId)).isEqualTo(1);
  }

  @Test
  public void deltaFullValuesSentWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(deltaFullValuesSentId, Integer.MAX_VALUE);

    cachePerfStats.incDeltaFullValuesSent();

    assertThat(cachePerfStats.getDeltaFullValuesSent()).isPositive();
  }

  @Test
  public void getDeltaFullValuesRequestedDelegatesToStatistics() {
    statistics.incLong(deltaFullValuesRequestedId, Integer.MAX_VALUE);

    assertThat(cachePerfStats.getDeltaFullValuesRequested()).isEqualTo(Integer.MAX_VALUE);
  }

  @Test
  public void incDeltaFullValuesRequestedIncrementsDeltaFullValuesRequested() {
    cachePerfStats.incDeltaFullValuesRequested();

    assertThat(statistics.getLong(deltaFullValuesRequestedId)).isEqualTo(1);
  }

  @Test
  public void deltaFullValuesRequestedWrapsFromMaxIntegerStaysPositive() {
    statistics.incLong(deltaFullValuesRequestedId, Integer.MAX_VALUE);

    cachePerfStats.incDeltaFullValuesRequested();

    assertThat(cachePerfStats.getDeltaFullValuesRequested()).isPositive();
  }
}
