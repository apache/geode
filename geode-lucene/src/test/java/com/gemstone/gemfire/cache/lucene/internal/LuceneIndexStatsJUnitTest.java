/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.gemstone.gemfire.cache.lucene.internal;

import static org.mockito.Mockito.*;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

@Category(UnitTest.class)
public class LuceneIndexStatsJUnitTest {

  private Statistics statistics;
  private LuceneIndexStats stats;
  private StatisticsType type;

  @Before
  public void createStats() {
    StatisticsFactory statsFactory = mock(StatisticsFactory.class);
    statistics =  mock(Statistics.class);
    when(statsFactory.createAtomicStatistics(any(), anyString())).thenReturn(statistics);
    stats = new LuceneIndexStats(statsFactory, "region", "index");


    ArgumentCaptor<StatisticsType> statsTypeCaptor = ArgumentCaptor.forClass(StatisticsType.class);
    verify(statsFactory).createAtomicStatistics(statsTypeCaptor.capture(), anyString());
    type = statsTypeCaptor.getValue();
  }

  @Test
  public void shouldIncrementQueryStats() {

    stats.startQuery();
    verifyIncInt("queryExecutionsInProgress", 1);
    stats.endQuery(5);
    verifyIncInt("queryExecutionsInProgress", -1);
    verifyIncInt("queryExecutions", 1);
    //Because the initial stat time is 0 and the final time is 5, the delta is -5
    verifyIncLong("queryExecutionTime", -5);
  }

  @Test
  public void shouldIncrementUpdateStats() {

    stats.startUpdate();
    verifyIncInt("updatesInProgress", 1);
    stats.endUpdate(5);
    verifyIncInt("updatesInProgress", -1);
    verifyIncInt("updates", 1);
    //Because the initial stat time is 0 and the final time is 5, the delta is -5
    verifyIncLong("updateTime", -5);
  }

  @Test
  public void shouldIncrementCommitStats() {

    stats.startCommit();
    verifyIncInt("commitsInProgress", 1);
    stats.endCommit(5);
    verifyIncInt("commitsInProgress", -1);
    verifyIncInt("commits", 1);
    //Because the initial stat time is 0 and the final time is 5, the delta is -5
    verifyIncLong("commitTime", -5);
  }

  @Test
  public void shouldIncrementDocumentStat() {
    stats.incDocuments(5);
    verifyIncInt("documents", 5);
  }

  private void verifyIncInt(final String statName, final int value) {
    final int statId = type.nameToId(statName);
    verify(statistics).incInt(eq(statId), eq(value));
  }

  private void verifyIncLong(final String statName, final long value) {
    final int statId = type.nameToId(statName);
    verify(statistics).incLong(eq(statId), eq(value));
  }

}