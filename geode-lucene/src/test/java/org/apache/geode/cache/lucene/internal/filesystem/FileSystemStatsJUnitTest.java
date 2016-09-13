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
package org.apache.geode.cache.lucene.internal.filesystem;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.test.junit.categories.UnitTest;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.ArgumentCaptor;

@Category(UnitTest.class)
public class FileSystemStatsJUnitTest {

  private Statistics statistics;
  private FileSystemStats stats;
  private StatisticsType type;

  @Before
  public void createStats() {
    StatisticsFactory statsFactory = mock(StatisticsFactory.class);
    statistics =  mock(Statistics.class);
    when(statsFactory.createAtomicStatistics(any(), anyString())).thenReturn(statistics);
    stats = new FileSystemStats(statsFactory, "stats");


    ArgumentCaptor<StatisticsType> statsTypeCaptor = ArgumentCaptor.forClass(StatisticsType.class);
    verify(statsFactory).createAtomicStatistics(statsTypeCaptor.capture(), anyString());
    type = statsTypeCaptor.getValue();
  }

  @Test
  public void shouldIncrementReadBytes() {
    stats.incReadBytes(5);
    verifyIncLong("readBytes", 5);
  }

  @Test
  public void shouldIncrementWrittenBytes() {
    stats.incWrittenBytes(5);
    verifyIncLong("writtenBytes", 5);
  }

  @Test
  public void shouldIncrementFileCreates() {
    stats.incFileCreates(5);
    verifyIncInt("fileCreates", 5);
  }

  @Test
  public void shouldIncrementFileDeletes() {
    stats.incFileDeletes(5);
    verifyIncInt("fileDeletes", 5);
  }

  @Test
  public void shouldIncrementFileRenames() {
    stats.incFileRenames(5);
    verifyIncInt("fileRenames", 5);
  }

  @Test
  public void shouldIncrementTemporyFileCreates() {
    stats.incTemporaryFileCreates(5);
    verifyIncInt("temporaryFileCreates", 5);
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