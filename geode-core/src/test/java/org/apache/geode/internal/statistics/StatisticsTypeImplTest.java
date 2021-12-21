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
package org.apache.geode.internal.statistics;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.StatisticsTypeFactory;


public class StatisticsTypeImplTest {

  private final StatisticsTypeFactory statsFactory = StatisticsTypeFactoryImpl.singleton();

  private final StatisticDescriptor[] stats = new StatisticDescriptor[] {
      statsFactory.createDoubleGauge("doubleCount", "double counter", "doubles"),
      statsFactory.createLongCounter("longCount", "long counter", "longs"),
      statsFactory.createIntCounter("intCount", "int counter", "ints"),
      statsFactory.createLongCounter("longCount2", "long counter", "longs"),
  };

  private StatisticsTypeImpl statisticsType = new StatisticsTypeImpl("abc", "mock stats", stats);

  @Test
  public void testIdsAreAssignedInGroupsByType() {
    int idForIntCounter = statisticsType.nameToId("intCount");
    int idForLongCounter = statisticsType.nameToId("longCount");
    int idForLongCounter2 = statisticsType.nameToId("longCount2");
    int idForDoubleCounter = statisticsType.nameToId("doubleCount");

    assertThat(idForIntCounter).isLessThan(idForDoubleCounter);
    assertThat(idForLongCounter).isLessThan(idForDoubleCounter);
    assertThat(idForLongCounter2).isLessThan(idForDoubleCounter);
  }

  @Test
  public void testTypeCounts() {
    assertThat(statisticsType.getLongStatCount()).isEqualTo(3);
    assertThat(statisticsType.getDoubleStatCount()).isEqualTo(1);
  }

  @Test
  public void testIsValidIntId() {
    int idForIntCounter = statisticsType.nameToId("intCount");

    assertThat(statisticsType.isValidLongId(idForIntCounter)).isEqualTo(true);
    assertThat(statisticsType.isValidDoubleId(idForIntCounter)).isEqualTo(false);
  }

  @Test
  public void testIsValidLongId() {
    int idForLongCounter = statisticsType.nameToId("longCount");
    int idForLongCounter2 = statisticsType.nameToId("longCount2");

    assertThat(statisticsType.isValidLongId(idForLongCounter)).isEqualTo(true);
    assertThat(statisticsType.isValidDoubleId(idForLongCounter)).isEqualTo(false);

    assertThat(statisticsType.isValidLongId(idForLongCounter2)).isEqualTo(true);
    assertThat(statisticsType.isValidDoubleId(idForLongCounter2)).isEqualTo(false);
  }

  @Test
  public void testIsValidDoubleId() {
    int idForDoubleCounter = statisticsType.nameToId("doubleCount");

    assertThat(statisticsType.isValidLongId(idForDoubleCounter)).isEqualTo(false);
    assertThat(statisticsType.isValidDoubleId(idForDoubleCounter)).isEqualTo(true);
  }

  @Test
  public void testIsValidId_NoDescriptors() {
    StatisticDescriptor[] stats = {};
    statisticsType = new StatisticsTypeImpl("abc", "mock stats", stats);

    assertThat(statisticsType.isValidLongId(0)).isEqualTo(false);
    assertThat(statisticsType.isValidDoubleId(0)).isEqualTo(false);

  }
}
