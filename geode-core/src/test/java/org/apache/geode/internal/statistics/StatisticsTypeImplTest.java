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
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;


public class StatisticsTypeImplTest {

  private StatisticsTypeFactory statsFactory = StatisticsTypeFactoryImpl.singleton();

  private StatisticDescriptor[] stats = new StatisticDescriptor[] {
      statsFactory.createDoubleGauge("doubleCount", "double counter", "doubles"),
      statsFactory.createLongCounter("longCount", "long counter", "longs"),
      statsFactory.createIntCounter("intCount", "int counter", "ints"),
      statsFactory.createLongCounter("longCount2", "long counter", "longs"),
  };

  @Test
  public void testIdsAreAssignedInGroupsByType() {
    StatisticsType statisticsType = new StatisticsTypeImpl("abc", "mock stats", stats);

    int idForIntCounter = statisticsType.nameToId("intCount");
    int idForLongCounter = statisticsType.nameToId("longCount");
    int idForLongCounter2 = statisticsType.nameToId("longCount2");
    int idForDoubleCounter = statisticsType.nameToId("doubleCount");

    assertThat(idForIntCounter).isLessThan(idForLongCounter);
    assertThat(idForLongCounter).isLessThan(idForDoubleCounter);
    assertThat(idForLongCounter2).isLessThan(idForDoubleCounter);
  }

  @Test
  public void testTypeCounts() {
    StatisticsTypeImpl statisticsType = new StatisticsTypeImpl("abc", "mock stats", stats);

    assertThat(statisticsType.getIntStatCount()).isEqualTo(1);
    assertThat(statisticsType.getLongStatCount()).isEqualTo(2);
    assertThat(statisticsType.getDoubleStatCount()).isEqualTo(1);
  }

  @Test
  public void testIsValidIntId() {
    StatisticsTypeImpl statisticsType = new StatisticsTypeImpl("abc", "mock stats", stats);

    int idForIntCounter = statisticsType.nameToId("intCount");

    assertThat(statisticsType.isValidIntId(idForIntCounter)).isEqualTo(true);
    assertThat(statisticsType.isValidLongId(idForIntCounter)).isEqualTo(false);
    assertThat(statisticsType.isValidDoubleId(idForIntCounter)).isEqualTo(false);
  }

  @Test
  public void testIsValidLongId() {
    StatisticsTypeImpl statisticsType = new StatisticsTypeImpl("abc", "mock stats", stats);

    int idForLongCounter = statisticsType.nameToId("longCount");
    int idForLongCounter2 = statisticsType.nameToId("longCount2");

    assertThat(statisticsType.isValidIntId(idForLongCounter)).isEqualTo(false);
    assertThat(statisticsType.isValidLongId(idForLongCounter)).isEqualTo(true);
    assertThat(statisticsType.isValidDoubleId(idForLongCounter)).isEqualTo(false);

    assertThat(statisticsType.isValidIntId(idForLongCounter2)).isEqualTo(false);
    assertThat(statisticsType.isValidLongId(idForLongCounter2)).isEqualTo(true);
    assertThat(statisticsType.isValidDoubleId(idForLongCounter2)).isEqualTo(false);
  }

  @Test
  public void testIsValidDoubleId() {
    StatisticsTypeImpl statisticsType = new StatisticsTypeImpl("abc", "mock stats", stats);

    int idForDoubleCounter = statisticsType.nameToId("doubleCount");

    assertThat(statisticsType.isValidIntId(idForDoubleCounter)).isEqualTo(false);
    assertThat(statisticsType.isValidLongId(idForDoubleCounter)).isEqualTo(false);
    assertThat(statisticsType.isValidDoubleId(idForDoubleCounter)).isEqualTo(true);
  }

  @Test
  public void testIsValidId_NoDescriptors() {
    StatisticDescriptor[] stats = {};
    StatisticsTypeImpl statisticsType = new StatisticsTypeImpl("abc", "mock stats", stats);


    assertThat(statisticsType.isValidIntId(0)).isEqualTo(false);
    assertThat(statisticsType.isValidLongId(0)).isEqualTo(false);
    assertThat(statisticsType.isValidDoubleId(0)).isEqualTo(false);

  }
}
