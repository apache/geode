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

package org.apache.geode.redis.internal.collections;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Iterator;
import java.util.Set;

import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.generator.InRange;
import com.pholser.junit.quickcheck.generator.Size;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import org.junit.Assume;
import org.junit.runner.RunWith;

/**
 * These tests compare the OrderedStatisticsTree with a a TreeSet that has
 * undergone the same operations and ensures that the results match.
 */
@RunWith(JUnitQuickcheck.class)
public class OrderedStatisticTreeQuickCheckTest {
  OrderStatisticsSet<Long> list = new OrderStatisticsTree<>();
  OrderStatisticsSet<Long> expected = new IndexibleTreeSet<>();

  @Property
  public void equalsMatchesTreeSet(@Size(min = 2, max = 500) Set<Long> insertData) {
    expected.addAll(insertData);
    list.addAll(insertData);
    assertThat(expected.equals(list)).isTrue();
    // TODO - OST doesn't implement equals
    assertThat(list.equals(expected)).isTrue();
  }

  @Property
  public void getFindsCorrectElement(@Size(min = 2, max = 500) Set<Long> insertData,
      @InRange(minInt = 0, maxInt = 500) int index) {
    Assume.assumeTrue(index < insertData.size());
    expected.addAll(insertData);
    list.addAll(insertData);
    assertThat(list.get(index)).isEqualTo(expected.get(index));
  }

  @Property
  public void indexOfFindsElements(@Size(min = 2, max = 500) Set<Long> insertData,
      @InRange(minInt = 0, maxInt = 500) int elementIndex) {
    Assume.assumeTrue(elementIndex < insertData.size());
    expected.addAll(insertData);
    list.addAll(insertData);

    Long element = expected.get(elementIndex);

    assertThat(list.indexOf(element)).isEqualTo(expected.indexOf(element));
  }

  @Property
  public void iteratorIsInOrder(@Size(min = 2, max = 500) Set<Long> insertData) {
    expected.addAll(insertData);
    list.addAll(insertData);
    Iterator<Long> expectedIterator = expected.iterator();
    Iterator<Long> listIterator = list.iterator();
    while (expectedIterator.hasNext()) {
      assertThat(listIterator.hasNext()).isTrue();
      assertThat(listIterator.next()).isEqualTo(expectedIterator.next());
    }
    assertThat(listIterator.hasNext()).isFalse();
  }
}
